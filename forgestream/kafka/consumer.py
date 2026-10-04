"""
Kafka Telemetry Consumer and Smoke Testing Reader.
Provides offset inspection, partition decoding, and batch consumption.
"""

import json
import socket
from typing import Any, Dict, Generator, List, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from forgestream.kafka.config import KafkaClientConfig
from forgestream.observability.metrics import metrics
from forgestream.observability.logging import get_logger

logger = get_logger("kafka.consumer")


def _probe_kafka_broker(bootstrap_servers: str, timeout_sec: float = 0.5) -> bool:
    """Fast non-blocking probe to verify Kafka broker reachability."""
    try:
        first_endpoint = bootstrap_servers.split(",")[0].strip()
        host, port_str = first_endpoint.split(":")
        sock = socket.create_connection((host, int(port_str)), timeout=timeout_sec)
        sock.close()
        return True
    except Exception:
        return False


class TelemetryConsumer:
    """
    Subscribes to telemetry topics and decodes JSON events with offset and partition tracking.
    """

    def __init__(
        self,
        config: Optional[KafkaClientConfig] = None,
        group_id: Optional[str] = None,
        force_fallback: bool = False,
    ):
        self.config = config or KafkaClientConfig()
        self.group_id = group_id or self.config.group_id
        self.force_fallback = force_fallback
        self._consumer: Optional[Consumer] = None
        self._is_live_broker = False

        if not force_fallback:
            self._init_consumer()

    def _init_consumer(self) -> None:
        """Initializes confluent-kafka consumer connection."""
        if not _probe_kafka_broker(self.config.bootstrap_servers):
            self._is_live_broker = False
            logger.info("Kafka consumer detected broker offline. Using fallback streaming queue.")
            return

        try:
            conf = self.config.to_consumer_dict(group_id_override=self.group_id)
            self._consumer = Consumer(conf)
            self._is_live_broker = True
            logger.info("Confluent Kafka Consumer initialized successfully")
        except Exception as e:
            self._is_live_broker = False
            logger.warning(f"Kafka consumer connection offline: {e}")

    @property
    def is_live(self) -> bool:
        return self._is_live_broker and not self.force_fallback

    def subscribe(self, topics: List[str]) -> None:
        """Subscribes the consumer to one or more topics."""
        if self.is_live and self._consumer:
            self._consumer.subscribe(topics)

    def consume_batch(
        self,
        max_messages: int = 100,
        timeout_sec: float = 1.0,
        fallback_source: Optional[List] = None,
    ) -> List[Dict[str, Any]]:
        """
        Polls for a batch of messages and decodes payloads with metadata (partition, offset).
        """
        results = []

        if fallback_source is not None:
            # Drain from in-memory fallback queue
            count = 0
            while fallback_source and count < max_messages:
                target_topic, key_bytes, payload_bytes = fallback_source.pop(0)
                try:
                    payload = json.loads(payload_bytes.decode("utf-8"))
                    payload["_kafka_metadata"] = {
                        "topic": target_topic,
                        "partition": 0,
                        "offset": count,
                        "key": key_bytes.decode("utf-8") if key_bytes else None,
                    }
                    results.append(payload)
                    metrics.increment("events_consumed_kafka")
                    count += 1
                except Exception as e:
                    logger.warning(f"Malformed fallback message: {e}")
            return results

        if self.is_live and self._consumer:
            import time
            deadline = time.perf_counter() + timeout_sec
            while len(results) < max_messages:
                remaining_time = max(0.01, deadline - time.perf_counter())
                msg = self._consumer.poll(timeout=min(0.5, remaining_time))
                if msg is None:
                    if time.perf_counter() >= deadline:
                        break
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Consumer message error: {msg.error()}")
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    payload["_kafka_metadata"] = {
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "key": msg.key().decode("utf-8") if msg.key() else None,
                    }
                    results.append(payload)
                    metrics.increment("events_consumed_kafka")
                except Exception as e:
                    logger.warning(f"Malformed message in topic {msg.topic()} at offset {msg.offset()}: {e}")

                if time.perf_counter() >= deadline:
                    break

        return results

    def close(self) -> None:
        """Closes the consumer handle."""
        if self.is_live and self._consumer:
            self._consumer.close()
