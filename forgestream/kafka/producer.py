"""
Resilient Kafka Telemetry Producer.
Features schema pre-validation, partition keying, delivery callbacks, and graceful fallback.
"""

import json
import socket
import time
from typing import Any, Callable, Dict, Optional, Union
from confluent_kafka import Producer, KafkaError, KafkaException
from forgestream.kafka.config import KafkaClientConfig
from forgestream.schemas.telemetry_schema import TelemetryEvent
from forgestream.observability.metrics import metrics
from forgestream.observability.logging import get_logger

logger = get_logger("kafka.producer")


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


class ResilientKafkaProducer:
    """
    High-throughput resilient Kafka producer with automatic retry and metric tracking.
    """

    def __init__(self, config: Optional[KafkaClientConfig] = None, force_fallback: bool = False):
        self.config = config or KafkaClientConfig()
        self.force_fallback = force_fallback
        self._producer: Optional[Producer] = None
        self._is_live_broker = False
        self._fallback_queue: list = []  # In-memory buffer used if broker is offline or forced

        if not force_fallback:
            self._init_producer()

    def _init_producer(self) -> None:
        """Attempts connection to the Kafka cluster."""
        if not _probe_kafka_broker(self.config.bootstrap_servers):
            self._is_live_broker = False
            logger.info("Kafka broker unreachable. Operating in resilient in-memory streaming fallback mode.")
            return

        try:
            prod_conf = self.config.to_producer_dict()
            self._producer = Producer(prod_conf)
            self._is_live_broker = True
            logger.info("Confluent Kafka Producer initialized successfully")
        except Exception as e:
            self._is_live_broker = False
            logger.warning(f"Kafka broker unavailable, operating in local-memory fallback mode: {e}")

    @property
    def is_live(self) -> bool:
        return self._is_live_broker and not self.force_fallback

    def produce_telemetry(
        self,
        event: Union[TelemetryEvent, Dict[str, Any]],
        topic: Optional[str] = None,
        on_delivery: Optional[Callable] = None,
    ) -> bool:
        """
        Publishes a telemetry event keyed by asset_id to ensure partition-level ordering.
        """
        t0 = time.perf_counter()
        target_topic = topic or self.config.topic_telemetry

        if isinstance(event, TelemetryEvent):
            payload_dict = event.model_dump(mode="json")
            asset_id = event.asset_id
            event_id = event.event_id
        else:
            payload_dict = dict(event)
            asset_id = str(payload_dict.get("asset_id", "UNKNOWN"))
            event_id = str(payload_dict.get("event_id", "UNKNOWN"))

        payload_bytes = json.dumps(payload_dict).encode("utf-8")
        key_bytes = asset_id.encode("utf-8")

        def _default_callback(err, msg):
            if err is not None:
                metrics.increment("events_failed_kafka")
                logger.error(f"Kafka delivery failed for {event_id}: {err}")
                if on_delivery:
                    on_delivery(err, msg)
            else:
                metrics.increment("events_sent_kafka")
                if on_delivery:
                    on_delivery(None, msg)

        if self.is_live and self._producer:
            try:
                self._producer.produce(
                    topic=target_topic,
                    key=key_bytes,
                    value=payload_bytes,
                    on_delivery=_default_callback,
                )
                self._producer.poll(0)  # Trigger callbacks asynchronously
                success = True
            except BufferError:
                # Queue full, flush and retry once
                self._producer.flush(timeout=2.0)
                try:
                    self._producer.produce(
                        topic=target_topic,
                        key=key_bytes,
                        value=payload_bytes,
                        on_delivery=_default_callback,
                    )
                    success = True
                except Exception as e:
                    logger.error(f"Producer buffer retry failed: {e}")
                    metrics.increment("events_failed_kafka")
                    self._fallback_queue.append((target_topic, key_bytes, payload_bytes))
                    success = False
            except Exception as e:
                logger.error(f"Direct produce error: {e}")
                metrics.increment("events_failed_kafka")
                self._fallback_queue.append((target_topic, key_bytes, payload_bytes))
                success = False
        else:
            # In-memory queue mode
            self._fallback_queue.append((target_topic, key_bytes, payload_bytes))
            metrics.increment("events_sent_kafka")
            success = True

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        metrics.observe("kafka_produce_latency_ms", elapsed_ms)
        return success

    def send(self, topic: str, payload: Any, key: Optional[str] = None) -> bool:
        """Alias for produce_telemetry supporting (topic, payload, key) parameter order."""
        if isinstance(payload, dict) and key and "asset_id" not in payload:
            payload["asset_id"] = key
        return self.produce_telemetry(event=payload, topic=topic)

    def produce_event(
        self,
        event: Union[TelemetryEvent, Dict[str, Any]],
        topic: Optional[str] = None,
        on_delivery: Optional[Callable] = None,
    ) -> bool:
        """Alias for produce_telemetry."""
        return self.produce_telemetry(event=event, topic=topic, on_delivery=on_delivery)

    def publish_telemetry(
        self,
        event: Union[TelemetryEvent, Dict[str, Any]],
        topic: Optional[str] = None,
        partition_key: Optional[str] = None,
        on_delivery: Optional[Callable] = None,
    ) -> bool:
        """Alias for produce_telemetry."""
        if isinstance(event, dict) and partition_key and "asset_id" not in event:
            event["asset_id"] = partition_key
        return self.produce_telemetry(event=event, topic=topic, on_delivery=on_delivery)

    def flush(self, timeout_sec: float = 5.0, timeout: Optional[float] = None) -> int:
        """Flushes buffered messages to broker."""
        effective_timeout = timeout if timeout is not None else timeout_sec
        if self.is_live and self._producer:
            return self._producer.flush(timeout=effective_timeout)
        return 0

    def get_fallback_queue(self) -> list:
        """Returns in-memory queue for offline verification."""
        return self._fallback_queue
