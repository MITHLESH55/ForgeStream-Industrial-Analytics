"""
Kafka Topic Definitions and Cluster Provisioning.
Manages topic lifecycle, partitions, replication factors, and retention policies.
"""

import socket
from dataclasses import dataclass
from typing import Dict, List, Optional
from confluent_kafka.admin import AdminClient, NewTopic
from forgestream.config import settings
from forgestream.observability.logging import get_logger

logger = get_logger("kafka.topics")


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


@dataclass
class TopicDefinition:
    name: str
    num_partitions: int
    replication_factor: int
    retention_ms: int
    cleanup_policy: str = "delete"
    description: str = ""


TOPIC_DEFINITIONS: List[TopicDefinition] = [
    TopicDefinition(
        name=settings.kafka.topic_telemetry,
        num_partitions=5,
        replication_factor=1,
        retention_ms=7 * 24 * 3600 * 1000,
        description="High-frequency validated industrial equipment telemetry",
    ),
    TopicDefinition(
        name=settings.kafka.topic_maintenance,
        num_partitions=3,
        replication_factor=1,
        retention_ms=30 * 24 * 3600 * 1000,
        description="Maintenance history and technician work order event logs",
    ),
    TopicDefinition(
        name=settings.kafka.topic_alerts,
        num_partitions=3,
        replication_factor=1,
        retention_ms=14 * 24 * 3600 * 1000,
        description="Threshold breach alarms and operational state alerts",
    ),
    TopicDefinition(
        name=settings.kafka.topic_models,
        num_partitions=1,
        replication_factor=1,
        retention_ms=30 * 24 * 3600 * 1000,
        description="Predictive model training events and RUL score broadcasts",
    ),
    TopicDefinition(
        name=settings.kafka.topic_metrics,
        num_partitions=1,
        replication_factor=1,
        retention_ms=7 * 24 * 3600 * 1000,
        description="Pipeline metrics and health telemetry stream",
    ),
]


class TopicManager:
    """Provisions and inspects topics on the target Kafka broker."""

    def __init__(self, bootstrap_servers: Optional[str] = None, force_fallback: bool = False):
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.force_fallback = force_fallback
        if not force_fallback and _probe_kafka_broker(self.bootstrap_servers):
            try:
                self.admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            except Exception:
                self.admin = None
        else:
            self.admin = None

    def provision_all_topics(self) -> Dict[str, str]:
        """
        Idempotently creates all required ForgeStream topics.
        Returns a dictionary mapping topic names to status ("created", "already_exists", "error").
        """
        if self.force_fallback or not self.admin:
            return {td.name: "fallback_queue" for td in TOPIC_DEFINITIONS}

        results = {}
        new_topics = []

        try:
            # Query existing topics
            metadata = self.admin.list_topics(timeout=2.0)
            existing_topics = set(metadata.topics.keys())

            for td in TOPIC_DEFINITIONS:
                if td.name in existing_topics:
                    results[td.name] = "already_exists"
                else:
                    new_topics.append(
                        NewTopic(
                            topic=td.name,
                            num_partitions=td.num_partitions,
                            replication_factor=td.replication_factor,
                            config={
                                "retention.ms": str(td.retention_ms),
                                "cleanup.policy": td.cleanup_policy,
                            },
                        )
                    )

            if new_topics:
                futures = self.admin.create_topics(new_topics, request_timeout=5.0)
                for topic_name, future in futures.items():
                    try:
                        future.result()  # Blocks until created
                        results[topic_name] = "created"
                        logger.info(f"Created topic '{topic_name}' successfully")
                    except Exception as e:
                        results[topic_name] = f"error: {str(e)}"
                        logger.warning(f"Topic creation failed for '{topic_name}': {e}")
        except Exception as e:
            logger.debug(f"Kafka admin unavailable ({e}). Fallback queue active.")
            for td in TOPIC_DEFINITIONS:
                results[td.name] = f"fallback_active"

        return results

    def provision_topics(self) -> Dict[str, str]:
        """Alias for provision_all_topics."""
        return self.provision_all_topics()

    def list_topic_metadata(self) -> Dict[str, Dict]:
        """Returns details on partitions and brokers."""
        if self.force_fallback or not self.admin:
            return {td.name: {"partitions": td.num_partitions, "partition_ids": list(range(td.num_partitions))} for td in TOPIC_DEFINITIONS}

        try:
            metadata = self.admin.list_topics(timeout=2.0)
            info = {}
            for name, topic_obj in metadata.topics.items():
                if name.startswith("forgestream") or name in [t.name for t in TOPIC_DEFINITIONS]:
                    info[name] = {
                        "partitions": len(topic_obj.partitions),
                        "partition_ids": list(topic_obj.partitions.keys()),
                    }
            return info
        except Exception as e:
            logger.debug(f"Failed listing topic metadata: {e}")
            return {td.name: {"partitions": td.num_partitions, "partition_ids": list(range(td.num_partitions))} for td in TOPIC_DEFINITIONS}


# Alias for compatibility
KafkaTopicManager = TopicManager
