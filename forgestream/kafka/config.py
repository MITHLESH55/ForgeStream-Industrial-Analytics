"""
Kafka Client Configuration Dataclass and Settings Mapper.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from forgestream.config import settings


@dataclass
class KafkaClientConfig:
    """Configures Kafka producer and consumer parameters."""
    bootstrap_servers: str = settings.kafka.bootstrap_servers
    client_id: str = settings.kafka.client_id
    group_id: str = "forgestream-consumer-group-01"
    topic_telemetry: str = settings.kafka.topic_telemetry
    topic_maintenance: str = settings.kafka.topic_maintenance
    topic_alerts: str = settings.kafka.topic_alerts
    topic_models: str = settings.kafka.topic_models
    topic_metrics: str = settings.kafka.topic_metrics
    auto_offset_reset: str = settings.kafka.auto_offset_reset
    enable_auto_commit: bool = True
    max_retries: int = settings.kafka.max_retries
    retry_backoff_ms: int = settings.kafka.retry_backoff_ms

    def to_producer_dict(self) -> Dict[str, Any]:
        """Returns standard configuration dictionary for confluent-kafka Producer."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "retries": self.max_retries,
            "retry.backoff.ms": self.retry_backoff_ms,
            "acks": "1",  # Leader acknowledgment for fast reliable stream
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.ms": 10,
        }

    def to_consumer_dict(self, group_id_override: Optional[str] = None) -> Dict[str, Any]:
        """Returns standard configuration dictionary for confluent-kafka Consumer."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id_override or self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
            "session.timeout.ms": 6000,
        }
