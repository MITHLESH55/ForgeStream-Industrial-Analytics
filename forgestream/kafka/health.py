"""
Kafka Health and Readiness Checker.
"""

from typing import Any, Dict, Optional
from confluent_kafka.admin import AdminClient
from forgestream.config import settings
from forgestream.observability.logging import get_logger

logger = get_logger("kafka.health")


class KafkaHealthChecker:
    """Probes the live Kafka cluster for broker readiness and topic existence."""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers

    def check_health(self, timeout_sec: float = 3.0) -> Dict[str, Any]:
        """
        Executes a live cluster probe and returns health status dictionary.
        """
        try:
            admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            meta = admin.list_topics(timeout=timeout_sec)

            brokers = [b.id for b in meta.brokers.values()]
            topics = list(meta.topics.keys())

            return {
                "status": "HEALTHY",
                "bootstrap_servers": self.bootstrap_servers,
                "broker_count": len(brokers),
                "broker_ids": brokers,
                "topic_count": len(topics),
                "topics": topics,
            }
        except Exception as e:
            return {
                "status": "UNAVAILABLE",
                "bootstrap_servers": self.bootstrap_servers,
                "error": str(e),
                "broker_count": 0,
                "topic_count": 0,
            }
