"""
Apache Kafka Streaming subsystem for ForgeStream.
"""

from forgestream.kafka.config import KafkaClientConfig
from forgestream.kafka.topics import TopicManager, TOPIC_DEFINITIONS
from forgestream.kafka.producer import ResilientKafkaProducer
from forgestream.kafka.consumer import TelemetryConsumer
from forgestream.kafka.health import KafkaHealthChecker

__all__ = [
    "KafkaClientConfig",
    "TopicManager",
    "TOPIC_DEFINITIONS",
    "ResilientKafkaProducer",
    "TelemetryConsumer",
    "KafkaHealthChecker",
]
