"""Resilient Kafka and in-memory emission sinks for ForgeStream Phase 2."""

import json
import logging
from typing import List, Optional, Union, Dict, Any
from forgestream.streaming.schemas import AssetAlertEvent, SystemMetricEvent, WindowStateEvent
from forgestream.kafka.producer import ResilientKafkaProducer
from forgestream.kafka.config import KafkaClientConfig

logger = logging.getLogger(__name__)


class StreamingSink:
    """Abstract interface for streaming emission sinks."""

    def emit_alert(self, alert: AssetAlertEvent) -> None:
        raise NotImplementedError

    def emit_metric(self, metric: SystemMetricEvent) -> None:
        raise NotImplementedError

    def emit_window(self, window: WindowStateEvent) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class InMemoryStreamingSink(StreamingSink):
    """In-memory sink collector for deterministic unit tests and benchmark assertions."""

    def __init__(self):
        self.emitted_alerts: List[AssetAlertEvent] = []
        self.emitted_metrics: List[SystemMetricEvent] = []
        self.emitted_windows: List[WindowStateEvent] = []

    def emit_alert(self, alert: AssetAlertEvent) -> None:
        self.emitted_alerts.append(alert)

    def emit_metric(self, metric: SystemMetricEvent) -> None:
        self.emitted_metrics.append(metric)

    def emit_window(self, window: WindowStateEvent) -> None:
        self.emitted_windows.append(window)

    def clear(self) -> None:
        self.emitted_alerts.clear()
        self.emitted_metrics.clear()
        self.emitted_windows.clear()


class KafkaStreamingSink(StreamingSink):
    """Resilient Kafka producer sink emitting structured Phase 2 events to Kafka topics."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic_alerts: str = "asset-alerts",
        topic_metrics: str = "system-metrics",
        topic_windows: str = "model-events",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic_alerts = topic_alerts
        self.topic_metrics = topic_metrics
        self.topic_windows = topic_windows

        kafka_config = KafkaClientConfig(
            bootstrap_servers=bootstrap_servers,
            topic_telemetry="industrial-telemetry",
            topic_alerts=topic_alerts,
            topic_metrics=topic_metrics,
        )
        self.producer = ResilientKafkaProducer(config=kafka_config)
        logger.info(
            "KafkaStreamingSink initialized (Live Broker: %s)",
            self.producer.is_live,
        )

    def emit_alert(self, alert: AssetAlertEvent) -> None:
        """Publish alert event to Kafka."""
        try:
            payload = json.loads(alert.model_dump_json())
            self.producer.send(
                topic=self.topic_alerts,
                key=alert.asset_id,
                payload=payload,
            )
        except Exception as e:
            logger.error("Failed to emit alert %s to Kafka: %s", alert.alert_id, e)

    def emit_metric(self, metric: SystemMetricEvent) -> None:
        """Publish system operational metric to Kafka."""
        try:
            payload = json.loads(metric.model_dump_json())
            self.producer.send(
                topic=self.topic_metrics,
                key="system",
                payload=payload,
            )
        except Exception as e:
            logger.error("Failed to emit system metric %s to Kafka: %s", metric.metric_id, e)

    def emit_window(self, window: WindowStateEvent) -> None:
        """Publish window summary event to Kafka."""
        try:
            payload = json.loads(window.model_dump_json())
            self.producer.send(
                topic=self.topic_windows,
                key=window.asset_id,
                payload=payload,
            )
        except Exception as e:
            logger.error("Failed to emit window %s to Kafka: %s", window.window_id, e)

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.producer.flush()
