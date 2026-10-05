"""Core streaming execution pipeline coordinator for ForgeStream Phase 2."""

import time
import logging
from typing import Dict, List, Optional, Union, Any, Tuple
from forgestream.schemas.telemetry_schema import TelemetryEvent
from forgestream.streaming.config import StreamingConfig
from forgestream.streaming.timestamps import EventTimeExtractor
from forgestream.streaming.watermarks import (
    BoundedOutOfOrdernessWatermarkGenerator,
    LatenessPolicy,
    LatenessClassification,
)
from forgestream.streaming.state import AssetStateStore, AssetStreamingState
from forgestream.streaming.features import StreamingFeatureEngine, StreamFeatures
from forgestream.streaming.windows import WindowAccumulator, WindowResult
from forgestream.streaming.anomaly import StreamingAnomalyDetector
from forgestream.streaming.health import AssetHealthModel, HealthEvaluation
from forgestream.streaming.alerts import AlertQualityEngine
from forgestream.streaming.metrics import StreamingMetricsCollector
from forgestream.streaming.sinks import StreamingSink, InMemoryStreamingSink
from forgestream.streaming.schemas import AssetAlertEvent, SystemMetricEvent, WindowStateEvent

logger = logging.getLogger(__name__)


class StreamProcessingJob:
    """End-to-end event-time stream processing job for industrial asset health intelligence."""

    def __init__(
        self,
        config: Optional[StreamingConfig] = None,
        sink: Optional[StreamingSink] = None,
    ):
        self.config = config or StreamingConfig()
        self.sink = sink or InMemoryStreamingSink()

        # Watermark & Timestamp Policy
        lateness_policy = LatenessPolicy(
            max_out_of_orderness_sec=self.config.watermark_delay_sec,
            allowed_lateness_sec=self.config.allowed_lateness_sec,
        )
        self.watermark_generator = BoundedOutOfOrdernessWatermarkGenerator(lateness_policy)

        # Keyed State Store
        self.state_store = AssetStateStore(
            buffer_capacity=self.config.state_buffer_capacity,
            state_ttl_hours=self.config.state_ttl_hours,
        )

        # Feature Engine
        self.feature_engine = StreamingFeatureEngine(
            medium_window_sec=self.config.window_medium_sliding_sec,
            short_window_sec=self.config.window_short_tumbling_sec,
        )

        # Window Accumulator
        self.window_accumulator = WindowAccumulator(
            tumbling_size_sec=self.config.window_short_tumbling_sec,
            sliding_size_sec=self.config.window_medium_sliding_sec,
            sliding_step_sec=self.config.window_medium_slide_step_sec,
        )

        # Intelligence Engines
        self.anomaly_detector = StreamingAnomalyDetector(self.config)
        self.health_model = AssetHealthModel(self.config)
        self.alert_engine = AlertQualityEngine(self.config)

        # Operational Metrics
        self.metrics_collector = StreamingMetricsCollector(
            window_duration_sec=self.config.metric_reporting_interval_sec
        )

    def process_event(
        self,
        raw_event: Union[TelemetryEvent, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Process a single incoming telemetry event through the streaming pipeline.

        Returns:
            Dict[str, Any]: Detailed execution context including extracted features,
                            detected anomalies, health score, and emitted alerts.
        """
        start_time = time.perf_counter()

        # 1. Parse Event & Extract Monotonic Event Timestamp
        try:
            timestamp = EventTimeExtractor.extract_timestamp(raw_event)
        except Exception as e:
            self.metrics_collector.record_event_processed(
                latency_ms=(time.perf_counter() - start_time) * 1000.0,
                is_quarantined=True,
            )
            return {"status": "QUARANTINED", "error": str(e)}

        # Extract event dictionary
        if isinstance(raw_event, TelemetryEvent):
            event_dict = raw_event.model_dump()
            asset_id = raw_event.asset_id
        elif isinstance(raw_event, dict):
            event_dict = raw_event
            asset_id = raw_event.get("asset_id", "UNKNOWN-ASSET")
        else:
            event_dict = {}
            asset_id = "UNKNOWN-ASSET"

        asset_type = event_dict.get("asset_type", asset_id.split("-")[0] if "-" in asset_id else "GENERIC")
        operating_mode = event_dict.get("operating_mode", "NORMAL")

        # 2. Watermark Evaluation & Lateness Classification
        lateness_cls, current_watermark = self.watermark_generator.observe_timestamp(timestamp)

        if lateness_cls == LatenessClassification.EXCESSIVELY_LATE:
            self.metrics_collector.record_event_processed(
                latency_ms=(time.perf_counter() - start_time) * 1000.0,
                is_excessively_late=True,
            )
            return {
                "status": "EXCESSIVELY_LATE_DROPPED",
                "asset_id": asset_id,
                "timestamp": timestamp,
                "watermark": current_watermark,
            }

        is_late = (lateness_cls == LatenessClassification.LATE_ACCEPTED)

        # 3. Retrieve or Initialize Keyed Asset State
        state = self.state_store.get_or_create(asset_id=asset_id, asset_type=asset_type)

        # Initialize baselines if available
        if state.baseline_temperature is None and "temperature" in event_dict:
            state.baseline_temperature = float(event_dict["temperature"])
        if state.baseline_vibration is None and "vibration" in event_dict:
            state.baseline_vibration = float(event_dict["vibration"])
        if state.baseline_pressure is None and "pressure" in event_dict:
            state.baseline_pressure = float(event_dict["pressure"])
        if state.baseline_current is None and "current" in event_dict:
            state.baseline_current = float(event_dict["current"])

        # Update State Buffers
        state.update_telemetry(timestamp=timestamp, sensor_data=event_dict, operating_mode=operating_mode)

        # 4. Extract Real-Time Stream Features
        features = self.feature_engine.extract_features(
            state=state,
            current_timestamp=timestamp,
            current_telemetry=event_dict,
        )

        # 5. Execute 3-Level Anomaly Detection
        anomalies = self.anomaly_detector.evaluate(
            state=state,
            features=features,
            telemetry=event_dict,
        )

        # 6. Evaluate Deterministic Asset Health Model
        health_eval = self.health_model.evaluate_health(
            state=state,
            features=features,
            anomalies=anomalies,
            telemetry=event_dict,
        )

        # 7. Evaluate Windowed Analytics & Emit Window Summaries
        completed_windows = self.window_accumulator.add_event(
            asset_id=asset_id,
            timestamp=timestamp,
            metrics={
                "temperature": event_dict.get("temperature", 0.0),
                "vibration": event_dict.get("vibration", 0.0),
                "pressure": event_dict.get("pressure", 0.0),
            },
            is_anomaly=bool(anomalies),
        )
        for w in completed_windows:
            self.sink.emit_window(w.to_event())

        # 8. Alert Quality Engine (Debouncing, Suppression & Recovery)
        alert_event = self.alert_engine.evaluate_and_generate_alert(
            state=state,
            health=health_eval,
            anomalies=anomalies,
            features=features,
            telemetry=event_dict,
        )
        if alert_event:
            self.sink.emit_alert(alert_event)

        # 9. Record Operational Metrics
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        self.metrics_collector.record_event_processed(
            latency_ms=latency_ms,
            is_late=is_late,
            anomalies_count=len(anomalies),
            alert_emitted=bool(alert_event),
        )

        return {
            "status": "PROCESSED",
            "asset_id": asset_id,
            "timestamp": timestamp,
            "watermark": current_watermark,
            "lateness": lateness_cls.value,
            "health_score": health_eval.health_score,
            "health_state": health_eval.health_state.value,
            "anomalies": [a.model_dump() for a in anomalies],
            "alert_emitted": alert_event.model_dump() if alert_event else None,
            "features": features.model_dump(),
            "windows_closed": len(completed_windows),
            "latency_ms": latency_ms,
        }

    def emit_system_metrics(self) -> SystemMetricEvent:
        """Calculate and emit current system performance metrics."""
        metric_event = self.metrics_collector.calculate_metrics(
            current_watermark=self.watermark_generator.current_watermark,
            watermark_lag_sec=self.watermark_generator.watermark_lag_sec,
            active_assets_count=self.state_store.active_asset_count,
        )
        self.sink.emit_metric(metric_event)
        return metric_event

    def checkpoint_state(self) -> Dict[str, Any]:
        """Snapshot entire state for checkpoint recovery."""
        return {
            "watermark": self.watermark_generator.current_watermark,
            "max_timestamp": self.watermark_generator.max_timestamp,
            "state_store": self.state_store.snapshot(),
        }

    def restore_checkpoint(self, checkpoint_data: Dict[str, Any]) -> None:
        """Restore pipeline state from checkpoint snapshot."""
        self.watermark_generator.current_watermark = checkpoint_data.get("watermark", float("-inf"))
        self.watermark_generator.max_timestamp = checkpoint_data.get("max_timestamp", float("-inf"))
        if "state_store" in checkpoint_data:
            self.state_store.restore(checkpoint_data["state_store"])
