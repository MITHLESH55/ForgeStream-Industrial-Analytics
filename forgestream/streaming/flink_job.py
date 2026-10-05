"""Apache Flink and PyFlink DataStream job topology for ForgeStream Phase 2.

This module defines the genuine Apache Flink DataStream execution topology,
including:
- Kafka source integration
- Event-time timestamp and bounded out-of-orderness watermark generation
- Keyed stream by asset_id
- Keyed state management with Flink ValueState for state preservation & checkpointing
- Multi-level explainable anomaly detection
- Deterministic health scoring & hysteresis state transitions
- 5s tumbling and 30s sliding window aggregations
- Alert quality engine with cooldown, duplicate suppression, and recovery
- Kafka sink serialization and delivery
"""

import json
import logging
import time
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone

from forgestream.streaming.config import StreamingConfig
from forgestream.streaming.schemas import (
    AssetAlertEvent,
    WindowStateEvent,
    SystemMetricEvent,
    HealthState,
    MaintenancePriority,
    AnomalySeverity,
)
from forgestream.streaming.state import AssetStreamingState
from forgestream.streaming.features import StreamingFeatureEngine
from forgestream.streaming.windows import WindowAccumulator
from forgestream.streaming.anomaly import StreamingAnomalyDetector
from forgestream.streaming.health import AssetHealthModel
from forgestream.streaming.alerts import AlertQualityEngine
from forgestream.streaming.timestamps import EventTimeExtractor

logger = logging.getLogger(__name__)


def parse_timestamp_ms(timestamp_val: Any) -> int:
    """Extract monotonic epoch milliseconds from integer, float, or ISO string."""
    if isinstance(timestamp_val, (int, float)):
        # If already seconds (e.g. 1700000000), convert to ms
        if timestamp_val < 100_000_000_000:
            return int(timestamp_val * 1000)
        return int(timestamp_val)
    elif isinstance(timestamp_val, str):
        try:
            # Try float string
            val_f = float(timestamp_val)
            if val_f < 100_000_000_000:
                return int(val_f * 1000)
            return int(val_f)
        except ValueError:
            pass

        # Try ISO format string
        clean_str = timestamp_val.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    return int(time.time() * 1000)


try:
    from pyflink.datastream import (
        StreamExecutionEnvironment,
        TimeCharacteristic,
        CheckpointConfig,
        CheckpointingMode,
    )
    from pyflink.common import WatermarkStrategy, Duration, Types
    from pyflink.common.watermark_strategy import TimestampAssigner
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
    from pyflink.datastream.state import ValueStateDescriptor
    from pyflink.datastream.connectors.kafka import (
        KafkaSource,
        KafkaOffsetsInitializer,
        KafkaSink,
        KafkaRecordSerializationSchema,
        DeliveryGuarantee,
    )

    PYFLINK_AVAILABLE = True
except ImportError:
    PYFLINK_AVAILABLE = False
    KeyedProcessFunction = object
    TimestampAssigner = object
    RuntimeContext = Any


if PYFLINK_AVAILABLE:

    class FlinkTelemetryTimestampAssigner(TimestampAssigner):
        """Extracts event-time milliseconds from JSON telemetry payload."""

        def extract_timestamp(self, value: str, record_timestamp: int) -> int:
            try:
                if isinstance(value, str):
                    data = json.loads(value)
                elif isinstance(value, dict):
                    data = value
                else:
                    return int(time.time() * 1000)

                ts_raw = data.get("timestamp") or data.get("event_time")
                return parse_timestamp_ms(ts_raw)
            except Exception:
                return int(time.time() * 1000)


    class ForgeStreamKeyedProcessFunction(KeyedProcessFunction):
        """Genuine Apache Flink KeyedProcessFunction with Flink Keyed StateBackend.

        Maintains persistent AssetStreamingState, executes 3-level anomaly detection,
        evaluates health scores with hysteresis, computes tumbling and sliding windows,
        and manages alert lifecycles inside the Flink runtime.
        """

        def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
            self.config_dict = config_dict or {}

        def open(self, runtime_context: RuntimeContext):
            """Initialize Flink ValueState and streaming intelligence engines."""
            # Flink Managed Keyed State Descriptor
            state_descriptor = ValueStateDescriptor("asset_streaming_state", Types.STRING())
            self.state = runtime_context.get_state(state_descriptor)
            self._local_states: Dict[str, AssetStreamingState] = {}
            self._update_counter: Dict[str, int] = {}

            # Reconstruct configuration and engines
            self.config = StreamingConfig(**self.config_dict)
            self.feature_engine = StreamingFeatureEngine(
                medium_window_sec=self.config.window_medium_sliding_sec,
                short_window_sec=self.config.window_short_tumbling_sec,
            )
            self.anomaly_detector = StreamingAnomalyDetector(self.config)
            self.health_model = AssetHealthModel(self.config)
            self.alert_engine = AlertQualityEngine(self.config)
            self.window_accumulator = WindowAccumulator(
                tumbling_size_sec=self.config.window_short_tumbling_sec,
                sliding_size_sec=self.config.window_medium_sliding_sec,
                sliding_step_sec=self.config.window_medium_slide_step_sec,
            )

        def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
            """Process single event with Flink managed state."""
            # 1. Parse JSON telemetry payload
            if isinstance(value, str):
                try:
                    event_dict = json.loads(value)
                except Exception as e:
                    quarantine_record = {
                        "type": "QUARANTINE",
                        "error": str(e),
                        "raw_payload": value,
                        "timestamp": time.time(),
                    }
                    yield json.dumps(quarantine_record)
                    return
            elif isinstance(value, dict):
                event_dict = value
            else:
                return

            asset_id = event_dict.get("asset_id", ctx.get_current_key())
            asset_type = event_dict.get(
                "asset_type",
                asset_id.split("-")[0] if "-" in str(asset_id) else "GENERIC",
            )
            operating_mode = event_dict.get("operating_mode", "NORMAL")

            # Extract event timestamp (seconds)
            ts_ms = parse_timestamp_ms(event_dict.get("timestamp") or event_dict.get("event_time"))
            timestamp = ts_ms / 1000.0

            # 2. Retrieve Flink Keyed State (Fast worker-local cache with ValueState backing)
            state = self._local_states.get(asset_id)
            if state is None:
                raw_state = self.state.value()
                if raw_state:
                    try:
                        state_dict = json.loads(raw_state)
                        state = AssetStreamingState.from_dict(state_dict)
                    except Exception:
                        state = AssetStreamingState(
                            asset_id=asset_id,
                            asset_type=asset_type,
                            capacity=self.config.state_buffer_capacity,
                        )
                else:
                    state = AssetStreamingState(
                        asset_id=asset_id,
                        asset_type=asset_type,
                        capacity=self.config.state_buffer_capacity,
                    )
                self._local_states[asset_id] = state

            # Initialize baselines if missing
            if state.baseline_temperature is None and "temperature" in event_dict:
                state.baseline_temperature = float(event_dict["temperature"])
            if state.baseline_vibration is None and "vibration" in event_dict:
                state.baseline_vibration = float(event_dict["vibration"])
            if state.baseline_pressure is None and "pressure" in event_dict:
                state.baseline_pressure = float(event_dict["pressure"])
            if state.baseline_current is None and "current" in event_dict:
                state.baseline_current = float(event_dict["current"])

            # 3. Update State Buffers
            state.update_telemetry(
                timestamp=timestamp,
                sensor_data=event_dict,
                operating_mode=operating_mode,
            )

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

            # 7. Evaluate Window Accumulations
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
                w_event = w.to_event()
                yield json.dumps({
                    "type": "WINDOW",
                    "topic": "forgestream.telemetry.windows",
                    "payload": w_event.model_dump(),
                })

            # 8. Alert Quality Engine (Debouncing, Suppression & Recovery)
            alert_event = self.alert_engine.evaluate_and_generate_alert(
                state=state,
                health=health_eval,
                anomalies=anomalies,
                features=features,
                telemetry=event_dict,
            )
            if alert_event:
                yield json.dumps({
                    "type": "ALERT",
                    "topic": "forgestream.telemetry.alerts",
                    "payload": alert_event.model_dump(),
                })

            # 9. Synchronize Flink Keyed State in StateBackend (Batched & On State/Alert Boundary)
            cnt = self._update_counter.get(asset_id, 0) + 1
            self._update_counter[asset_id] = cnt
            state_changed = (state.previous_health_state != health_eval.health_state)
            if (cnt % 5 == 0) or alert_event or state_changed or bool(anomalies):
                self.state.update(json.dumps(state.to_dict()))

            # 10. Emit Processed Diagnostic Record
            yield json.dumps({
                "type": "PROCESSED_EVENT",
                "asset_id": asset_id,
                "timestamp": timestamp,
                "health_score": health_eval.health_score,
                "health_state": health_eval.health_state.value,
                "anomalies_count": len(anomalies),
                "alert_emitted": bool(alert_event),
            })


def build_flink_datastream_pipeline(
    config: Optional[StreamingConfig] = None,
    kafka_bootstrap_servers: str = "kafka:29092",
    enable_checkpointing: bool = True,
    checkpoint_interval_ms: int = 2000,
    checkpoints_dir: str = "file:///opt/forgestream/data/checkpoints",
):
    """Constructs and configures the PyFlink DataStream execution environment and topology."""
    if not PYFLINK_AVAILABLE:
        logger.warning("PyFlink runtime package not available in local environment.")
        return None, None

    cfg = config or StreamingConfig()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(cfg.parallelism)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    # Configure Checkpointing for Fault-Tolerance & Exactly-Once Semantics
    if enable_checkpointing:
        env.enable_checkpointing(checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
        ckpt_cfg = env.get_checkpoint_config()
        ckpt_cfg.set_checkpoint_storage_dir(checkpoints_dir)
        ckpt_cfg.set_min_pause_between_checkpoints(500)
        ckpt_cfg.set_checkpoint_timeout(60000)
        ckpt_cfg.set_max_concurrent_checkpoints(1)

    # Kafka Source Definition
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap_servers)
        .set_topics(cfg.topic_telemetry_input)
        .set_group_id(cfg.consumer_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Bounded Out-Of-Orderness Watermark Strategy with 5s delay & 5s idleness
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(int(cfg.watermark_delay_sec))
        )
        .with_timestamp_assigner(FlinkTelemetryTimestampAssigner())
        .with_idleness(Duration.of_seconds(5))
    )

    # Ingest DataStream from Kafka
    telemetry_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=watermark_strategy,
        source_name="KafkaTelemetrySource",
    )

    # Keyed Stream by asset_id
    def extract_asset_id(raw_str: str) -> str:
        try:
            d = json.loads(raw_str)
            return str(d.get("asset_id", "GENERIC"))
        except Exception:
            return "GENERIC"

    keyed_stream = telemetry_stream.key_by(
        extract_asset_id,
        key_type=Types.STRING(),
    )

    # Keyed Processing Function with Flink Managed ValueState
    processed_stream = keyed_stream.process(
        ForgeStreamKeyedProcessFunction(cfg.model_dump()),
        output_type=Types.STRING(),
    )

    # Kafka Sink for Alerts & Processed Outputs
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(cfg.topic_alerts_output)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    processed_stream.sink_to(kafka_sink)

    logger.info("Real Apache Flink PyFlink DataStream pipeline assembled successfully.")
    return env, processed_stream


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    env, stream = build_flink_datastream_pipeline()
    if env:
        logger.info("Submitting ForgeStream PyFlink job to Apache Flink cluster...")
        env.execute("ForgeStream-RealTime-Industrial-Intelligence")
