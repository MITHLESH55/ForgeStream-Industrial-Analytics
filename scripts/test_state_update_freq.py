"""Test state update frequency impact on PyFlink throughput."""

import time
import json
import numpy as np
import sys
import os

sys.path.insert(0, "/opt/forgestream")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from forgestream.streaming.config import StreamingConfig
from forgestream.streaming.flink_job import (
    FlinkTelemetryTimestampAssigner,
    parse_timestamp_ms,
)
from forgestream.streaming.state import AssetStreamingState
from forgestream.streaming.features import StreamingFeatureEngine
from forgestream.streaming.anomaly import StreamingAnomalyDetector
from forgestream.streaming.health import AssetHealthModel
from forgestream.streaming.alerts import AlertQualityEngine
from forgestream.streaming.windows import WindowAccumulator


class TunedKeyedProcessFunction(KeyedProcessFunction):
    def __init__(self, config_dict=None, update_interval=5):
        self.config_dict = config_dict or {}
        self.update_interval = update_interval

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("asset_streaming_state", Types.STRING())
        self.state = runtime_context.get_state(state_descriptor)
        self._local_states = {}
        self._counter = 0

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
        if isinstance(value, str):
            event_dict = json.loads(value)
        else:
            event_dict = value

        asset_id = event_dict.get("asset_id", ctx.get_current_key())
        asset_type = event_dict.get("asset_type", "MOTOR")
        operating_mode = event_dict.get("operating_mode", "NORMAL")

        ts_ms = parse_timestamp_ms(event_dict.get("timestamp") or event_dict.get("event_time"))
        timestamp = ts_ms / 1000.0

        state = self._local_states.get(asset_id)
        if state is None:
            raw_state = self.state.value()
            if raw_state:
                try:
                    state = AssetStreamingState.from_dict(json.loads(raw_state))
                except Exception:
                    state = AssetStreamingState(asset_id=asset_id, asset_type=asset_type, capacity=120)
            else:
                state = AssetStreamingState(asset_id=asset_id, asset_type=asset_type, capacity=120)
            self._local_states[asset_id] = state

        # Initialize baselines
        if state.baseline_temperature is None and "temperature" in event_dict:
            state.baseline_temperature = float(event_dict["temperature"])
        if state.baseline_vibration is None and "vibration" in event_dict:
            state.baseline_vibration = float(event_dict["vibration"])

        state.update_telemetry(timestamp=timestamp, sensor_data=event_dict, operating_mode=operating_mode)
        features = self.feature_engine.extract_features(state=state, current_timestamp=timestamp, current_telemetry=event_dict)
        anomalies = self.anomaly_detector.evaluate(state=state, features=features, telemetry=event_dict)
        health_eval = self.health_model.evaluate_health(state=state, features=features, anomalies=anomalies, telemetry=event_dict)

        completed_windows = self.window_accumulator.add_event(
            asset_id=asset_id,
            timestamp=timestamp,
            metrics={"temperature": event_dict.get("temperature", 0.0), "vibration": event_dict.get("vibration", 0.0), "pressure": event_dict.get("pressure", 0.0)},
            is_anomaly=bool(anomalies),
        )
        for w in completed_windows:
            yield json.dumps({"type": "WINDOW", "payload": w.to_event().model_dump()})

        alert_event = self.alert_engine.evaluate_and_generate_alert(
            state=state,
            health=health_eval,
            anomalies=anomalies,
            features=features,
            telemetry=event_dict,
        )
        if alert_event:
            yield json.dumps({"type": "ALERT", "payload": alert_event.model_dump()})

        self._counter += 1
        # Synchronize to Flink ValueState periodically or on alert / state transition
        if (self._counter % self.update_interval == 0) or alert_event or (health_eval.health_state != state.current_health_state):
            self.state.update(json.dumps(state.to_dict()))

        yield json.dumps({
            "type": "PROCESSED_EVENT",
            "asset_id": asset_id,
            "timestamp": timestamp,
            "health_score": health_eval.health_score,
            "health_state": health_eval.health_state.value,
        })


def run_test(update_interval=5, num_events=10000, parallelism=4):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)

    base_time = 1700000000.0
    batch = []
    for i in range(num_events):
        aid = f"MOTOR-{(i % 10) + 1:03d}"
        batch.append(json.dumps({
            "asset_id": aid,
            "timestamp": base_time + (i * 0.01),
            "temperature": 65.0 + np.sin(i * 0.1) * 3.0,
            "vibration": 1.5 + np.cos(i * 0.1) * 0.3,
            "pressure": 100.0,
            "current": 45.0,
            "voltage": 480.0,
            "speed": 1780.0,
            "power": 32.0,
        }))

    ds = env.from_collection(batch, type_info=Types.STRING())
    wm_strat = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(FlinkTelemetryTimestampAssigner())
        .with_idleness(Duration.of_seconds(1))
    )
    timed = ds.assign_timestamps_and_watermarks(wm_strat)
    keyed = timed.key_by(lambda x: json.loads(x)["asset_id"], key_type=Types.STRING())
    processed = keyed.process(TunedKeyedProcessFunction(update_interval=update_interval), output_type=Types.STRING())

    t0 = time.perf_counter()
    cnt = 0
    with processed.execute_and_collect("Flink-UpdateInterval-Test") as it:
        for _ in it:
            cnt += 1
    t1 = time.perf_counter()

    elapsed = t1 - t0
    tps = cnt / elapsed
    print(f"Update Interval={update_interval} | Events={num_events} | Parallelism={parallelism} | Elapsed={elapsed:.3f}s | Throughput={tps:.2f} ev/s")


if __name__ == "__main__":
    for interval in [1, 5, 10, 25]:
        run_test(update_interval=interval, num_events=5000, parallelism=4)
