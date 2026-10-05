"""Profiling script for PyFlink and domain streaming components."""

import cProfile
import json
import pstats
import time
import io
import sys
import os
import numpy as np

sys.path.insert(0, "/opt/forgestream")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from forgestream.streaming.config import StreamingConfig
from forgestream.streaming.state import AssetStreamingState
from forgestream.streaming.features import StreamingFeatureEngine
from forgestream.streaming.windows import WindowAccumulator
from forgestream.streaming.anomaly import StreamingAnomalyDetector
from forgestream.streaming.health import AssetHealthModel
from forgestream.streaming.alerts import AlertQualityEngine


def profile_pure_python_domain(num_events=5000):
    """Profile the pure Python domain intelligence logic without PyFlink overhead."""
    config = StreamingConfig()
    feature_engine = StreamingFeatureEngine(
        medium_window_sec=config.window_medium_sliding_sec,
        short_window_sec=config.window_short_tumbling_sec,
    )
    anomaly_detector = StreamingAnomalyDetector(config)
    health_model = AssetHealthModel(config)
    alert_engine = AlertQualityEngine(config)
    window_accumulator = WindowAccumulator(
        tumbling_size_sec=config.window_short_tumbling_sec,
        sliding_size_sec=config.window_medium_sliding_sec,
        sliding_step_sec=config.window_medium_slide_step_sec,
    )

    states = {
        f"MOTOR-{i:03d}": AssetStreamingState(asset_id=f"MOTOR-{i:03d}", asset_type="MOTOR", capacity=120)
        for i in range(1, 11)
    }

    base_time = 1700000000.0
    events = []
    for i in range(num_events):
        asset_id = f"MOTOR-{(i % 10) + 1:03d}"
        events.append({
            "asset_id": asset_id,
            "asset_type": "MOTOR",
            "timestamp": base_time + (i * 0.1),
            "operating_mode": "NORMAL",
            "temperature": 65.0 + np.sin(i * 0.1) * 5.0,
            "vibration": 2.0 + np.cos(i * 0.1) * 0.8,
            "pressure": 100.0,
            "current": 45.0,
            "voltage": 480.0,
            "speed": 1780.0,
            "power": 32.0,
        })

    # Warm-up (1000 events)
    for evt in events[:1000]:
        aid = evt["asset_id"]
        st = states[aid]
        ts = evt["timestamp"]
        st.update_telemetry(ts, evt, "NORMAL")
        feat = feature_engine.extract_features(st, ts, evt)
        anom = anomaly_detector.evaluate(st, feat, evt)
        hlth = health_model.evaluate_health(st, feat, anom, evt)
        wins = window_accumulator.add_event(aid, ts, {"temperature": evt["temperature"], "vibration": evt["vibration"], "pressure": evt["pressure"]}, bool(anom))
        alrt = alert_engine.evaluate_and_generate_alert(st, hlth, anom, feat, evt)

    # Measured Run
    pr = cProfile.Profile()
    pr.enable()
    t0 = time.perf_counter()

    for evt in events:
        aid = evt["asset_id"]
        st = states[aid]
        ts = evt["timestamp"]
        st.update_telemetry(ts, evt, "NORMAL")
        feat = feature_engine.extract_features(st, ts, evt)
        anom = anomaly_detector.evaluate(st, feat, evt)
        hlth = health_model.evaluate_health(st, feat, anom, evt)
        wins = window_accumulator.add_event(aid, ts, {"temperature": evt["temperature"], "vibration": evt["vibration"], "pressure": evt["pressure"]}, bool(anom))
        alrt = alert_engine.evaluate_and_generate_alert(st, hlth, anom, feat, evt)

        # State serialization / deserialization test
        s_dict = st.to_dict()
        s_str = json.dumps(s_dict)
        _ = AssetStreamingState.from_dict(json.loads(s_str))

    t1 = time.perf_counter()
    pr.disable()

    elapsed = t1 - t0
    rate = num_events / elapsed

    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
    ps.print_stats(20)

    print(f"--- Pure Python Domain Logic (with full JSON state serialization): {rate:.2f} ev/s (time: {elapsed:.3f}s for {num_events} ev) ---")
    print(s.getvalue())


if __name__ == "__main__":
    profile_pure_python_domain()
