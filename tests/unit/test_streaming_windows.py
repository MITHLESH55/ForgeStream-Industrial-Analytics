"""Unit tests for tumbling and sliding event-time window aggregators."""

from forgestream.streaming.windows import WindowAccumulator


def test_window_accumulator_tumbling_and_sliding_firings():
    """Verify window accumulator closes 5s tumbling and 30s sliding windows at boundaries."""
    acc = WindowAccumulator(tumbling_size_sec=5.0, sliding_size_sec=10.0, sliding_step_sec=5.0)

    # Feed 15 events spaced 1 second apart from t=0 to t=14
    emitted_windows = []
    for t in range(15):
        ts = float(t)
        windows = acc.add_event(
            asset_id="MOTOR-001",
            timestamp=ts,
            metrics={"temperature": 60.0 + t, "vibration": 1.5, "pressure": 4.0},
            is_anomaly=(t == 7),
        )
        emitted_windows.extend(windows)

    # Check tumbling windows closed
    tumbling_windows = [w for w in emitted_windows if w.window_type == "TUMBLING"]
    sliding_windows = [w for w in emitted_windows if w.window_type == "SLIDING"]

    assert len(tumbling_windows) >= 2
    # Verify first tumbling window covers [0.0, 5.0)
    w1 = tumbling_windows[0]
    assert w1.window_start == 0.0
    assert w1.window_end == 5.0
    assert w1.sample_count == 5

    # Verify sliding windows closed
    assert len(sliding_windows) >= 1
