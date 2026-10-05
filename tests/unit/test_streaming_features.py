"""Unit tests for online streaming feature engineering and physical derivatives."""

from forgestream.streaming.state import AssetStreamingState
from forgestream.streaming.features import StreamingFeatureEngine


def test_streaming_feature_engine_mean_std_and_thermal_slope():
    """Verify feature engine computes rolling mean, standard deviation, and thermal slope."""
    engine = StreamingFeatureEngine(medium_window_sec=30.0, short_window_sec=5.0)
    state = AssetStreamingState(asset_id="MOTOR-001", asset_type="MOTOR", capacity=60)
    state.baseline_current = 50.0

    # Inject a 10-second thermal ramp: 60°C at t=0 up to 70°C at t=10 (+1.0°C/sec = +60°C/min)
    for i in range(11):
        t = 100.0 + i
        temp = 60.0 + (i * 1.0)
        vib = 1.5 + (0.1 * (i % 2))
        state.update_telemetry(
            timestamp=t,
            sensor_data={"temperature": temp, "vibration": vib, "current": 50.0, "pressure": 4.0},
        )

    features = engine.extract_features(
        state=state,
        current_timestamp=110.0,
        current_telemetry={"temperature": 70.0, "vibration": 1.5, "current": 55.0, "pressure": 4.0},
    )

    assert features.asset_id == "MOTOR-001"
    assert features.sample_count == 11
    assert 64.0 < features.mean_temperature < 66.0
    assert features.std_temperature > 3.0
    # Slope should be ~60°C/min
    assert 55.0 < features.thermal_rise_rate < 65.0
    assert features.current_ratio == 1.10  # 55.0 / 50.0
