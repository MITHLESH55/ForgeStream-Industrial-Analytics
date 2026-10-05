"""Unit tests for 3-Level explainable anomaly detection engine."""

from forgestream.streaming.config import StreamingConfig
from forgestream.streaming.schemas import AnomalySeverity
from forgestream.streaming.state import AssetStreamingState
from forgestream.streaming.features import StreamFeatures
from forgestream.streaming.anomaly import StreamingAnomalyDetector


def test_level_1_physical_limit_breaches():
    """Verify Level 1 detectors trip immediately on absolute threshold breaches."""
    config = StreamingConfig()
    detector = StreamingAnomalyDetector(config)
    state = AssetStreamingState(asset_id="MOTOR-001", asset_type="MOTOR")

    # 1. Critical Vibration Trip (> 12.0 mm/s)
    features = StreamFeatures(asset_id="MOTOR-001", timestamp=100.0)
    anomalies = detector.evaluate(
        state=state,
        features=features,
        telemetry={"temperature": 65.0, "vibration": 14.5, "pressure": 4.0, "current": 50.0},
    )
    l1_vib = [a for a in anomalies if a.anomaly_type == "L1_VIBRATION_CRITICAL_ISO"]
    assert len(l1_vib) == 1
    assert l1_vib[0].severity == AnomalySeverity.CRITICAL
    assert l1_vib[0].level == 1

    # 2. Temperature Trip (> 120.0 °C)
    anomalies_temp = detector.evaluate(
        state=state,
        features=features,
        telemetry={"temperature": 135.0, "vibration": 1.5, "pressure": 4.0, "current": 50.0},
    )
    l1_temp = [a for a in anomalies_temp if a.anomaly_type == "L1_TEMPERATURE_TRIP"]
    assert len(l1_temp) == 1
    assert l1_temp[0].severity == AnomalySeverity.CRITICAL


def test_level_2_contextual_zscore_and_thermal_rate():
    """Verify Level 2 detectors trigger on statistical z-score and thermal rise rate."""
    config = StreamingConfig()
    detector = StreamingAnomalyDetector(config)
    state = AssetStreamingState(asset_id="PUMP-001", asset_type="PUMP")

    features = StreamFeatures(
        asset_id="PUMP-001",
        timestamp=100.0,
        thermal_rise_rate=5.5,  # > 4.0 threshold
        vibration_zscore=3.8,   # > 3.0 threshold
        sample_count=10,
    )
    anomalies = detector.evaluate(
        state=state,
        features=features,
        telemetry={"temperature": 75.0, "vibration": 3.0, "pressure": 4.0, "current": 50.0},
    )
    l2_types = {a.anomaly_type for a in anomalies if a.level == 2}
    assert "L2_THERMAL_RISE_RATE" in l2_types
    assert "L2_VIBRATION_ZSCORE_DEVIATION" in l2_types


def test_level_3_physical_correlation_bearing_and_cavitation():
    """Verify Level 3 detectors identify coupled physical degradation patterns."""
    config = StreamingConfig()
    detector = StreamingAnomalyDetector(config)
    state = AssetStreamingState(asset_id="PUMP-001", asset_type="PUMP")

    # Bearing degradation: vibration > 4.5 + thermal rise rate > 1.5
    features_bearing = StreamFeatures(
        asset_id="PUMP-001",
        timestamp=100.0,
        thermal_rise_rate=2.2,
    )
    anomalies_bearing = detector.evaluate(
        state=state,
        features=features_bearing,
        telemetry={"temperature": 80.0, "vibration": 5.2, "pressure": 4.0, "current": 50.0},
    )
    l3_bearing = [a for a in anomalies_bearing if a.anomaly_type == "L3_BEARING_DEGRADATION"]
    assert len(l3_bearing) == 1
    assert l3_bearing[0].level == 3
    assert "friction" in l3_bearing[0].description.lower()
