"""Unit tests for deterministic asset health modeling, hysteresis transitions, and alert gating."""

from forgestream.streaming.config import StreamingConfig
from forgestream.streaming.schemas import (
    HealthState,
    AnomalySeverity,
    MaintenancePriority,
    AnomalyRecord,
)
from forgestream.streaming.state import AssetStreamingState
from forgestream.streaming.features import StreamFeatures
from forgestream.streaming.health import AssetHealthModel
from forgestream.streaming.alerts import AlertQualityEngine


def test_asset_health_model_hysteresis_and_degradation():
    """Verify health score degrades properly and obeys asymmetric hysteresis state boundaries."""
    config = StreamingConfig()
    health_model = AssetHealthModel(config)
    state = AssetStreamingState(asset_id="MOTOR-001", asset_type="MOTOR")

    features = StreamFeatures(asset_id="MOTOR-001", timestamp=100.0)

    # 1. Healthy baseline
    eval_healthy = health_model.evaluate_health(
        state=state,
        features=features,
        anomalies=[],
        telemetry={"temperature": 60.0, "vibration": 1.5, "current": 50.0, "pressure": 4.0},
    )
    assert eval_healthy.health_state == HealthState.HEALTHY
    assert eval_healthy.health_score > 0.95
    assert eval_healthy.maintenance_priority == MaintenancePriority.NORMAL

    # 2. Moderate vibration degradation -> Drops to WATCH (< 0.85)
    eval_watch = health_model.evaluate_health(
        state=state,
        features=features,
        anomalies=[],
        telemetry={"temperature": 70.0, "vibration": 5.5, "current": 50.0, "pressure": 4.0},
    )
    assert eval_watch.health_state == HealthState.WATCH
    assert eval_watch.is_state_transition is True

    # 3. Severe degradation with Level 1 trip -> Drops to CRITICAL (< 0.40)
    l1_anomaly = AnomalyRecord(
        anomaly_id="ano-1",
        anomaly_type="L1_VIBRATION_CRITICAL_ISO",
        level=1,
        severity=AnomalySeverity.CRITICAL,
        reason_code="ERR_L1_VIB_ZONE_D",
        description="Trip",
        triggering_value=14.0,
        threshold_value=12.0,
        timestamp=102.0,
    )
    eval_crit = health_model.evaluate_health(
        state=state,
        features=features,
        anomalies=[l1_anomaly],
        telemetry={"temperature": 85.0, "vibration": 14.0, "current": 60.0, "pressure": 4.0},
    )
    assert eval_crit.health_state == HealthState.CRITICAL
    assert eval_crit.maintenance_priority == MaintenancePriority.EMERGENCY


def test_alert_quality_engine_cooldown_and_recovery():
    """Verify alert engine suppresses redundant alerts in cooldown, but always emits on state transition and recovery."""
    config = StreamingConfig(alert_cooldown_sec=30.0)
    alert_engine = AlertQualityEngine(config)
    health_model = AssetHealthModel(config)
    state = AssetStreamingState(asset_id="MOTOR-001", asset_type="MOTOR")

    features = StreamFeatures(asset_id="MOTOR-001", timestamp=100.0)

    # 1. State transition from HEALTHY to WATCH at t=100 -> Alert emitted
    eval_watch = health_model.evaluate_health(
        state=state,
        features=features,
        anomalies=[],
        telemetry={"temperature": 70.0, "vibration": 5.5, "current": 50.0, "pressure": 4.0},
    )
    alert1 = alert_engine.evaluate_and_generate_alert(
        state=state,
        health=eval_watch,
        anomalies=[],
        features=features,
        telemetry={"temperature": 70.0, "vibration": 5.5},
    )
    assert alert1 is not None
    assert alert1.is_state_transition is True

    # 2. Same condition at t=105 (within 30s cooldown) -> Suppressed
    features_t105 = StreamFeatures(asset_id="MOTOR-001", timestamp=105.0)
    eval_watch_same = health_model.evaluate_health(
        state=state,
        features=features_t105,
        anomalies=[],
        telemetry={"temperature": 70.0, "vibration": 5.5, "current": 50.0, "pressure": 4.0},
    )
    alert2 = alert_engine.evaluate_and_generate_alert(
        state=state,
        health=eval_watch_same,
        anomalies=[],
        features=features_t105,
        telemetry={"temperature": 70.0, "vibration": 5.5},
    )
    assert alert2 is None  # Suppressed by cooldown

    # 3. Recovery back to HEALTHY at t=110 -> Alert emitted with is_recovery=True
    features_t110 = StreamFeatures(asset_id="MOTOR-001", timestamp=110.0)
    eval_healthy = health_model.evaluate_health(
        state=state,
        features=features_t110,
        anomalies=[],
        telemetry={"temperature": 60.0, "vibration": 1.5, "current": 50.0, "pressure": 4.0},
    )
    alert3 = alert_engine.evaluate_and_generate_alert(
        state=state,
        health=eval_healthy,
        anomalies=[],
        features=features_t110,
        telemetry={"temperature": 60.0, "vibration": 1.5},
    )
    assert alert3 is not None
    assert alert3.is_recovery is True
