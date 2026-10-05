"""Unit tests for Phase 2 streaming event models and schema serialization."""

import json
from forgestream.streaming.schemas import (
    AssetAlertEvent,
    SystemMetricEvent,
    WindowStateEvent,
    AnomalySeverity,
    HealthState,
    MaintenancePriority,
)


def test_asset_alert_event_json_serialization():
    """Verify AssetAlertEvent serializes to JSON and matches schema constraints."""
    alert = AssetAlertEvent(
        alert_id="alert-001",
        asset_id="MOTOR-001",
        asset_type="MOTOR",
        event_time="2026-10-05T12:00:00Z",
        timestamp=1770000000.0,
        alert_timestamp=1770000000.5,
        severity=AnomalySeverity.HIGH,
        previous_health_state=HealthState.HEALTHY,
        current_health_state=HealthState.DEGRADED,
        health_score=0.62,
        is_state_transition=True,
        is_recovery=False,
        anomaly_types=["L3_BEARING_DEGRADATION"],
        reason_codes=["FAULT_L3_BEARING_FRICTION_COUPLING"],
        triggering_values={"temperature": 85.0, "vibration": 5.4},
        supporting_features={"thermal_rise_rate": 2.5, "vibration_slope": 1.2},
        maintenance_priority=MaintenancePriority.URGENT,
        schema_version="2.0.0",
    )

    json_str = alert.model_dump_json()
    data = json.loads(json_str)

    assert data["alert_id"] == "alert-001"
    assert data["severity"] == "HIGH"
    assert data["current_health_state"] == "DEGRADED"
    assert data["health_score"] == 0.62
    assert data["schema_version"] == "2.0.0"


def test_system_metric_event_json_serialization():
    """Verify SystemMetricEvent serializes cleanly."""
    metric = SystemMetricEvent(
        metric_id="met-001",
        timestamp=1770000000.0,
        event_time="2026-10-05T12:00:00Z",
        events_ingested=500,
        events_processed=500,
        anomalies_detected=12,
        alerts_emitted=3,
        active_assets_count=5,
        current_watermark=1769999995.0,
        watermark_lag_sec=5.0,
        processing_latency_p50_ms=0.15,
        processing_latency_p95_ms=0.35,
        processing_latency_p99_ms=0.65,
        throughput_events_per_sec=1250.0,
    )

    data = json.loads(metric.model_dump_json())
    assert data["events_processed"] == 500
    assert data["throughput_events_per_sec"] == 1250.0
    assert data["processing_latency_p95_ms"] == 0.35
