"""
Unit tests for ground-truth isolation and ML target leakage prevention.
"""

import pytest
from datetime import datetime, timezone
from forgestream.schemas.telemetry_schema import (
    TelemetryEvent,
    TelemetryMetadata,
    AssetType,
    OperatingMode,
    MaintenanceState,
    ScenarioID,
)


def test_ground_truth_isolation_in_features():
    """Proves that feature dictionaries never contain ground-truth target labels."""
    metadata = TelemetryMetadata(
        scenario_id=ScenarioID.SCENARIO_002,
        maintenance_state=MaintenanceState.CRITICAL,
        is_synthetic=True,
        ground_truth_rul_hours=12.5,
    )

    event = TelemetryEvent(
        event_id="evt-motor-00000001",
        asset_id="MOTOR-001",
        asset_type=AssetType.MOTOR,
        timestamp=1775347200.0,
        event_time=datetime(2026, 10, 5, 12, 0, 0, tzinfo=timezone.utc),
        temperature=85.4,
        vibration=4.2,
        pressure=1.0,
        rpm=1750.0,
        current=55.2,
        voltage=400.0,
        power=30.1,
        load=95.0,
        operating_mode=OperatingMode.DEGRADED,
        sequence_number=1,
        metadata=metadata,
    )

    features = event.to_feature_dict()

    # Assert ground truth fields are absent from feature extraction
    assert "scenario_id" not in features
    assert "maintenance_state" not in features
    assert "ground_truth_rul_hours" not in features
    assert "metadata" not in features

    # Assert raw telemetry features are present
    assert features["temperature"] == 85.4
    assert features["vibration"] == 4.2
    assert features["power"] == 30.1
