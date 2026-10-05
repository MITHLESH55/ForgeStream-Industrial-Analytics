"""Unit tests for ML serving contract schemas and serialization."""

import pytest
import time
from forgestream.ml.serving.contract import (
    AssetPredictionEvent,
    MaintenancePriorityEnum,
    FeatureAttribution,
    ModelPredictionOutput,
)


def test_asset_prediction_event_valid():
    """Verify that AssetPredictionEvent validates fields, bounds, and serializes to JSON."""
    event = AssetPredictionEvent(
        prediction_id="PRED-123456",
        asset_id="MOTOR-001",
        asset_type="PUMP",
        timestamp=time.time(),
        failure_probability=0.88,
        predicted_failure_risk=1,
        predicted_rul_hours=12.5,
        maintenance_priority=MaintenancePriorityEnum.EMERGENCY,
        top_contributing_features=[
            FeatureAttribution(feature="rolling_mean_vib", importance=0.45, observed_value=8.2),
            FeatureAttribution(feature="thermal_rise_rate", importance=0.25, observed_value=1.5),
        ],
        model_version="v3.0.0-champion",
        inference_latency_ms=1.42,
    )

    assert event.prediction_id == "PRED-123456"
    assert event.maintenance_priority == MaintenancePriorityEnum.EMERGENCY
    assert len(event.top_contributing_features) == 2

    # Check serialization
    json_data = event.model_dump_json()
    assert "PRED-123456" in json_data
    assert "EMERGENCY" in json_data


def test_asset_prediction_event_probability_bounds():
    """Verify failure probability enforces [0.0, 1.0] bounds."""
    with pytest.raises(Exception):
        AssetPredictionEvent(
            prediction_id="PRED-FAIL",
            asset_id="MOTOR-001",
            asset_type="PUMP",
            timestamp=time.time(),
            failure_probability=1.5,  # Invalid: > 1.0
            predicted_failure_risk=1,
            predicted_rul_hours=10.0,
            maintenance_priority=MaintenancePriorityEnum.EMERGENCY,
        )
