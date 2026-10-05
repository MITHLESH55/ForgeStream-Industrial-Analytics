"""E2E tests for Phase 3 downstream prediction scoring and Kafka payload generation."""

import pytest
import pandas as pd
import numpy as np
import time
from forgestream.ml.config import FeatureRegistryConfig
from forgestream.ml.models.classification import create_full_classification_pipeline
from forgestream.ml.models.regression import create_full_regression_pipeline
from forgestream.ml.serving.inference import SparkModelInferenceEngine
from forgestream.ml.serving.contract import MaintenancePriorityEnum


def test_spark_model_inference_engine_e2e(spark_session):
    """Verify that SparkModelInferenceEngine fits models and scores streaming DataFrame slices."""
    registry = FeatureRegistryConfig()
    n_train = 40

    data = {}
    for col in (
        registry.raw_sensor_features
        + registry.rolling_features
        + registry.rate_of_change_features
        + registry.dimensionless_indicators
        + registry.health_score_feature
    ):
        data[col] = np.random.uniform(10.0, 100.0, size=n_train)

    data["asset_type"] = ["MOTOR"] * 20 + ["PUMP"] * 20
    data["operating_mode"] = ["STEADY_HIGH"] * 40
    data["sample_weight"] = [1.0] * n_train
    data["label_failure"] = [0.0] * 30 + [1.0] * 10
    data["label_rul"] = np.linspace(120.0, 0.0, n_train)

    train_pdf = pd.DataFrame(data)
    train_spark = spark_session.createDataFrame(train_pdf)

    # Train mini-models
    clf_pipe = create_full_classification_pipeline(model_type="random_forest", num_trees=5, max_depth=3)
    clf_model = clf_pipe.fit(train_spark)

    reg_pipe = create_full_regression_pipeline(model_type="random_forest", num_trees=5, max_depth=3)
    reg_model = reg_pipe.fit(train_spark)

    # Test inference scoring
    engine = SparkModelInferenceEngine(
        spark=spark_session,
        clf_model=clf_model,
        reg_model=reg_model,
        feature_registry=registry,
        top_feature_attributions=[{"feature": "rolling_mean_temp", "importance": 0.35}],
        model_version="v3.0.0-test",
    )

    # Streaming test batch
    test_pdf = train_pdf.head(5).copy()
    test_pdf["asset_id"] = [f"MOTOR-{i:03d}" for i in range(5)]
    test_pdf["timestamp"] = [time.time() + i for i in range(5)]

    events = engine.predict_dataframe(test_pdf)

    assert len(events) == 5
    for ev in events:
        assert ev.prediction_id.startswith("PRED-")
        assert ev.model_version == "v3.0.0-test"
        assert 0.0 <= ev.failure_probability <= 1.0
        assert 0.0 <= ev.predicted_rul_hours <= 120.0
        assert ev.maintenance_priority in [
            MaintenancePriorityEnum.LOW,
            MaintenancePriorityEnum.MEDIUM,
            MaintenancePriorityEnum.HIGH,
            MaintenancePriorityEnum.EMERGENCY,
        ]
        assert ev.inference_latency_ms >= 0.0
        assert len(ev.top_contributing_features) > 0
