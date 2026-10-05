"""Integration tests for Spark MLlib training and inference pipelines."""

import pytest
import pandas as pd
import numpy as np
from forgestream.ml.config import FeatureRegistryConfig
from forgestream.ml.feature_pipeline import SparkFeaturePipelineBuilder
from forgestream.ml.models.classification import create_full_classification_pipeline
from forgestream.ml.models.regression import create_full_regression_pipeline


@pytest.fixture
def mock_spark_dataset(spark_session):
    """Generate a small realistic dataset for Spark MLlib integration testing."""
    registry = FeatureRegistryConfig()
    n_samples = 60

    data = {}
    for col in (
        registry.raw_sensor_features
        + registry.rolling_features
        + registry.rate_of_change_features
        + registry.dimensionless_indicators
        + registry.health_score_feature
    ):
        data[col] = np.random.uniform(10.0, 100.0, size=n_samples)

    data["asset_type"] = ["MOTOR"] * 20 + ["PUMP"] * 20 + ["COMPRESSOR"] * 20
    data["operating_mode"] = ["STEADY_HIGH"] * 30 + ["DEGRADED"] * 30
    data["sample_weight"] = [1.0] * n_samples
    data["label_failure"] = [0.0] * 40 + [1.0] * 20
    data["label_rul"] = np.linspace(120.0, 0.0, n_samples)

    pdf = pd.DataFrame(data)
    return spark_session.createDataFrame(pdf)


def test_classification_pipeline_spark_fit_transform(spark_session, mock_spark_dataset):
    """Verify end-to-end training and prediction of Random Forest Classification pipeline."""
    pipe = create_full_classification_pipeline(
        model_type="random_forest",
        weight_col="sample_weight",
        num_trees=10,
        max_depth=4,
    )
    model = pipe.fit(mock_spark_dataset)
    predictions = model.transform(mock_spark_dataset)

    assert "probability" in predictions.columns
    assert "prediction" in predictions.columns
    assert predictions.count() == 60


def test_regression_pipeline_spark_fit_transform(spark_session, mock_spark_dataset):
    """Verify end-to-end training and prediction of Random Forest Regression pipeline."""
    pipe = create_full_regression_pipeline(
        model_type="random_forest",
        num_trees=10,
        max_depth=4,
    )
    model = pipe.fit(mock_spark_dataset)
    predictions = model.transform(mock_spark_dataset)

    assert "prediction" in predictions.columns
    assert predictions.count() == 60
