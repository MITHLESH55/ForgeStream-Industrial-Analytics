"""Unit tests for Spark MLlib feature transformation pipelines."""

import pytest
import pandas as pd
import numpy as np
from forgestream.ml.config import FeatureRegistryConfig
from forgestream.ml.feature_pipeline import SparkFeaturePipelineBuilder


def test_spark_feature_pipeline_builder(spark_session):
    """Verify that SparkFeaturePipelineBuilder creates valid stages and transforms data."""
    registry = FeatureRegistryConfig()
    builder = SparkFeaturePipelineBuilder(registry)

    assembled_names = builder.get_assembled_feature_names()
    assert len(assembled_names) == 25
    assert "temperature" in assembled_names
    assert "asset_type_idx" in assembled_names

    # Build mock DataFrame matching registry
    data = {}
    for col in registry.raw_sensor_features + registry.rolling_features + registry.rate_of_change_features + registry.dimensionless_indicators + registry.health_score_feature:
        data[col] = np.random.uniform(10.0, 100.0, size=20)

    data["asset_type"] = ["MOTOR"] * 10 + ["PUMP"] * 10
    data["operating_mode"] = ["STEADY_HIGH"] * 20

    pdf = pd.DataFrame(data)
    spark_df = spark_session.createDataFrame(pdf)

    pipeline = builder.build_feature_pipeline(output_features_col="scaled_features")
    model = pipeline.fit(spark_df)
    transformed_df = model.transform(spark_df)

    assert "scaled_features" in transformed_df.columns
    assert "raw_features" in transformed_df.columns
    row = transformed_df.select("scaled_features").first()
    assert len(row["scaled_features"]) == 25
