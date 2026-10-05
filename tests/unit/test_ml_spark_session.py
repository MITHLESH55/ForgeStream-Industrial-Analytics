"""Unit tests for SparkSession lifecycle and configuration in Phase 3."""

import pytest
from forgestream.ml.config import SparkConfig
from forgestream.ml.spark_session import get_spark_session


def test_spark_session_initialization(spark_session):
    """Verify that SparkSession initializes cleanly and can execute queries."""
    assert spark_session is not None
    assert spark_session.sparkContext is not None
    assert not spark_session.sparkContext._jsc.sc().isStopped()

    df = spark_session.createDataFrame([(1, "motor"), (2, "pump")], ["id", "asset_type"])
    assert df.count() == 2
    assert "asset_type" in df.columns


def test_spark_session_idempotent():
    """Verify that calling get_spark_session repeatedly returns the same active instance."""
    s1 = get_spark_session()
    s2 = get_spark_session()
    assert s1 is s2
