"""Integration tests for MLflow tracking, parameter logging, and artifact persistence."""

import os
import pytest
import mlflow
from forgestream.ml.config import MLflowConfig
from forgestream.ml.tracking import MLflowTracker


def test_mlflow_tracker_logging(temp_data_dir):
    """Verify that MLflowTracker logs parameters, metrics across splits, and creates valid run records."""
    db_path = temp_data_dir / "test_mlflow.db"
    config = MLflowConfig(
        tracking_uri=f"sqlite:///{db_path}",
        experiment_name="test-experiment",
    )

    tracker = MLflowTracker(config)

    run_id = tracker.log_model_run(
        run_name="test_rf_run",
        model_type="random_forest",
        task="classification",
        params={"num_trees": 20, "max_depth": 5},
        train_metrics={"pr_auc": 0.95, "recall": 0.92},
        val_metrics={"pr_auc": 0.88, "recall": 0.85},
        test_metrics={"pr_auc": 0.82, "recall": 0.80},
        feature_attributions=[{"feature": "temperature", "importance": 0.35}],
    )

    assert run_id is not None

    # Query logged run from MLflow
    client = mlflow.tracking.MlflowClient(tracking_uri=config.tracking_uri)
    run = client.get_run(run_id)

    assert run.data.params["num_trees"] == "20"
    assert run.data.metrics["test_pr_auc"] == 0.82
    assert run.data.tags["task"] == "classification"
