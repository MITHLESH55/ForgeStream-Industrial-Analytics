"""E2E test for complete Phase 3 ML lifecycle from simulation replay to governance."""

import pytest
import os
import pandas as pd
import numpy as np
from forgestream.simulator.asset_models import get_default_asset_list
from forgestream.schemas.telemetry_schema import ScenarioID
from forgestream.ml.config import MLDatasetConfig, FeatureRegistryConfig
from forgestream.ml.dataset_generator import MLDatasetGenerator
from forgestream.ml.leakage_auditor import TargetLeakageAuditor
from forgestream.ml.splits import split_by_complete_runs, calculate_train_class_weights, add_sample_weights
from forgestream.ml.models.classification import create_full_classification_pipeline
from forgestream.ml.evaluation.metrics import evaluate_classification_predictions
from forgestream.ml.governance import ModelGovernanceEngine


def test_phase3_ml_lifecycle_end_to_end(spark_session, temp_data_dir):
    """Verify end-to-end Phase 3 machine learning workflow on a multi-asset subset."""
    config = MLDatasetConfig(num_asset_runs=4, records_per_run=50)
    registry = FeatureRegistryConfig()
    generator = MLDatasetGenerator(config, registry)

    profiles = get_default_asset_list()[:2]
    scenarios = [ScenarioID.SCENARIO_001, ScenarioID.SCENARIO_002]

    dfs = []
    for i, profile in enumerate(profiles):
        for j, sc in enumerate(scenarios):
            run_id = f"E2E-RUN-{i}-{j}"
            df_run = generator.generate_asset_trajectory(
                profile=profile,
                scenario_id=sc,
                run_id=run_id,
                num_steps=50,
                random_seed=100 + i * 10 + j,
            )
            dfs.append(df_run)

    full_df = pd.concat(dfs, ignore_index=True)
    assert len(full_df) == 200

    # 1. Target Leakage Audit
    auditor = TargetLeakageAuditor(registry)
    leakage_res = auditor.audit_feature_whitelist(registry.all_feature_names)
    assert leakage_res["status"] == "PASS"

    # 2. Chronological complete run splitting
    split_df = split_by_complete_runs(full_df, train_ratio=0.5, val_ratio=0.25, test_ratio=0.25, random_seed=42)
    train_split = split_df[split_df["split"] == "train"]
    test_split = split_df[split_df["split"] == "test"]
    assert len(train_split) > 0
    assert len(test_split) > 0

    # 3. Class Imbalance Weighting
    class_weights = calculate_train_class_weights(train_split, label_col="label_failure")
    split_df = add_sample_weights(split_df, class_weights, label_col="label_failure")

    # 4. Spark MLlib Training
    train_spark = spark_session.createDataFrame(split_df[split_df["split"] == "train"])
    test_spark = spark_session.createDataFrame(split_df[split_df["split"] == "test"])

    pipe = create_full_classification_pipeline(
        model_type="random_forest",
        weight_col="sample_weight",
        num_trees=5,
        max_depth=3,
    )
    model = pipe.fit(train_spark)

    # 5. Prediction & Evaluation
    predictions = model.transform(test_spark)
    pred_pdf = predictions.toPandas()

    y_true = pred_pdf["label_failure"].values
    y_pred = pred_pdf["prediction"].values
    y_prob = np.array([float(p[1]) for p in pred_pdf["probability"]])

    metrics = evaluate_classification_predictions(y_true, y_pred, y_prob)
    assert "pr_auc" in metrics
    assert "recall" in metrics
    assert "confusion_matrix" in metrics

    # 6. Governance Evaluation
    gov = ModelGovernanceEngine()
    gov_res = gov.evaluate_classification_model("test_e2e_model", metrics, leakage_status=leakage_res["status"])
    assert gov_res["promotion_status"] in ["CHAMPION", "CANDIDATE"]
