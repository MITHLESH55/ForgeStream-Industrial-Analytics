"""Unit tests for dataset splits and class weighting in Phase 3."""

import pytest
import pandas as pd
import numpy as np
from forgestream.ml.splits import (
    split_by_complete_runs,
    split_chronologically_by_run,
    calculate_train_class_weights,
    add_sample_weights,
)


def test_split_by_complete_runs():
    """Verify complete run splits partition unique runs without overlap."""
    runs = [f"RUN-{i:02d}" for i in range(20)]
    records = []
    for r in runs:
        for step in range(50):
            records.append({"run_id": r, "step": step, "val": np.random.randn()})

    df = pd.DataFrame(records)
    split_df = split_by_complete_runs(df, train_ratio=0.70, val_ratio=0.15, test_ratio=0.15, random_seed=42)

    assert "split" in split_df.columns
    train_runs = set(split_df[split_df["split"] == "train"]["run_id"])
    val_runs = set(split_df[split_df["split"] == "val"]["run_id"])
    test_runs = set(split_df[split_df["split"] == "test"]["run_id"])

    # Disjoint run sets
    assert len(train_runs.intersection(val_runs)) == 0
    assert len(train_runs.intersection(test_runs)) == 0
    assert len(val_runs.intersection(test_runs)) == 0
    assert len(train_runs) + len(val_runs) + len(test_runs) == 20
    assert len(train_runs) == 14  # 70% of 20
    assert len(val_runs) == 3    # 15% of 20
    assert len(test_runs) == 3   # 15% of 20


def test_calculate_train_class_weights():
    """Verify class weights are inversely proportional to class frequencies."""
    # 80 negative (0), 20 positive (1)
    labels = [0.0] * 80 + [1.0] * 20
    df = pd.DataFrame({"label_failure": labels})
    weights = calculate_train_class_weights(df, label_col="label_failure")

    assert weights[0] == pytest.approx(100.0 / (2.0 * 80), rel=1e-3)  # 0.625
    assert weights[1] == pytest.approx(100.0 / (2.0 * 20), rel=1e-3)  # 2.500
    assert weights[1] > weights[0]


def test_add_sample_weights():
    """Verify sample weights are attached accurately."""
    df = pd.DataFrame({"label_failure": [0.0, 1.0, 0.0, 1.0]})
    class_weights = {0: 0.5, 1: 2.0}
    weighted_df = add_sample_weights(df, class_weights)

    assert "sample_weight" in weighted_df.columns
    assert list(weighted_df["sample_weight"]) == [0.5, 2.0, 0.5, 2.0]
