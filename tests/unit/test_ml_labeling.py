"""Unit tests for label engineering and target isolation in Phase 3."""

import numpy as np
import pandas as pd
import pytest
from forgestream.ml.config import FeatureRegistryConfig
from forgestream.ml.labeling import (
    compute_piecewise_linear_rul,
    compute_binary_failure_risk,
    isolate_features_and_targets,
)


def test_piecewise_linear_rul_with_failure():
    """Verify piecewise linear RUL decreases monotonically and is capped at T_max."""
    traj_len = 600
    max_cap = 120.0
    rul = compute_piecewise_linear_rul(
        trajectory_length=traj_len,
        max_rul_cap=max_cap,
        time_to_failure=max_cap,
        has_failure=True,
    )
    assert len(rul) == traj_len
    assert rul[0] <= max_cap
    assert rul[-1] == 0.0
    assert np.all(np.diff(rul) <= 0.0)  # Monotonically non-increasing


def test_piecewise_linear_rul_without_failure():
    """Verify healthy runs maintain constant RUL capped at T_max."""
    traj_len = 600
    max_cap = 120.0
    rul = compute_piecewise_linear_rul(
        trajectory_length=traj_len,
        max_rul_cap=max_cap,
        time_to_failure=max_cap,
        has_failure=False,
    )
    assert len(rul) == traj_len
    assert np.all(rul == max_cap)


def test_binary_failure_risk_horizon():
    """Verify binary risk is 1.0 within horizon H and 0.0 before."""
    traj_len = 600
    horizon = 120
    labels = compute_binary_failure_risk(
        trajectory_length=traj_len,
        horizon_steps=horizon,
        has_failure=True,
    )
    assert len(labels) == traj_len
    assert np.all(labels[: traj_len - horizon - 1] == 0.0)
    assert np.all(labels[traj_len - horizon :] == 1.0)
    assert np.sum(labels) == horizon + 1


def test_binary_failure_risk_no_failure():
    """Verify no-failure runs have zero positive labels."""
    labels = compute_binary_failure_risk(600, 120, has_failure=False)
    assert np.all(labels == 0.0)


def test_isolate_features_and_targets_whitelist():
    """Verify that feature isolation drops non-whitelisted columns and ground truth metadata."""
    reg = FeatureRegistryConfig()
    data = {feat: np.random.randn(10) for feat in reg.all_feature_names}
    data["scenario_id"] = ["SCENARIO_001"] * 10
    data["maintenance_state"] = ["NORMAL"] * 10
    data["ground_truth_rul_hours"] = np.linspace(100, 0, 10)
    data["label_failure"] = np.zeros(10)
    data["label_rul"] = np.full(10, 120.0)

    df = pd.DataFrame(data)
    X, y_clf, y_reg = isolate_features_and_targets(df, reg)

    assert set(X.columns) == set(reg.all_feature_names)
    assert "scenario_id" not in X.columns
    assert "maintenance_state" not in X.columns
    assert "ground_truth_rul_hours" not in X.columns
    assert len(y_clf) == 10
    assert len(y_reg) == 10
