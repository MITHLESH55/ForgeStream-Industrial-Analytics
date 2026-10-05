"""Label engineering and target isolation for predictive maintenance ML."""

from typing import List, Dict, Any, Tuple
import numpy as np
import pandas as pd
from forgestream.ml.config import MLDatasetConfig, FeatureRegistryConfig


def compute_piecewise_linear_rul(
    trajectory_length: int,
    max_rul_cap: float = 120.0,
    time_to_failure: float = 120.0,
    has_failure: bool = True,
) -> np.ndarray:
    """Compute piecewise linear Remaining Useful Life (RUL) target vector.

    Formulation:
      y_RUL(t) = min(T_max, max(0.0, T_fail - t))

    When healthy and far from failure, RUL is capped at T_max.
    As degradation progresses past T_max horizon, RUL decreases linearly to 0.0 at failure.
    If the run does not experience failure (e.g. Normal baseline), RUL remains constant at T_max.
    """
    rul = np.full(trajectory_length, max_rul_cap, dtype=np.float64)

    if not has_failure:
        return rul

    for t in range(trajectory_length):
        remaining_steps = max(0, trajectory_length - 1 - t)
        remaining_hours = (remaining_steps / trajectory_length) * time_to_failure
        rul[t] = min(max_rul_cap, remaining_hours)

    return rul


def compute_binary_failure_risk(
    trajectory_length: int,
    horizon_steps: int = 120,
    has_failure: bool = True,
) -> np.ndarray:
    """Compute binary failure risk classification label within prediction horizon H.

    Formulation:
      y_fail(t) = 1 if (t_fail - t) <= H else 0

    If the run has no failure, all labels are 0.
    """
    labels = np.zeros(trajectory_length, dtype=np.float64)

    if not has_failure:
        return labels

    failure_step = trajectory_length - 1
    start_risk_step = max(0, failure_step - horizon_steps)
    labels[start_risk_step:] = 1.0

    return labels


def isolate_features_and_targets(
    raw_df: pd.DataFrame,
    feature_registry: FeatureRegistryConfig,
) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    """Strictly isolate operational features from target labels and ground-truth metadata.

    Guarantees zero target leakage by dropping any simulator internal states,
    scenario IDs, maintenance states, or explicit future timestamps.

    Returns:
      (X_features_df, y_classification_series, y_regression_series)
    """
    feature_cols = feature_registry.all_feature_names

    missing_cols = [col for col in feature_cols if col not in raw_df.columns]
    if missing_cols:
        raise ValueError(f"Input DataFrame is missing required feature columns: {missing_cols}")

    # Explicit whitelist selection
    X = raw_df[feature_cols].copy()
    y_clf = raw_df["label_failure"].copy()
    y_reg = raw_df["label_rul"].copy()

    return X, y_clf, y_reg
