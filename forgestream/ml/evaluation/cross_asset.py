"""Cross-asset and degradation-scenario performance evaluation slices."""

from typing import Dict, List, Any
import numpy as np
import pandas as pd
from forgestream.ml.evaluation.metrics import (
    evaluate_classification_predictions,
    evaluate_regression_predictions,
)


def evaluate_cross_asset_performance(
    df: pd.DataFrame,
    clf_pred_col: str = "pred_failure",
    clf_prob_col: str = "prob_failure",
    reg_pred_col: str = "pred_rul",
) -> Dict[str, Any]:
    """Calculate performance breakdowns across all 5 asset types and 8 degradation scenarios."""
    asset_types = ["MOTOR", "PUMP", "COMPRESSOR", "CONVEYOR", "TURBINE"]
    scenarios = [
        "SCENARIO_001_NORMAL",
        "SCENARIO_002_BEARING_WEAR",
        "SCENARIO_003_OVERHEATING",
        "SCENARIO_004_CAVITATION",
        "SCENARIO_005_ELECTRICAL_FAULT",
        "SCENARIO_006_TRANSIENT_LOAD",
        "SCENARIO_007_DELAYED_EVENTS",
        "SCENARIO_008_OUT_OF_ORDER",
    ]

    by_asset_type: Dict[str, Any] = {}
    for atype in asset_types:
        sub = df[df["asset_type"] == atype]
        if len(sub) > 0:
            clf_metrics = evaluate_classification_predictions(
                sub["label_failure"].values,
                sub[clf_pred_col].values,
                sub[clf_prob_col].values if clf_prob_col in sub.columns else None,
            )
            reg_metrics = evaluate_regression_predictions(
                sub["label_rul"].values,
                sub[reg_pred_col].values,
            )
            by_asset_type[atype] = {
                "sample_count": len(sub),
                "classification": clf_metrics,
                "regression": reg_metrics,
            }

    by_scenario: Dict[str, Any] = {}
    for sc in scenarios:
        sub = df[df["scenario_id"] == sc]
        if len(sub) > 0:
            clf_metrics = evaluate_classification_predictions(
                sub["label_failure"].values,
                sub[clf_pred_col].values,
                sub[clf_prob_col].values if clf_prob_col in sub.columns else None,
            )
            reg_metrics = evaluate_regression_predictions(
                sub["label_rul"].values,
                sub[reg_pred_col].values,
            )
            by_scenario[sc] = {
                "sample_count": len(sub),
                "classification": clf_metrics,
                "regression": reg_metrics,
            }

    return {
        "by_asset_type": by_asset_type,
        "by_scenario": by_scenario,
    }
