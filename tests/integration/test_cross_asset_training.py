"""Integration tests for cross-asset and degradation scenario evaluations."""

import pytest
import pandas as pd
import numpy as np
from forgestream.ml.evaluation.cross_asset import evaluate_cross_asset_performance


def test_cross_asset_performance_slices():
    """Verify cross-asset performance evaluation slices metrics by asset_type and scenario_id."""
    records = []
    asset_types = ["MOTOR", "PUMP", "COMPRESSOR", "CONVEYOR", "TURBINE"]
    scenarios = ["SCENARIO_001_NORMAL", "SCENARIO_002_BEARING_WEAR"]

    for atype in asset_types:
        for sc in scenarios:
            for _ in range(10):
                records.append({
                    "asset_type": atype,
                    "scenario_id": sc,
                    "label_failure": 1.0 if sc != "SCENARIO_001_NORMAL" else 0.0,
                    "pred_failure": 1.0 if sc != "SCENARIO_001_NORMAL" else 0.0,
                    "prob_failure": 0.9 if sc != "SCENARIO_001_NORMAL" else 0.1,
                    "label_rul": 20.0 if sc != "SCENARIO_001_NORMAL" else 120.0,
                    "pred_rul": 22.0 if sc != "SCENARIO_001_NORMAL" else 118.0,
                })

    df = pd.DataFrame(records)
    results = evaluate_cross_asset_performance(df)

    assert "by_asset_type" in results
    assert "by_scenario" in results

    for atype in asset_types:
        assert atype in results["by_asset_type"]
        assert results["by_asset_type"][atype]["classification"]["recall"] >= 0.9

    for sc in scenarios:
        assert sc in results["by_scenario"]
