"""Unit tests for TargetLeakageAuditor in Phase 3."""

import pytest
import pandas as pd
import numpy as np
from forgestream.ml.config import FeatureRegistryConfig
from forgestream.ml.leakage_auditor import TargetLeakageAuditor


def test_target_leakage_auditor_whitelist_pass():
    """Verify whitelist audit passes on standard feature registry."""
    registry = FeatureRegistryConfig()
    auditor = TargetLeakageAuditor(registry)

    res = auditor.audit_feature_whitelist(registry.all_feature_names)
    assert res["status"] == "PASS"
    assert len(res["leaked_columns"]) == 0


def test_target_leakage_auditor_whitelist_fail():
    """Verify whitelist audit catches forbidden ground truth columns."""
    registry = FeatureRegistryConfig()
    auditor = TargetLeakageAuditor(registry)

    forbidden_cols = registry.all_feature_names + ["scenario_id", "ground_truth_rul_hours"]
    res = auditor.audit_feature_whitelist(forbidden_cols)
    assert res["status"] == "FAIL"
    assert "scenario_id" in res["leaked_columns"]
    assert "ground_truth_rul_hours" in res["leaked_columns"]


def test_target_leakage_auditor_correlations_pass():
    """Verify correlation audit passes on realistic non-leaking features."""
    registry = FeatureRegistryConfig()
    auditor = TargetLeakageAuditor(registry)

    df = pd.DataFrame({
        "temperature": np.random.uniform(50, 90, 100),
        "vibration": np.random.uniform(1, 5, 100),
        "label_failure": np.random.choice([0.0, 1.0], 100),
    })

    res = auditor.audit_feature_correlations(df, ["temperature", "vibration"], label_col="label_failure")
    assert res["status"] == "PASS"
    assert len(res["suspicious_features"]) == 0


def test_target_leakage_auditor_correlations_fail():
    """Verify correlation audit flags exact copy of label (correlation 1.0)."""
    registry = FeatureRegistryConfig()
    auditor = TargetLeakageAuditor(registry)

    labels = np.random.choice([0.0, 1.0], 100)
    df = pd.DataFrame({
        "temperature": labels,  # Leakage!
        "label_failure": labels,
    })

    res = auditor.audit_feature_correlations(df, ["temperature"], label_col="label_failure")
    assert res["status"] == "FAIL"
    assert len(res["suspicious_features"]) > 0
