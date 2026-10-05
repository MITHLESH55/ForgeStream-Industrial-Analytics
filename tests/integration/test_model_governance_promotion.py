"""Integration tests for Model Governance promotion gates and policy checks."""

import pytest
from forgestream.ml.config import ModelGovernanceConfig
from forgestream.ml.governance import ModelGovernanceEngine


def test_model_governance_classification_promotion_pass():
    """Verify classification promotion to CHAMPION when metrics exceed thresholds."""
    gov = ModelGovernanceEngine()
    metrics = {
        "pr_auc": 0.82,
        "recall": 0.88,
        "roc_auc": 0.85,
    }
    eval_res = gov.evaluate_classification_model("rf_classifier", metrics, leakage_status="PASS")

    assert eval_res["is_promoted"] is True
    assert eval_res["promotion_status"] == "CHAMPION"
    assert eval_res["gate_evaluations"]["pr_auc"]["status"] == "PASS"


def test_model_governance_classification_promotion_fail_recall():
    """Verify rejection when recall drops below required threshold."""
    gov = ModelGovernanceEngine()
    metrics = {
        "pr_auc": 0.85,
        "recall": 0.70,  # Below 0.80
        "roc_auc": 0.85,
    }
    eval_res = gov.evaluate_classification_model("rf_classifier", metrics, leakage_status="PASS")

    assert eval_res["is_promoted"] is False
    assert eval_res["promotion_status"] == "CANDIDATE"
    assert eval_res["gate_evaluations"]["recall"]["status"] == "FAIL"


def test_model_governance_rejection_on_leakage():
    """Verify rejection when target leakage audit fails, regardless of metric values."""
    gov = ModelGovernanceEngine()
    metrics = {
        "pr_auc": 0.99,
        "recall": 0.99,
        "roc_auc": 0.99,
    }
    eval_res = gov.evaluate_classification_model("leaked_model", metrics, leakage_status="FAIL")

    assert eval_res["is_promoted"] is False
    assert eval_res["promotion_status"] == "CANDIDATE"
    assert eval_res["gate_evaluations"]["target_leakage_audit"]["status"] == "FAIL"


def test_model_governance_regression_promotion_pass():
    """Verify regression promotion to CHAMPION when R^2 and accuracy satisfy gates."""
    gov = ModelGovernanceEngine()
    metrics = {
        "r2": 0.45,
        "rmse_to_cap_ratio": 0.25,
        "accuracy_within_25pct": 0.60,
    }
    eval_res = gov.evaluate_regression_model("rf_regressor", metrics, leakage_status="PASS")

    assert eval_res["is_promoted"] is True
    assert eval_res["promotion_status"] == "CHAMPION"
