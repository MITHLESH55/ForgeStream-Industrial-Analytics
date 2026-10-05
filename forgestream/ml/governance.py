"""Model governance, promotion policy, and validation gates."""

import json
import logging
from typing import Dict, Any, Optional
from forgestream.ml.config import ModelGovernanceConfig

logger = logging.getLogger("forgestream.ml.governance")


class ModelGovernanceEngine:
    """Evaluates candidate models against strict production promotion criteria."""

    def __init__(self, config: Optional[ModelGovernanceConfig] = None):
        self.config = config or ModelGovernanceConfig()

    def evaluate_classification_model(
        self,
        model_name: str,
        test_metrics: Dict[str, Any],
        leakage_status: str = "PASS",
    ) -> Dict[str, Any]:
        """Evaluate a classification model against production promotion criteria."""
        pr_auc = test_metrics.get("pr_auc", 0.0)
        recall = test_metrics.get("recall", 0.0)
        roc_auc = test_metrics.get("roc_auc", 0.0)

        pr_auc_pass = pr_auc >= self.config.min_classification_pr_auc
        recall_pass = recall >= self.config.min_classification_recall
        roc_auc_pass = roc_auc >= self.config.min_classification_roc_auc
        leakage_pass = leakage_status == "PASS"

        is_promoted = pr_auc_pass and recall_pass and roc_auc_pass and leakage_pass
        promotion_status = "CHAMPION" if is_promoted else "CANDIDATE"

        return {
            "model_name": model_name,
            "task": "classification",
            "promotion_status": promotion_status,
            "is_promoted": is_promoted,
            "gate_evaluations": {
                "pr_auc": {
                    "value": pr_auc,
                    "threshold": self.config.min_classification_pr_auc,
                    "status": "PASS" if pr_auc_pass else "FAIL",
                },
                "recall": {
                    "value": recall,
                    "threshold": self.config.min_classification_recall,
                    "status": "PASS" if recall_pass else "FAIL",
                },
                "roc_auc": {
                    "value": roc_auc,
                    "threshold": self.config.min_classification_roc_auc,
                    "status": "PASS" if roc_auc_pass else "FAIL",
                },
                "target_leakage_audit": {
                    "value": leakage_status,
                    "required": "PASS",
                    "status": "PASS" if leakage_pass else "FAIL",
                },
            },
        }

    def evaluate_regression_model(
        self,
        model_name: str,
        test_metrics: Dict[str, Any],
        leakage_status: str = "PASS",
    ) -> Dict[str, Any]:
        """Evaluate a regression model against production promotion criteria."""
        r2 = test_metrics.get("r2", 0.0)
        rmse_ratio = test_metrics.get("rmse_to_cap_ratio", 1.0)
        acc_25pct = test_metrics.get("accuracy_within_25pct", 0.0)

        r2_pass = r2 >= self.config.min_regression_r2
        rmse_pass = rmse_ratio <= self.config.max_regression_rmse_ratio
        acc_pass = acc_25pct >= self.config.min_accuracy_within_25pct
        leakage_pass = leakage_status == "PASS"

        is_promoted = r2_pass and rmse_pass and acc_pass and leakage_pass
        promotion_status = "CHAMPION" if is_promoted else "CANDIDATE"

        return {
            "model_name": model_name,
            "task": "regression",
            "promotion_status": promotion_status,
            "is_promoted": is_promoted,
            "gate_evaluations": {
                "r2_score": {
                    "value": r2,
                    "threshold": self.config.min_regression_r2,
                    "status": "PASS" if r2_pass else "FAIL",
                },
                "rmse_to_cap_ratio": {
                    "value": rmse_ratio,
                    "threshold": self.config.max_regression_rmse_ratio,
                    "status": "PASS" if rmse_pass else "FAIL",
                },
                "accuracy_within_25pct": {
                    "value": acc_25pct,
                    "threshold": self.config.min_accuracy_within_25pct,
                    "status": "PASS" if acc_pass else "FAIL",
                },
                "target_leakage_audit": {
                    "value": leakage_status,
                    "required": "PASS",
                    "status": "PASS" if leakage_pass else "FAIL",
                },
            },
        }
