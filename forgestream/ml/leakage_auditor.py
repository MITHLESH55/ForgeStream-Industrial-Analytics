"""Target leakage audit and causality verification engine for predictive maintenance ML."""

import json
import logging
from typing import Dict, List, Any, Tuple, Optional
import numpy as np
import pandas as pd
from forgestream.ml.config import FeatureRegistryConfig

logger = logging.getLogger("forgestream.ml.leakage_auditor")

FORBIDDEN_METADATA_COLUMNS = [
    "scenario_id",
    "scenario_name",
    "maintenance_state",
    "degradation_stage",
    "ground_truth_rul_hours",
    "ground_truth_rul",
    "has_failure",
    "label_failure",
    "label_rul",
    "failure_time",
    "target",
    "label",
    "is_synthetic",
]


class TargetLeakageAuditor:
    """Automated validator ensuring absolute target isolation and zero lookahead bias."""

    def __init__(self, feature_registry: FeatureRegistryConfig):
        self.feature_registry = feature_registry

    def audit_feature_whitelist(self, feature_cols: List[str]) -> Dict[str, Any]:
        """Verify feature set contains zero forbidden ground-truth metadata columns."""
        leaked_columns = [
            c for c in feature_cols
            if any(forbidden in c.lower() for forbidden in FORBIDDEN_METADATA_COLUMNS)
        ]

        passed = len(leaked_columns) == 0
        return {
            "check": "feature_whitelist",
            "status": "PASS" if passed else "FAIL",
            "evaluated_feature_count": len(feature_cols),
            "leaked_columns": leaked_columns,
            "message": "Zero forbidden ground-truth columns detected." if passed else f"Forbidden columns in feature set: {leaked_columns}",
        }

    def audit_temporal_ordering(self, df: pd.DataFrame, run_col: str = "run_id") -> Dict[str, Any]:
        """Verify monotonic timestamp progression and non-overlapping train/val/test splits per run."""
        violations = []

        for run_id, group in df.groupby(run_col):
            # Check monotonic time
            times = group["timestamp"].values
            if not np.all(np.diff(times) >= 0):
                violations.append(f"Run {run_id} has non-monotonic timestamps")

            # Check split ordering
            train_t = group[group["split"] == "train"]["timestamp"].values
            val_t = group[group["split"] == "val"]["timestamp"].values
            test_t = group[group["split"] == "test"]["timestamp"].values

            if len(train_t) > 0 and len(val_t) > 0:
                if train_t.max() > val_t.min():
                    violations.append(f"Run {run_id} train split overlaps with val split")

            if len(val_t) > 0 and len(test_t) > 0:
                if val_t.max() > test_t.min():
                    violations.append(f"Run {run_id} val split overlaps with test split")

        passed = len(violations) == 0
        return {
            "check": "temporal_ordering",
            "status": "PASS" if passed else "FAIL",
            "evaluated_runs_count": df[run_col].nunique(),
            "temporal_violations": violations,
            "message": "Strict chronological temporal separation verified." if passed else f"Temporal violations found: {violations[:5]}",
        }

    def audit_feature_correlations(
        self,
        df: pd.DataFrame,
        feature_cols: List[str],
        label_col: str = "label_failure",
        max_allowed_corr: float = 0.999,
    ) -> Dict[str, Any]:
        """Scan feature-target correlations to detect artificial or direct leakage."""
        correlations: Dict[str, float] = {}
        suspicious_features: List[str] = []

        numeric_features = [c for c in feature_cols if pd.api.types.is_numeric_dtype(df[c])]
        for col in numeric_features:
            std = df[col].std()
            if std > 1e-6:
                corr = float(np.abs(np.corrcoef(df[col], df[label_col])[0, 1]))
                correlations[col] = round(corr, 4)
                if corr >= max_allowed_corr:
                    suspicious_features.append(f"{col} (r={corr:.4f})")
            else:
                correlations[col] = 0.0

        passed = len(suspicious_features) == 0
        return {
            "check": "feature_correlations",
            "status": "PASS" if passed else "FAIL",
            "max_feature_correlation": max(correlations.values()) if correlations else 0.0,
            "suspicious_features": suspicious_features,
            "feature_correlations": correlations,
            "message": "No artificial 1.0 correlations detected." if passed else f"Potential target leakage in: {suspicious_features}",
        }

    def run_full_audit(
        self,
        df: pd.DataFrame,
        output_json_path: Optional[str] = "results/phase3_leakage_audit.json",
    ) -> Dict[str, Any]:
        """Execute comprehensive target leakage audit and return structured audit report."""
        feature_cols = self.feature_registry.all_feature_names

        whitelist_res = self.audit_feature_whitelist(feature_cols)
        temporal_res = self.audit_temporal_ordering(df)
        corr_res = self.audit_feature_correlations(df, feature_cols, label_col="label_failure")

        overall_status = "PASS" if (
            whitelist_res["status"] == "PASS"
            and temporal_res["status"] == "PASS"
            and corr_res["status"] == "PASS"
        ) else "FAIL"

        audit_report = {
            "audit_name": "ForgeStream Phase 3 Target Leakage & Causality Audit",
            "overall_status": overall_status,
            "checks": {
                "feature_whitelist": whitelist_res,
                "temporal_ordering": temporal_res,
                "feature_correlations": corr_res,
            },
            "summary": {
                "total_records_audited": len(df),
                "total_features_audited": len(feature_cols),
                "isolated_splits": list(df["split"].unique()) if "split" in df.columns else [],
            }
        }

        if output_json_path:
            import os
            os.makedirs(os.path.dirname(output_json_path), exist_ok=True)
            with open(output_json_path, "w") as f:
                json.dump(audit_report, f, indent=2)
            logger.info("Saved target leakage audit report to %s", output_json_path)

        return audit_report
