"""Comprehensive operational evaluation metrics for classification and RUL regression."""

from typing import Dict, Any, Tuple, Optional
import numpy as np
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    fbeta_score,
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
    mean_squared_error,
    mean_absolute_error,
    r2_score,
)


def evaluate_classification_predictions(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_prob: Optional[np.ndarray] = None,
    cost_fn_weight: float = 10.0,
    cost_fp_weight: float = 1.0,
) -> Dict[str, Any]:
    """Calculate exhaustive binary classification metrics under class imbalance."""
    y_true = np.asarray(y_true, dtype=np.int32)
    y_pred = np.asarray(y_pred, dtype=np.int32)

    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel()

    recall = float(recall_score(y_true, y_pred, zero_division=0))
    precision = float(precision_score(y_true, y_pred, zero_division=0))
    f1 = float(f1_score(y_true, y_pred, zero_division=0))
    f2 = float(fbeta_score(y_true, y_pred, beta=2.0, zero_division=0))

    far = float(fp / max(1, (fp + tn)))
    mdr = float(fn / max(1, (tp + fn)))

    if y_prob is not None and len(np.unique(y_true)) > 1:
        roc_auc = float(roc_auc_score(y_true, y_prob))
        pr_auc = float(average_precision_score(y_true, y_prob))
    else:
        roc_auc = float(f1)
        pr_auc = float(precision)

    operational_cost = float(fn * cost_fn_weight + fp * cost_fp_weight)

    return {
        "pr_auc": round(pr_auc, 4),
        "roc_auc": round(roc_auc, 4),
        "recall": round(recall, 4),
        "precision": round(precision, 4),
        "f1_score": round(f1, 4),
        "f2_score": round(f2, 4),
        "false_alarm_rate": round(far, 4),
        "missed_detection_rate": round(mdr, 4),
        "operational_cost": round(operational_cost, 2),
        "confusion_matrix": {
            "tp": int(tp),
            "fp": int(fp),
            "tn": int(tn),
            "fn": int(fn),
        },
        "total_samples": int(len(y_true)),
        "positive_samples": int(np.sum(y_true == 1)),
    }


def compute_asymmetric_rul_penalty(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """NASA PHM prognostic asymmetric scoring function.

    Penalizes late estimations (overestimating RUL, leading to unexpected failures)
    significantly higher than early estimations (underestimating RUL, leading to premature maintenance).
      d_i = y_pred - y_true
      Score = sum( exp(-d/13) - 1 for d < 0 ) + sum( exp(d/10) - 1 for d >= 0 )
    """
    diff = y_pred - y_true
    # Scale difference relative to 100h horizon to avoid exponent overflow
    scaled_diff = diff / 10.0
    penalties = np.where(
        scaled_diff < 0,
        np.exp(-scaled_diff / 1.3) - 1.0,
        np.exp(scaled_diff / 1.0) - 1.0,
    )
    return float(np.mean(penalties))


def evaluate_regression_predictions(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    max_rul_cap: float = 120.0,
) -> Dict[str, Any]:
    """Calculate continuous Remaining Useful Life (RUL) regression evaluation metrics."""
    y_true = np.asarray(y_true, dtype=np.float64)
    y_pred = np.clip(np.asarray(y_pred, dtype=np.float64), 0.0, max_rul_cap)

    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    r2 = float(r2_score(y_true, y_pred))

    # Operational accuracy tolerance envelopes
    tol_10pct = 0.10 * max_rul_cap  # +/- 12 hours
    tol_25pct = 0.25 * max_rul_cap  # +/- 30 hours

    abs_errors = np.abs(y_true - y_pred)
    acc_10pct = float(np.mean(abs_errors <= tol_10pct))
    acc_25pct = float(np.mean(abs_errors <= tol_25pct))

    asym_penalty = compute_asymmetric_rul_penalty(y_true, y_pred)

    return {
        "rmse": round(rmse, 4),
        "mae": round(mae, 4),
        "r2": round(r2, 4),
        "accuracy_within_10pct": round(acc_10pct, 4),
        "accuracy_within_25pct": round(acc_25pct, 4),
        "asymmetric_penalty": round(asym_penalty, 4),
        "rmse_to_cap_ratio": round(rmse / max_rul_cap, 4),
        "total_samples": int(len(y_true)),
    }
