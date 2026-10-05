"""Unit tests for operational classification and regression metrics evaluation."""

import numpy as np
import pytest
from forgestream.ml.evaluation.metrics import (
    evaluate_classification_predictions,
    evaluate_regression_predictions,
    compute_asymmetric_rul_penalty,
)


def test_evaluate_classification_predictions():
    """Verify calculation of PR-AUC, ROC-AUC, Recall, Precision, and Confusion Matrix."""
    y_true = np.array([0, 0, 0, 0, 1, 1, 1, 1])
    y_pred = np.array([0, 0, 0, 1, 0, 1, 1, 1])
    y_prob = np.array([0.1, 0.2, 0.1, 0.7, 0.4, 0.8, 0.9, 0.95])

    res = evaluate_classification_predictions(y_true, y_pred, y_prob)

    assert res["recall"] == 0.75  # 3 / 4
    assert res["precision"] == 0.75  # 3 / 4
    assert res["confusion_matrix"]["tp"] == 3
    assert res["confusion_matrix"]["fp"] == 1
    assert res["confusion_matrix"]["tn"] == 3
    assert res["confusion_matrix"]["fn"] == 1
    assert res["pr_auc"] > 0.5
    assert res["roc_auc"] > 0.5


def test_evaluate_regression_predictions():
    """Verify RMSE, MAE, R^2, and tolerance envelope accuracy calculations."""
    y_true = np.array([100.0, 80.0, 50.0, 20.0, 0.0])
    y_pred = np.array([95.0, 85.0, 48.0, 22.0, 5.0])  # All errors <= 5 hours

    res = evaluate_regression_predictions(y_true, y_pred, max_rul_cap=120.0)

    assert res["mae"] == pytest.approx(3.8, rel=1e-2)
    assert res["rmse"] < 5.0
    assert res["r2"] > 0.95
    assert res["accuracy_within_10pct"] == 1.0  # Tol is 12h, all errors <= 5h
    assert res["accuracy_within_25pct"] == 1.0  # Tol is 30h


def test_asymmetric_rul_penalty_late_penalty_higher():
    """Verify that overestimating RUL (late estimation) incurs higher penalty than underestimating."""
    y_true = np.array([50.0])
    y_pred_early = np.array([40.0])  # Underestimating RUL by 10h
    y_pred_late = np.array([60.0])   # Overestimating RUL by 10h

    pen_early = compute_asymmetric_rul_penalty(y_true, y_pred_early)
    pen_late = compute_asymmetric_rul_penalty(y_true, y_pred_late)

    assert pen_late > pen_early
