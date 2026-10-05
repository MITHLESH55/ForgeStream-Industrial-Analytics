"""Model evaluation, operational metrics, explainability, and cross-asset analysis."""

from forgestream.ml.evaluation.metrics import (
    evaluate_classification_predictions,
    evaluate_regression_predictions,
)
from forgestream.ml.evaluation.explainability import (
    extract_tree_feature_importances,
    extract_linear_coefficients,
)
from forgestream.ml.evaluation.cross_asset import evaluate_cross_asset_performance

__all__ = [
    "evaluate_classification_predictions",
    "evaluate_regression_predictions",
    "extract_tree_feature_importances",
    "extract_linear_coefficients",
    "evaluate_cross_asset_performance",
]
