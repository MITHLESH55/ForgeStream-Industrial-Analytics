"""Model explainability, feature attribution, and coefficient extraction."""

from typing import List, Dict, Any, Tuple
import numpy as np


def extract_tree_feature_importances(
    tree_model: Any,
    feature_names: List[str],
) -> List[Dict[str, Any]]:
    """Extract Mean Decrease Impurity (MDI) feature importances from a Spark tree ensemble."""
    if hasattr(tree_model, "featureImportances"):
        raw_importances = tree_model.featureImportances.toArray()
    else:
        raw_importances = np.zeros(len(feature_names))

    ranked = []
    for idx, name in enumerate(feature_names):
        val = float(raw_importances[idx]) if idx < len(raw_importances) else 0.0
        ranked.append({"feature": name, "importance": round(val, 6)})

    ranked.sort(key=lambda x: x["importance"], reverse=True)
    return ranked


def extract_linear_coefficients(
    linear_model: Any,
    feature_names: List[str],
) -> List[Dict[str, Any]]:
    """Extract standardized coefficients and intercept from a Spark linear model."""
    if hasattr(linear_model, "coefficients"):
        raw_coeffs = linear_model.coefficients.toArray()
    else:
        raw_coeffs = np.zeros(len(feature_names))

    ranked = []
    for idx, name in enumerate(feature_names):
        val = float(raw_coeffs[idx]) if idx < len(raw_coeffs) else 0.0
        ranked.append({
            "feature": name,
            "coefficient": round(val, 6),
            "abs_coefficient": round(abs(val), 6),
        })

    ranked.sort(key=lambda x: x["abs_coefficient"], reverse=True)
    return ranked
