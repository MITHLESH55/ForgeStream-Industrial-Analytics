"""Spark MLlib binary classification models for failure risk prediction."""

from typing import Optional, Dict, Any
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    GBTClassifier,
)
from forgestream.ml.feature_pipeline import SparkFeaturePipelineBuilder


def build_logistic_regression_classifier(
    features_col: str = "features",
    label_col: str = "label_failure",
    weight_col: Optional[str] = "sample_weight",
    reg_param: float = 0.01,
    elastic_net_param: float = 0.5,
    max_iter: int = 100,
) -> LogisticRegression:
    """Instantiate baseline regularized Logistic Regression classifier."""
    lr = LogisticRegression(
        featuresCol=features_col,
        labelCol=label_col,
        regParam=reg_param,
        elasticNetParam=elastic_net_param,
        maxIter=max_iter,
    )
    if weight_col:
        lr.setWeightCol(weight_col)
    return lr


def build_random_forest_classifier(
    features_col: str = "features",
    label_col: str = "label_failure",
    weight_col: Optional[str] = "sample_weight",
    num_trees: int = 50,
    max_depth: int = 8,
    seed: int = 42,
) -> RandomForestClassifier:
    """Instantiate advanced Random Forest classifier with tree ensemble MDI."""
    rf = RandomForestClassifier(
        featuresCol=features_col,
        labelCol=label_col,
        numTrees=num_trees,
        maxDepth=max_depth,
        seed=seed,
    )
    if weight_col:
        rf.setWeightCol(weight_col)
    return rf


def build_gbt_classifier(
    features_col: str = "features",
    label_col: str = "label_failure",
    max_iter: int = 40,
    max_depth: int = 5,
    seed: int = 42,
) -> GBTClassifier:
    """Instantiate Gradient-Boosted Trees classifier."""
    return GBTClassifier(
        featuresCol=features_col,
        labelCol=label_col,
        maxIter=max_iter,
        maxDepth=max_depth,
        seed=seed,
    )


def create_full_classification_pipeline(
    model_type: str = "random_forest",
    feature_builder: Optional[SparkFeaturePipelineBuilder] = None,
    weight_col: Optional[str] = "sample_weight",
    **model_params: Any,
) -> Pipeline:
    """Construct complete Spark MLlib Pipeline including feature stages and classifier."""
    builder = feature_builder or SparkFeaturePipelineBuilder()
    feature_pipe = builder.build_feature_pipeline(output_features_col="features")

    if model_type.lower() in ["lr", "logistic_regression", "baseline"]:
        classifier = build_logistic_regression_classifier(weight_col=weight_col, **model_params)
    elif model_type.lower() in ["gbt", "gradient_boosted"]:
        classifier = build_gbt_classifier(**model_params)
    else:
        classifier = build_random_forest_classifier(weight_col=weight_col, **model_params)

    all_stages = feature_pipe.getStages() + [classifier]
    return Pipeline(stages=all_stages)
