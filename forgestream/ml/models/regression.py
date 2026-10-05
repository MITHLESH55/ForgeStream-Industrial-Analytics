"""Spark MLlib regression models for Remaining Useful Life (RUL) estimation."""

from typing import Optional, Dict, Any
from pyspark.ml import Pipeline
from pyspark.ml.regression import (
    LinearRegression,
    RandomForestRegressor,
    GBTRegressor,
)
from forgestream.ml.feature_pipeline import SparkFeaturePipelineBuilder


def build_linear_regression_model(
    features_col: str = "features",
    label_col: str = "label_rul",
    weight_col: Optional[str] = None,
    reg_param: float = 0.01,
    elastic_net_param: float = 0.5,
    max_iter: int = 100,
) -> LinearRegression:
    """Instantiate baseline regularized Linear Regression model."""
    lr = LinearRegression(
        featuresCol=features_col,
        labelCol=label_col,
        regParam=reg_param,
        elasticNetParam=elastic_net_param,
        maxIter=max_iter,
    )
    if weight_col:
        lr.setWeightCol(weight_col)
    return lr


def build_random_forest_regressor(
    features_col: str = "features",
    label_col: str = "label_rul",
    weight_col: Optional[str] = None,
    num_trees: int = 50,
    max_depth: int = 8,
    seed: int = 42,
) -> RandomForestRegressor:
    """Instantiate advanced Random Forest Regressor for continuous RUL estimation."""
    rf = RandomForestRegressor(
        featuresCol=features_col,
        labelCol=label_col,
        numTrees=num_trees,
        maxDepth=max_depth,
        seed=seed,
    )
    if weight_col:
        rf.setWeightCol(weight_col)
    return rf


def build_gbt_regressor(
    features_col: str = "features",
    label_col: str = "label_rul",
    max_iter: int = 40,
    max_depth: int = 5,
    seed: int = 42,
) -> GBTRegressor:
    """Instantiate Gradient-Boosted Trees Regressor."""
    return GBTRegressor(
        featuresCol=features_col,
        labelCol=label_col,
        maxIter=max_iter,
        maxDepth=max_depth,
        seed=seed,
    )


def create_full_regression_pipeline(
    model_type: str = "random_forest",
    feature_builder: Optional[SparkFeaturePipelineBuilder] = None,
    weight_col: Optional[str] = None,
    **model_params: Any,
) -> Pipeline:
    """Construct complete Spark MLlib Pipeline including feature stages and regressor."""
    builder = feature_builder or SparkFeaturePipelineBuilder()
    feature_pipe = builder.build_feature_pipeline(output_features_col="features")

    if model_type.lower() in ["lr", "linear_regression", "baseline"]:
        regressor = build_linear_regression_model(weight_col=weight_col, **model_params)
    elif model_type.lower() in ["gbt", "gradient_boosted"]:
        regressor = build_gbt_regressor(**model_params)
    else:
        regressor = build_random_forest_regressor(weight_col=weight_col, **model_params)

    all_stages = feature_pipe.getStages() + [regressor]
    return Pipeline(stages=all_stages)
