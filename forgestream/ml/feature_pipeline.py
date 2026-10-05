"""Spark MLlib feature transformation pipelines and assembler stages."""

from typing import List, Tuple, Optional
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    StringIndexer,
    VectorAssembler,
    StandardScaler,
)
from forgestream.ml.config import FeatureRegistryConfig


class SparkFeaturePipelineBuilder:
    """Constructs Spark MLlib feature engineering pipelines for distributed ML."""

    def __init__(self, registry: Optional[FeatureRegistryConfig] = None):
        self.registry = registry or FeatureRegistryConfig()

    def get_assembled_feature_names(self) -> List[str]:
        """Return the exact ordered list of feature column names produced by the VectorAssembler."""
        numeric_cols = (
            self.registry.raw_sensor_features
            + self.registry.rolling_features
            + self.registry.rate_of_change_features
            + self.registry.dimensionless_indicators
            + self.registry.health_score_feature
        )
        categorical_indexed_cols = [f"{c}_idx" for c in self.registry.categorical_features]
        return numeric_cols + categorical_indexed_cols

    def build_feature_pipeline(
        self,
        output_features_col: str = "features",
        with_mean: bool = True,
        with_std: bool = True,
    ) -> Pipeline:
        """Create an unfitted Spark MLlib Pipeline consisting of Indexers, Assembler, and Scaler."""
        stages = []

        # 1. String Indexers for Categorical Features
        categorical_indexed_cols = []
        for cat_col in self.registry.categorical_features:
            indexed_col = f"{cat_col}_idx"
            indexer = StringIndexer(
                inputCol=cat_col,
                outputCol=indexed_col,
                handleInvalid="keep",
                stringOrderType="frequencyDesc",
            )
            stages.append(indexer)
            categorical_indexed_cols.append(indexed_col)

        # 2. Vector Assembler combining all numeric & indexed features
        numeric_cols = (
            self.registry.raw_sensor_features
            + self.registry.rolling_features
            + self.registry.rate_of_change_features
            + self.registry.dimensionless_indicators
            + self.registry.health_score_feature
        )
        assembler_inputs = numeric_cols + categorical_indexed_cols

        assembler = VectorAssembler(
            inputCols=assembler_inputs,
            outputCol="raw_features",
            handleInvalid="keep",
        )
        stages.append(assembler)

        # 3. Standard Scaler for normalization
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol=output_features_col,
            withMean=with_mean,
            withStd=with_std,
        )
        stages.append(scaler)

        return Pipeline(stages=stages)
