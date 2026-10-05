"""Configuration definitions for ForgeStream Phase 3 Predictive Maintenance ML."""

from dataclasses import dataclass, field
import os
import pathlib
from typing import List, Dict, Any


@dataclass
class MLDatasetConfig:
    """Dataset generation and splitting configuration."""
    num_asset_runs: int = 50
    records_per_run: int = 600
    sample_rate_hz: float = 1.0
    failure_horizon_hours: float = 24.0  # Operational prediction horizon H (hours)
    failure_horizon_steps: int = 120    # Step equivalent for simulation runs (e.g. final 120 steps)
    max_rul_cap_hours: float = 120.0    # Piecewise linear RUL cap T_max
    train_ratio: float = 0.70
    val_ratio: float = 0.15
    test_ratio: float = 0.15
    random_seed: int = 42
    output_parquet_path: str = "data/ml/telemetry_ml_features.parquet"
    iceberg_table_name: str = "forgestream.ml_features"


@dataclass
class SparkConfig:
    """Apache Spark configuration for local distributed ML."""
    app_name: str = "ForgeStream-Predictive-Maintenance-ML"
    master: str = "local[*]"
    driver_memory: str = "3g"
    shuffle_partitions: int = 8
    extra_java_options: str = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )


@dataclass
class MLflowConfig:
    """MLflow Tracking Server and Model Registry configuration."""
    tracking_uri: str = "sqlite:///data/mlflow.db"
    experiment_name: str = "forgestream-phase3-predictive-maintenance"
    artifact_root: str = "data/mlruns_artifacts"
    enable_autologging: bool = False


@dataclass
class ModelGovernanceConfig:
    """Governance and promotion criteria for ML models."""
    min_classification_pr_auc: float = 0.70
    min_classification_recall: float = 0.80
    min_classification_roc_auc: float = 0.75
    min_regression_r2: float = 0.35
    max_regression_rmse_ratio: float = 0.30  # RMSE <= 30% of max RUL horizon
    min_accuracy_within_25pct: float = 0.50
    leakage_audit_required: bool = True


@dataclass
class FeatureRegistryConfig:
    """Feature registry listing all 25 engineered operational features."""
    raw_sensor_features: List[str] = field(default_factory=lambda: [
        "temperature",
        "vibration",
        "pressure",
        "current",
        "voltage",
        "power",
        "load_pct",
        "ambient_temp",
    ])
    categorical_features: List[str] = field(default_factory=lambda: [
        "asset_type",
        "operating_mode",
    ])
    rolling_features: List[str] = field(default_factory=lambda: [
        "rolling_mean_temp",
        "rolling_std_temp",
        "rolling_mean_vib",
        "rolling_std_vib",
        "rolling_mean_pres",
        "rolling_std_pres",
        "rolling_mean_current",
        "rolling_std_current",
        "rolling_mean_power",
        "rolling_std_power",
    ])
    rate_of_change_features: List[str] = field(default_factory=lambda: [
        "thermal_rise_rate",
        "vibration_slope",
    ])
    dimensionless_indicators: List[str] = field(default_factory=lambda: [
        "current_load_ratio",
        "pressure_instability",
    ])
    health_score_feature: List[str] = field(default_factory=lambda: [
        "health_score",
    ])

    @property
    def all_feature_names(self) -> List[str]:
        """Return full ordered list of all 25 operational feature names."""
        return (
            self.raw_sensor_features
            + self.categorical_features
            + self.rolling_features
            + self.rate_of_change_features
            + self.dimensionless_indicators
            + self.health_score_feature
        )


@dataclass
class Phase3Config:
    """Consolidated Phase 3 configuration object."""
    dataset: MLDatasetConfig = field(default_factory=MLDatasetConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    mlflow: MLflowConfig = field(default_factory=MLflowConfig)
    governance: ModelGovernanceConfig = field(default_factory=ModelGovernanceConfig)
    features: FeatureRegistryConfig = field(default_factory=FeatureRegistryConfig)


def get_default_config() -> Phase3Config:
    """Obtain default Phase 3 configuration."""
    return Phase3Config()
