"""
ForgeStream Central Configuration Module.
Provides configuration schemas with environment variable overrides and sensible defaults.
"""

from pathlib import Path
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SimulationConfig(BaseSettings):
    """Configuration parameters for industrial telemetry simulation."""
    seed: int = Field(default=42, description="PRNG seed for deterministic runs")
    event_rate_hz: float = Field(default=10.0, description="Events emitted per second per asset")
    asset_count: int = Field(default=5, description="Number of assets to simulate")
    duration_sec: int = Field(default=30, description="Duration of the simulation in seconds")
    inject_anomalies: bool = Field(default=True, description="Enable controlled fault injection")
    anomaly_rate: float = Field(default=0.10, description="Fraction of events exhibiting degradation/anomalies")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", env_prefix="FORGE_SIM_", extra="ignore")


class KafkaConfig(BaseSettings):
    """Configuration for Apache Kafka streaming cluster."""
    bootstrap_servers: str = Field(default="localhost:9092", description="Kafka bootstrap broker list")
    client_id: str = Field(default="forgestream-client-01", description="Client identifier")
    topic_telemetry: str = Field(default="industrial-telemetry", description="Primary telemetry stream")
    topic_maintenance: str = Field(default="maintenance-events", description="Ground truth maintenance events")
    topic_alerts: str = Field(default="asset-alerts", description="Alerts & thresholds")
    topic_quarantine: str = Field(default="asset-alerts", description="Quarantine & DLQ events")
    topic_models: str = Field(default="model-events", description="ML model lifecycle events")
    topic_metrics: str = Field(default="system-metrics", description="System telemetry metrics")
    auto_offset_reset: str = Field(default="earliest", description="Consumer start offset")
    max_retries: int = Field(default=3, description="Producer retry limit")
    retry_backoff_ms: int = Field(default=100, description="Backoff between retries")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", env_prefix="KAFKA_", extra="ignore")


class PostgresConfig(BaseSettings):
    """Configuration for PostgreSQL operational metadata store."""
    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5433, description="PostgreSQL port")
    database: str = Field(default="forgestream_db", description="Database name")
    user: str = Field(default="forgestream_user", description="Database username")
    password: str = Field(default="forgestream_secret", description="Database password")
    use_sqlite_fallback: bool = Field(default=True, description="Fallback to SQLite if Postgres unavailable")
    sqlite_db_path: str = Field(default="data/metadata.db", description="Path for SQLite metadata fallback")

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", env_prefix="POSTGRES_", extra="ignore")


class IcebergConfig(BaseSettings):
    """Configuration for Apache Iceberg lakehouse catalog and storage."""
    catalog_name: str = Field(default="forgestream_catalog", description="Iceberg catalog identifier")
    catalog_type: str = Field(default="sql", description="Catalog type (sql)")
    catalog_uri: str = Field(default="sqlite:///data/catalog.db", description="Catalog URI")
    warehouse_path: str = Field(default="file:///data/warehouse", description="Lakehouse warehouse storage path")
    namespace: str = Field(default="forgestream", description="Iceberg namespace")
    table_telemetry: str = Field(default="historical_telemetry", description="Telemetry table name")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", env_prefix="ICEBERG_", extra="ignore")


class ObservabilityConfig(BaseSettings):
    """Configuration for logging and metrics."""
    log_level: str = Field(default="INFO", description="Standard logging level")
    log_format: str = Field(default="json", description="Log format: json or text")
    metrics_enabled: bool = Field(default=True, description="Enable internal metrics collection")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", env_prefix="LOG_", extra="ignore")


class Settings(BaseSettings):
    """Root application configuration."""
    project_root: Path = Field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    sim: SimulationConfig = Field(default_factory=SimulationConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    iceberg: IcebergConfig = Field(default_factory=IcebergConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


# Global singleton settings instance
settings = Settings()
