"""
Apache Iceberg Table Schema Definition and Table Lifecycle.
Defines the PyIceberg schema for industrial telemetry, partition specifications,
and table provisioning helpers.
"""

from typing import Optional
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    DoubleType,
    LongType,
    TimestamptzType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.table import Table
from forgestream.config import settings
from forgestream.iceberg.catalog import IcebergCatalogManager
from forgestream.observability.logging import get_logger

logger = get_logger("iceberg.tables")

# Formal PyIceberg Schema for Industrial Telemetry (Schema Version 1.0.0)
TELEMETRY_ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="event_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="asset_id", field_type=StringType(), required=True),
    NestedField(field_id=3, name="asset_type", field_type=StringType(), required=True),
    NestedField(field_id=4, name="timestamp", field_type=DoubleType(), required=True),
    NestedField(field_id=5, name="event_time", field_type=TimestamptzType(), required=True),
    NestedField(field_id=6, name="ingestion_time", field_type=TimestamptzType(), required=True),
    NestedField(field_id=7, name="temperature", field_type=DoubleType(), required=True),
    NestedField(field_id=8, name="vibration", field_type=DoubleType(), required=True),
    NestedField(field_id=9, name="pressure", field_type=DoubleType(), required=True),
    NestedField(field_id=10, name="rpm", field_type=DoubleType(), required=True),
    NestedField(field_id=11, name="current", field_type=DoubleType(), required=True),
    NestedField(field_id=12, name="voltage", field_type=DoubleType(), required=True),
    NestedField(field_id=13, name="power", field_type=DoubleType(), required=True),
    NestedField(field_id=14, name="load", field_type=DoubleType(), required=True),
    NestedField(field_id=15, name="operating_mode", field_type=StringType(), required=True),
    NestedField(field_id=16, name="sequence_number", field_type=LongType(), required=True),
    NestedField(field_id=17, name="schema_version", field_type=StringType(), required=True),
)

# Identity Partitioning on asset_id
TELEMETRY_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=2,
        field_id=1000,
        transform=IdentityTransform(),
        name="asset_id",
    )
)

# Phase 4 Lakehouse Serving: ML Predictions Table Schema
PREDICTION_ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="prediction_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="asset_id", field_type=StringType(), required=True),
    NestedField(field_id=3, name="asset_type", field_type=StringType(), required=True),
    NestedField(field_id=4, name="timestamp", field_type=DoubleType(), required=True),
    NestedField(field_id=5, name="event_time", field_type=TimestamptzType(), required=True),
    NestedField(field_id=6, name="failure_probability", field_type=DoubleType(), required=True),
    NestedField(field_id=7, name="predicted_failure_risk", field_type=LongType(), required=True),
    NestedField(field_id=8, name="predicted_rul_hours", field_type=DoubleType(), required=True),
    NestedField(field_id=9, name="maintenance_priority", field_type=StringType(), required=True),
    NestedField(field_id=10, name="top_features_summary", field_type=StringType(), required=False),
    NestedField(field_id=11, name="model_version", field_type=StringType(), required=True),
    NestedField(field_id=12, name="inference_latency_ms", field_type=DoubleType(), required=False),
)

# Phase 4 Lakehouse Serving: Streaming Alerts Table Schema
ALERT_ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="alert_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="asset_id", field_type=StringType(), required=True),
    NestedField(field_id=3, name="asset_type", field_type=StringType(), required=True),
    NestedField(field_id=4, name="severity", field_type=StringType(), required=True),
    NestedField(field_id=5, name="health_state", field_type=StringType(), required=True),
    NestedField(field_id=6, name="health_score", field_type=DoubleType(), required=True),
    NestedField(field_id=7, name="is_state_transition", field_type=LongType(), required=True),
    NestedField(field_id=8, name="is_recovery", field_type=LongType(), required=True),
    NestedField(field_id=9, name="reason_codes", field_type=StringType(), required=False),
    NestedField(field_id=10, name="triggering_values", field_type=StringType(), required=False),
    NestedField(field_id=11, name="maintenance_priority", field_type=StringType(), required=True),
    NestedField(field_id=12, name="timestamp", field_type=DoubleType(), required=True),
    NestedField(field_id=13, name="event_time", field_type=TimestamptzType(), required=True),
)

# Phase 4 Lakehouse Serving: Asset Current State Snapshot Table Schema
CURRENT_STATE_ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="asset_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="asset_type", field_type=StringType(), required=True),
    NestedField(field_id=3, name="operating_mode", field_type=StringType(), required=True),
    NestedField(field_id=4, name="health_state", field_type=StringType(), required=True),
    NestedField(field_id=5, name="health_score", field_type=DoubleType(), required=True),
    NestedField(field_id=6, name="failure_probability", field_type=DoubleType(), required=True),
    NestedField(field_id=7, name="predicted_failure_risk", field_type=LongType(), required=True),
    NestedField(field_id=8, name="predicted_rul_hours", field_type=DoubleType(), required=True),
    NestedField(field_id=9, name="maintenance_priority", field_type=StringType(), required=True),
    NestedField(field_id=10, name="anomaly_level", field_type=LongType(), required=True),
    NestedField(field_id=11, name="active_alert_count", field_type=LongType(), required=True),
    NestedField(field_id=12, name="temperature", field_type=DoubleType(), required=True),
    NestedField(field_id=13, name="vibration", field_type=DoubleType(), required=True),
    NestedField(field_id=14, name="pressure", field_type=DoubleType(), required=True),
    NestedField(field_id=15, name="load", field_type=DoubleType(), required=True),
    NestedField(field_id=16, name="rpm", field_type=DoubleType(), required=True),
    NestedField(field_id=17, name="power", field_type=DoubleType(), required=True),
    NestedField(field_id=18, name="last_event_time", field_type=TimestamptzType(), required=True),
    NestedField(field_id=19, name="last_prediction_time", field_type=TimestamptzType(), required=True),
    NestedField(field_id=20, name="model_version", field_type=StringType(), required=True),
)


class IcebergTableManager:
    """Provisions and manages Apache Iceberg table schemas and metadata."""

    def __init__(
        self,
        catalog_manager: Optional[IcebergCatalogManager] = None,
        table_name: Optional[str] = None,
    ):
        self.catalog_mgr = catalog_manager or IcebergCatalogManager()
        self.table_name = table_name or settings.iceberg.table_telemetry
        self.table_identifier = f"{self.catalog_mgr.namespace}.{self.table_name}"

    def get_or_create_telemetry_table(self) -> Table:
        """Loads existing telemetry table or creates a new one with formal schema & partitioning."""
        cat = self.catalog_mgr.catalog
        if cat.table_exists(self.table_identifier):
            logger.info(f"Loaded existing Iceberg table: {self.table_identifier}")
            return cat.load_table(self.table_identifier)

        table = cat.create_table(
            identifier=self.table_identifier,
            schema=TELEMETRY_ICEBERG_SCHEMA,
            partition_spec=TELEMETRY_PARTITION_SPEC,
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.parquet.compression-level": "3",
            },
        )
        logger.info(f"Created new Iceberg table: {self.table_identifier}")
        return table

    def get_or_create_predictions_table(self, table_name: str = "asset_predictions") -> Table:
        """Loads or creates the Iceberg table for ML prediction history."""
        cat = self.catalog_mgr.catalog
        table_id = f"{self.catalog_mgr.namespace}.{table_name}"
        if cat.table_exists(table_id):
            return cat.load_table(table_id)
        return cat.create_table(
            identifier=table_id,
            schema=PREDICTION_ICEBERG_SCHEMA,
            properties={"write.format.default": "parquet", "write.parquet.compression-codec": "zstd"},
        )

    def get_or_create_alerts_table(self, table_name: str = "asset_alerts") -> Table:
        """Loads or creates the Iceberg table for streaming alert history."""
        cat = self.catalog_mgr.catalog
        table_id = f"{self.catalog_mgr.namespace}.{table_name}"
        if cat.table_exists(table_id):
            return cat.load_table(table_id)
        return cat.create_table(
            identifier=table_id,
            schema=ALERT_ICEBERG_SCHEMA,
            properties={"write.format.default": "parquet", "write.parquet.compression-codec": "zstd"},
        )

    def get_or_create_current_state_table(self, table_name: str = "asset_current_state") -> Table:
        """Loads or creates the Iceberg table for asset current state snapshot."""
        cat = self.catalog_mgr.catalog
        table_id = f"{self.catalog_mgr.namespace}.{table_name}"
        if cat.table_exists(table_id):
            return cat.load_table(table_id)
        return cat.create_table(
            identifier=table_id,
            schema=CURRENT_STATE_ICEBERG_SCHEMA,
            properties={"write.format.default": "parquet", "write.parquet.compression-codec": "zstd"},
        )

    def drop_table(self, purge: bool = False) -> None:
        """Drops the table from the catalog."""
        cat = self.catalog_mgr.catalog
        if cat.table_exists(self.table_identifier):
            cat.drop_table(self.table_identifier, purge_requested=purge)
            logger.info(f"Dropped Iceberg table: {self.table_identifier} (purge={purge})")
