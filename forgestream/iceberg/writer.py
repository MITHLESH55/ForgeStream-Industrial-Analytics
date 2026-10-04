"""
Apache Iceberg Batch Writer.
Converts validated telemetry events into PyArrow RecordBatches matching Iceberg schema
and executes atomic snapshot commits to the historical lakehouse table.
"""

from datetime import datetime, timezone
import time
from typing import Any, Dict, List, Optional, Union
import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table
from forgestream.config import settings
from forgestream.iceberg.tables import IcebergTableManager, TELEMETRY_ICEBERG_SCHEMA
from forgestream.observability.logging import get_logger
from forgestream.observability.metrics import metrics
from forgestream.schemas.telemetry_schema import TelemetryEvent

logger = get_logger("iceberg.writer")


class IcebergHistoricalWriter:
    """Manages batch append operations to Apache Iceberg historical storage."""

    def __init__(
        self,
        table_manager: Optional[IcebergTableManager] = None,
        table: Optional[Table] = None,
    ):
        self.table_mgr = table_manager or IcebergTableManager()
        self._table = table
        self._arrow_schema = schema_to_pyarrow(TELEMETRY_ICEBERG_SCHEMA)

    @property
    def table(self) -> Table:
        if self._table is None:
            self._table = self.table_mgr.get_or_create_telemetry_table()
        return self._table

    def write_events(
        self,
        events: List[Union[TelemetryEvent, Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """
        Converts a list of telemetry events to PyArrow table and commits an append snapshot.
        """
        if not events:
            return {"records_written": 0, "snapshot_id": None, "duration_ms": 0.0}

        start_time = time.perf_counter()
        now_utc = datetime.now(timezone.utc)

        # Build column buffers
        event_ids: List[str] = []
        asset_ids: List[str] = []
        asset_types: List[str] = []
        timestamps: List[float] = []
        event_times: List[datetime] = []
        ingestion_times: List[datetime] = []
        temperatures: List[float] = []
        vibrations: List[float] = []
        pressures: List[float] = []
        rpms: List[float] = []
        currents: List[float] = []
        voltages: List[float] = []
        powers: List[float] = []
        loads: List[float] = []
        operating_modes: List[str] = []
        sequence_numbers: List[int] = []
        schema_versions: List[str] = []

        for e in events:
            if isinstance(e, TelemetryEvent):
                e_dict = e.model_dump()
            elif isinstance(e, dict):
                e_dict = e
            else:
                continue

            event_ids.append(str(e_dict.get("event_id", "")))
            asset_ids.append(str(e_dict.get("asset_id", "")))

            # Asset type
            a_type = e_dict.get("asset_type")
            asset_types.append(a_type.value if hasattr(a_type, "value") else str(a_type or "MOTOR"))

            timestamps.append(float(e_dict.get("timestamp", 0.0)))

            # Event time
            evt_time = e_dict.get("event_time")
            if isinstance(evt_time, str):
                try:
                    evt_time = datetime.fromisoformat(evt_time)
                except Exception:
                    evt_time = now_utc
            elif not isinstance(evt_time, datetime):
                evt_time = now_utc
            if evt_time.tzinfo is None:
                evt_time = evt_time.replace(tzinfo=timezone.utc)
            event_times.append(evt_time)

            # Ingestion time
            ing_time = e_dict.get("ingestion_time")
            if isinstance(ing_time, str):
                try:
                    ing_time = datetime.fromisoformat(ing_time)
                except Exception:
                    ing_time = now_utc
            elif not isinstance(ing_time, datetime):
                ing_time = now_utc
            if ing_time.tzinfo is None:
                ing_time = ing_time.replace(tzinfo=timezone.utc)
            ingestion_times.append(ing_time)

            temperatures.append(float(e_dict.get("temperature", 0.0)))
            vibrations.append(float(e_dict.get("vibration", 0.0)))
            pressures.append(float(e_dict.get("pressure", 0.0)))
            rpms.append(float(e_dict.get("rpm", 0.0)))
            currents.append(float(e_dict.get("current", 0.0)))
            voltages.append(float(e_dict.get("voltage", 0.0)))
            powers.append(float(e_dict.get("power", 0.0)))
            loads.append(float(e_dict.get("load", 0.0)))

            # Operating mode
            op_mode = e_dict.get("operating_mode")
            operating_modes.append(op_mode.value if hasattr(op_mode, "value") else str(op_mode or "NORMAL"))

            sequence_numbers.append(int(e_dict.get("sequence_number", 0)))
            schema_versions.append(str(e_dict.get("schema_version", "1.0.0")))

        # Construct PyArrow Table
        data_dict = {
            "event_id": event_ids,
            "asset_id": asset_ids,
            "asset_type": asset_types,
            "timestamp": timestamps,
            "event_time": event_times,
            "ingestion_time": ingestion_times,
            "temperature": temperatures,
            "vibration": vibrations,
            "pressure": pressures,
            "rpm": rpms,
            "current": currents,
            "voltage": voltages,
            "power": powers,
            "load": loads,
            "operating_mode": operating_modes,
            "sequence_number": sequence_numbers,
            "schema_version": schema_versions,
        }

        arrow_table = pa.Table.from_pydict(data_dict, schema=self._arrow_schema)

        # Commit append to Iceberg
        self.table.append(arrow_table)

        duration_sec = time.perf_counter() - start_time
        metrics.increment("events_persisted_iceberg", len(events))
        metrics.record_latency("iceberg_commit_latency_ms", duration_sec * 1000.0)

        current_snapshot = self.table.current_snapshot()
        snapshot_id = current_snapshot.snapshot_id if current_snapshot else None

        logger.info(
            f"Iceberg append committed: {len(events)} records, snapshot_id={snapshot_id}, duration={duration_sec*1000.0:.2f}ms"
        )

        return {
            "records_written": len(events),
            "snapshot_id": snapshot_id,
            "duration_ms": duration_sec * 1000.0,
        }
