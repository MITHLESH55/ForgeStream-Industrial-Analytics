"""
Apache Iceberg Historical Lakehouse Reader.
Provides querying, verification scanning, projection, and asset telemetry extraction.
"""

from typing import Any, Dict, List, Optional
import pyarrow as pa
from pyiceberg.expressions import EqualTo, GreaterThanOrEqual, LessThanOrEqual, And
from pyiceberg.table import Table
from forgestream.config import settings
from forgestream.iceberg.tables import IcebergTableManager
from forgestream.observability.logging import get_logger

logger = get_logger("iceberg.reader")


class IcebergHistoricalReader:
    """Provides analytical and verification read access to Iceberg telemetry tables."""

    def __init__(
        self,
        table_manager: Optional[IcebergTableManager] = None,
        table: Optional[Table] = None,
    ):
        self.table_mgr = table_manager or IcebergTableManager()
        self._table = table

    @property
    def table(self) -> Table:
        if self._table is None:
            self._table = self.table_mgr.get_or_create_telemetry_table()
        return self._table

    def get_total_row_count(self) -> int:
        """Returns total records stored in the Iceberg table."""
        try:
            arrow_data = self.table.scan().to_arrow()
            return len(arrow_data)
        except Exception as e:
            logger.error(f"Error scanning Iceberg table: {e}")
            return 0

    def scan_to_arrow(
        self,
        asset_id: Optional[str] = None,
        min_timestamp: Optional[float] = None,
        max_timestamp: Optional[float] = None,
        selected_fields: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pa.Table:
        """
        Executes a scan with optional predicates and projections, returning a PyArrow Table.
        """
        scan = self.table.scan(selected_fields=selected_fields or ("*"))

        # Build filter expressions if supplied
        filters = []
        if asset_id is not None:
            filters.append(EqualTo("asset_id", asset_id))
        if min_timestamp is not None:
            filters.append(GreaterThanOrEqual("timestamp", min_timestamp))
        if max_timestamp is not None:
            filters.append(LessThanOrEqual("timestamp", max_timestamp))

        if filters:
            combined_filter = filters[0]
            for f in filters[1:]:
                combined_filter = And(combined_filter, f)
            scan = scan.filter(combined_filter)

        arrow_table = scan.to_arrow()
        if limit is not None and limit > 0:
            arrow_table = arrow_table.slice(0, limit)

        return arrow_table

    def scan_to_dicts(
        self,
        asset_id: Optional[str] = None,
        min_timestamp: Optional[float] = None,
        max_timestamp: Optional[float] = None,
        selected_fields: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Scans records and returns standard Python dictionaries."""
        arrow_tbl = self.scan_to_arrow(
            asset_id=asset_id,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
            selected_fields=selected_fields,
            limit=limit,
        )
        return arrow_tbl.to_pylist()

    def get_asset_distribution(self) -> Dict[str, int]:
        """Returns count of telemetry records per asset_id."""
        arrow_tbl = self.table.scan(selected_fields=["asset_id"]).to_arrow()
        asset_col = arrow_tbl.column("asset_id").to_pylist()
        distribution: Dict[str, int] = {}
        for aid in asset_col:
            distribution[aid] = distribution.get(aid, 0) + 1
        return distribution

    def get_snapshots_summary(self) -> List[Dict[str, Any]]:
        """Returns summary metadata for all table commits and snapshots."""
        snapshots = self.table.snapshots()
        summary = []
        for s in snapshots:
            snap_dict = {
                "snapshot_id": s.snapshot_id,
                "timestamp_ms": s.timestamp_ms,
                "manifest_list": s.manifest_list,
            }
            if s.summary:
                sum_info = {"operation": str(s.summary.operation)}
                if hasattr(s.summary, "additional_properties") and s.summary.additional_properties:
                    sum_info.update(s.summary.additional_properties)
                snap_dict["summary"] = sum_info
            else:
                snap_dict["summary"] = {}
            summary.append(snap_dict)
        return summary
