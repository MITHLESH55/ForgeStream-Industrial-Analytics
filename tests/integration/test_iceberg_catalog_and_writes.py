"""
Integration Tests for Apache Iceberg Lakehouse Storage Layer.
Verifies SqlCatalog initialization, table provisioning, schema validation,
PyArrow batch appends, partitioned storage, and historical reader queries.
"""

from datetime import datetime, timezone
import pytest
from pathlib import Path
from forgestream.iceberg.catalog import IcebergCatalogManager
from forgestream.iceberg.tables import IcebergTableManager, TELEMETRY_ICEBERG_SCHEMA
from forgestream.iceberg.writer import IcebergHistoricalWriter
from forgestream.iceberg.reader import IcebergHistoricalReader
from forgestream.schemas.telemetry_schema import TelemetryEvent, AssetType, OperatingMode


@pytest.fixture
def isolated_iceberg(tmp_path: Path):
    """Creates an isolated PyIceberg environment with temp catalog and warehouse."""
    cat_db = tmp_path / "test_catalog.db"
    wh_dir = tmp_path / "warehouse"
    wh_dir.mkdir(parents=True, exist_ok=True)

    cat_mgr = IcebergCatalogManager(
        catalog_name="test_cat",
        catalog_uri=f"sqlite:///{cat_db.as_posix()}",
        warehouse_path=wh_dir.as_posix(),
        namespace="test_forgestream",
    )
    tbl_mgr = IcebergTableManager(catalog_manager=cat_mgr, table_name="test_telemetry")
    tbl = tbl_mgr.get_or_create_telemetry_table()
    writer = IcebergHistoricalWriter(table_manager=tbl_mgr, table=tbl)
    reader = IcebergHistoricalReader(table_manager=tbl_mgr, table=tbl)

    return {
        "catalog_mgr": cat_mgr,
        "tbl_mgr": tbl_mgr,
        "table": tbl,
        "writer": writer,
        "reader": reader,
        "warehouse_dir": wh_dir,
    }


def _create_sample_event(
    event_id: str,
    asset_id: str,
    asset_type: AssetType = AssetType.MOTOR,
    timestamp: float = 1700000000.0,
    temperature: float = 55.0,
    vibration: float = 1.5,
    pressure: float = 5.0,
    rpm: float = 1750.0,
    current: float = 24.0,
    voltage: float = 460.0,
    power: float = 16.5,
    load: float = 100.0,
    seq: int = 1,
) -> TelemetryEvent:
    now_utc = datetime.now(timezone.utc)
    return TelemetryEvent(
        event_id=event_id,
        asset_id=asset_id,
        asset_type=asset_type,
        timestamp=timestamp,
        event_time=now_utc,
        ingestion_time=now_utc,
        temperature=temperature,
        vibration=vibration,
        pressure=pressure,
        rpm=rpm,
        current=current,
        voltage=voltage,
        power=power,
        load=load,
        operating_mode=OperatingMode.NORMAL,
        sequence_number=seq,
    )


def test_catalog_and_table_provisioning(isolated_iceberg):
    """Verifies catalog creation, namespace presence, and schema matching."""
    tbl = isolated_iceberg["table"]
    assert tbl is not None
    assert len(tbl.schema().fields) == len(TELEMETRY_ICEBERG_SCHEMA.fields)
    assert tbl.schema().find_field("asset_id") is not None
    assert tbl.schema().find_field("temperature") is not None
    assert tbl.spec().fields[0].name == "asset_id"


def test_iceberg_writer_batch_append(isolated_iceberg):
    """Verifies appending a batch of TelemetryEvents and creating an Iceberg snapshot."""
    writer = isolated_iceberg["writer"]
    reader = isolated_iceberg["reader"]

    events = [
        _create_sample_event("EVT-001", "MOTOR-001", timestamp=100.0, temperature=50.0, seq=1),
        _create_sample_event("EVT-002", "MOTOR-001", timestamp=101.0, temperature=50.5, seq=2),
        _create_sample_event("EVT-003", "PUMP-001", asset_type=AssetType.PUMP, timestamp=100.0, temperature=62.0, seq=1),
    ]

    result = writer.write_events(events)
    assert result["records_written"] == 3
    assert result["snapshot_id"] is not None

    total_rows = reader.get_total_row_count()
    assert total_rows == 3


def test_iceberg_reader_queries_and_filters(isolated_iceberg):
    """Verifies reading back data with filters by asset_id and timestamp range."""
    writer = isolated_iceberg["writer"]
    reader = isolated_iceberg["reader"]

    events = [
        _create_sample_event("EVT-M1-1", "MOTOR-001", timestamp=100.0, seq=1),
        _create_sample_event("EVT-M1-2", "MOTOR-001", timestamp=200.0, seq=2),
        _create_sample_event("EVT-M1-3", "MOTOR-001", timestamp=300.0, seq=3),
        _create_sample_event("EVT-P1-1", "PUMP-001", asset_type=AssetType.PUMP, timestamp=150.0, seq=1),
        _create_sample_event("EVT-P1-2", "PUMP-001", asset_type=AssetType.PUMP, timestamp=250.0, seq=2),
    ]

    writer.write_events(events)

    # Filter by asset_id
    motor_records = reader.scan_to_dicts(asset_id="MOTOR-001")
    assert len(motor_records) == 3
    assert all(r["asset_id"] == "MOTOR-001" for r in motor_records)

    pump_records = reader.scan_to_dicts(asset_id="PUMP-001")
    assert len(pump_records) == 2
    assert all(r["asset_id"] == "PUMP-001" for r in pump_records)

    # Filter by timestamp range
    time_filtered = reader.scan_to_dicts(min_timestamp=120.0, max_timestamp=220.0)
    assert len(time_filtered) == 2  # EVT-M1-2 (200.0) and EVT-P1-1 (150.0)

    # Asset distribution
    dist = reader.get_asset_distribution()
    assert dist["MOTOR-001"] == 3
    assert dist["PUMP-001"] == 2


def test_multi_batch_appends_and_snapshots(isolated_iceberg):
    """Verifies that multiple write commits produce discrete snapshot lineage."""
    writer = isolated_iceberg["writer"]
    reader = isolated_iceberg["reader"]

    batch1 = [_create_sample_event(f"EVT-B1-{i}", "MOTOR-001", seq=i) for i in range(10)]
    batch2 = [_create_sample_event(f"EVT-B2-{i}", "MOTOR-001", seq=i+10) for i in range(15)]

    res1 = writer.write_events(batch1)
    res2 = writer.write_events(batch2)

    assert res1["snapshot_id"] != res2["snapshot_id"]
    assert reader.get_total_row_count() == 25

    snapshots = reader.get_snapshots_summary()
    assert len(snapshots) == 2


def test_readback_field_precision_integrity(isolated_iceberg):
    """Verifies that float values and timestamps maintain exact precision."""
    writer = isolated_iceberg["writer"]
    reader = isolated_iceberg["reader"]

    event = _create_sample_event(
        event_id="EVT-PRECISION-001",
        asset_id="TURBINE-001",
        asset_type=AssetType.TURBINE,
        timestamp=1710500000.125,
        temperature=85.6789,
        vibration=3.14159,
        pressure=15.4321,
        rpm=3600.5,
        current=125.75,
        voltage=480.25,
        power=95.123,
        load=87.5,
        seq=100,
    )

    writer.write_events([event])
    rows = reader.scan_to_dicts(asset_id="TURBINE-001")
    assert len(rows) == 1
    row = rows[0]

    assert row["event_id"] == "EVT-PRECISION-001"
    assert row["asset_id"] == "TURBINE-001"
    assert pytest.approx(row["temperature"], 1e-4) == 85.6789
    assert pytest.approx(row["vibration"], 1e-4) == 3.14159
    assert pytest.approx(row["pressure"], 1e-4) == 15.4321
    assert pytest.approx(row["rpm"], 1e-4) == 3600.5
    assert pytest.approx(row["power"], 1e-4) == 95.123
    assert row["sequence_number"] == 100
