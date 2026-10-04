"""
Fault Injection and Data Quality Quarantine End-to-End Tests.
Injects corrupted, anomalous, and defective telemetry payloads through the ingestion bus
and verifies accurate quarantine routing, PostgreSQL audit logging, and DLQ serialization.
"""

from datetime import datetime, timezone
import pytest
from pathlib import Path
from forgestream.iceberg.catalog import IcebergCatalogManager
from forgestream.pipeline.ingestion_worker import IngestionWorker
from forgestream.postgres.connection import DatabaseManager
from forgestream.postgres.repository import PostgresRepository
from forgestream.iceberg.tables import IcebergTableManager
from forgestream.iceberg.writer import IcebergHistoricalWriter
from forgestream.iceberg.reader import IcebergHistoricalReader
from forgestream.validation.validator import DataQualityValidator


@pytest.fixture
def fault_injection_setup(tmp_path: Path):
    """Creates an isolated environment for fault injection testing."""
    db_file = tmp_path / "fault_metadata.db"
    cat_file = tmp_path / "fault_catalog.db"
    wh_dir = tmp_path / "fault_warehouse"
    wh_dir.mkdir(parents=True, exist_ok=True)

    db_mgr = DatabaseManager(sqlite_fallback_path=str(db_file), force_sqlite=True)
    postgres_repo = PostgresRepository(db_manager=db_mgr)

    cat_mgr = IcebergCatalogManager(
        catalog_name="fault_cat",
        catalog_uri=f"sqlite:///{cat_file.as_posix()}",
        warehouse_path=wh_dir.as_posix(),
        namespace="fault_forgestream",
    )
    tbl_mgr = IcebergTableManager(catalog_manager=cat_mgr, table_name="fault_telemetry")
    tbl = tbl_mgr.get_or_create_telemetry_table()
    writer = IcebergHistoricalWriter(table_manager=tbl_mgr, table=tbl)
    reader = IcebergHistoricalReader(table_manager=tbl_mgr, table=tbl)
    validator = DataQualityValidator()

    worker = IngestionWorker(
        consumer=None,
        iceberg_writer=writer,
        postgres_repo=postgres_repo,
        validator=validator,
        dlq_producer=None,
    )

    return {
        "worker": worker,
        "repo": postgres_repo,
        "reader": reader,
        "validator": validator,
    }


def test_mixed_batch_quarantine_routing(fault_injection_setup):
    """
    Submits a batch containing 3 valid events and 4 defective events:
    - Defect 1: Negative vibration (DQ-007)
    - Defect 2: Missing required field 'rpm' (DQ-001)
    - Defect 3: Out-of-range temperature > 300C (DQ-005)
    - Defect 4: Malformed string type for voltage (DQ-002)

    Verifies:
    - 3 valid events written to Apache Iceberg lakehouse.
    - 4 defective events stored in PostgreSQL quarantine table.
    - Diagnostic rule codes logged in data_quality_events table.
    """
    worker = fault_injection_setup["worker"]
    repo = fault_injection_setup["repo"]
    reader = fault_injection_setup["reader"]

    now_iso = datetime.now(timezone.utc).isoformat()

    batch = [
        # Valid 1
        {
            "event_id": "V-001",
            "asset_id": "MOTOR-001",
            "asset_type": "MOTOR",
            "timestamp": 1710000001.0,
            "event_time": now_iso,
            "temperature": 65.0,
            "vibration": 1.2,
            "pressure": 1.0,
            "rpm": 1750.0,
            "current": 25.0,
            "voltage": 400.0,
            "power": 16.0,
            "load": 100.0,
            "operating_mode": "NORMAL",
            "sequence_number": 1,
        },
        # Defect 1: Negative vibration
        {
            "event_id": "BAD-001",
            "asset_id": "MOTOR-001",
            "asset_type": "MOTOR",
            "timestamp": 1710000002.0,
            "event_time": now_iso,
            "temperature": 65.0,
            "vibration": -2.4,  # DQ-007
            "pressure": 1.0,
            "rpm": 1750.0,
            "current": 25.0,
            "voltage": 400.0,
            "power": 16.0,
            "load": 100.0,
            "operating_mode": "NORMAL",
            "sequence_number": 2,
        },
        # Valid 2
        {
            "event_id": "V-002",
            "asset_id": "PUMP-001",
            "asset_type": "PUMP",
            "timestamp": 1710000001.0,
            "event_time": now_iso,
            "temperature": 55.0,
            "vibration": 1.5,
            "pressure": 6.5,
            "rpm": 2900.0,
            "current": 32.0,
            "voltage": 400.0,
            "power": 20.0,
            "load": 100.0,
            "operating_mode": "NORMAL",
            "sequence_number": 1,
        },
        # Defect 2: Missing field 'rpm'
        {
            "event_id": "BAD-002",
            "asset_id": "PUMP-001",
            "asset_type": "PUMP",
            "timestamp": 1710000002.0,
            "event_time": now_iso,
            "temperature": 55.0,
            "vibration": 1.5,
            "pressure": 6.5,
            # Missing rpm
            "current": 32.0,
            "voltage": 400.0,
            "power": 20.0,
            "load": 100.0,
            "operating_mode": "NORMAL",
            "sequence_number": 2,
        },
        # Defect 3: Out-of-range impossible temperature (2500C)
        {
            "event_id": "BAD-003",
            "asset_id": "TURBINE-001",
            "asset_type": "TURBINE",
            "timestamp": 1710000001.0,
            "event_time": now_iso,
            "temperature": 2500.0,  # DQ-005 (exceeds 1500C max bound)
            "vibration": 2.5,
            "pressure": 24.0,
            "rpm": 5400.0,
            "current": 180.0,
            "voltage": 6600.0,
            "power": 1200.0,
            "load": 100.0,
            "operating_mode": "NORMAL",
            "sequence_number": 1,
        },
        # Valid 3
        {
            "event_id": "V-003",
            "asset_id": "COMPRESSOR-001",
            "asset_type": "COMPRESSOR",
            "timestamp": 1710000001.0,
            "event_time": now_iso,
            "temperature": 80.0,
            "vibration": 2.0,
            "pressure": 8.0,
            "rpm": 3600.0,
            "current": 90.0,
            "voltage": 400.0,
            "power": 55.0,
            "load": 100.0,
            "operating_mode": "NORMAL",
            "sequence_number": 1,
        },
        # Defect 4: Invalid data type for voltage ("Four Hundred")
        {
            "event_id": "BAD-004",
            "asset_id": "CONVEYOR-001",
            "asset_type": "CONVEYOR",
            "timestamp": 1710000001.0,
            "event_time": now_iso,
            "temperature": 45.0,
            "vibration": 0.8,
            "pressure": 1.0,
            "rpm": 120.0,
            "current": 20.0,
            "voltage": "Four Hundred",  # DQ-002
            "power": 12.0,
            "load": 100.0,
            "operating_mode": "NORMAL",
            "sequence_number": 1,
        },
    ]

    run_id = "RUN-FAULT-TEST-001"
    repo.start_ingestion_run(run_id=run_id, seed=42, asset_count=5, duration_sec=10)
    valid_cnt, bad_cnt = worker.process_batch(batch, run_id=run_id)

    assert valid_cnt == 3
    assert bad_cnt == 4

    # Iceberg table contains only the 3 valid records
    iceberg_rows = reader.scan_to_dicts()
    assert len(iceberg_rows) == 3
    persisted_event_ids = {r["event_id"] for r in iceberg_rows}
    assert persisted_event_ids == {"V-001", "V-002", "V-003"}

    # PostgreSQL quarantine table contains the 4 defective records
    quarantined = repo.get_quarantined_records()
    assert len(quarantined) == 4
    quarantined_event_ids = {q["event_id"] for q in quarantined}
    assert quarantined_event_ids == {"BAD-001", "BAD-002", "BAD-003", "BAD-004"}

    # Audit events in Postgres
    dq_events = repo.get_dq_events_for_run(run_id)
    assert len(dq_events) >= 4
    rule_codes_logged = {e["rule_code"] for e in dq_events}
    assert any("DQ-007" in code for code in rule_codes_logged)  # Negative vibration
    assert any("DQ-001" in code for code in rule_codes_logged)  # Missing rpm
    assert any("DQ-005" in code for code in rule_codes_logged)  # Temperature range
    assert any("DQ-002" in code for code in rule_codes_logged)  # Invalid voltage type
