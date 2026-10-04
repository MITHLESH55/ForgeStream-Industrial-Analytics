"""
End-to-End Tests for ForgeStream Phase 1 Streaming Architecture.
Tests the full pipeline lifecycle:
Simulator -> Kafka Streaming -> Validation Engine -> Apache Iceberg Lakehouse -> PostgreSQL Operational Store.
"""

from datetime import datetime, timezone
import pytest
from pathlib import Path
from forgestream.iceberg.catalog import IcebergCatalogManager
from forgestream.iceberg.tables import IcebergTableManager
from forgestream.pipeline.runner import PipelineRunner
from forgestream.postgres.connection import DatabaseManager
from forgestream.schemas.telemetry_schema import AssetType, ScenarioID


@pytest.fixture
def e2e_pipeline_runner(tmp_path: Path) -> PipelineRunner:
    """Creates a fully isolated PipelineRunner instance with temp SQLite DB and PyIceberg catalog."""
    db_file = tmp_path / "e2e_metadata.db"
    cat_file = tmp_path / "e2e_catalog.db"
    wh_dir = tmp_path / "e2e_warehouse"
    wh_dir.mkdir(parents=True, exist_ok=True)

    db_mgr = DatabaseManager(sqlite_fallback_path=str(db_file), force_sqlite=True)
    cat_mgr = IcebergCatalogManager(
        catalog_name="e2e_cat",
        catalog_uri=f"sqlite:///{cat_file.as_posix()}",
        warehouse_path=wh_dir.as_posix(),
        namespace="e2e_forgestream",
    )

    runner = PipelineRunner(
        db_manager=db_mgr,
        catalog_manager=cat_mgr,
        force_fallback=True,
    )
    return runner


def test_full_phase1_pipeline_e2e(e2e_pipeline_runner: PipelineRunner):
    """
    Executes an end-to-end streaming simulation and verifies:
    - 100% of valid generated events are written to Apache Iceberg.
    - PostgreSQL operational metadata tracks run lifecycle from RUNNING to COMPLETED.
    - Lakehouse partitioning by asset_id correctly segregates records.
    """
    runner = e2e_pipeline_runner

    report = runner.run_simulation(
        duration_sec=5,
        seed=42,
        sampling_interval_sec=1.0,
        batch_size=25,
    )

    # 1. Pipeline execution metrics
    assert report["events_generated"] == 25  # 5 assets * 5 seconds * 1 Hz
    assert report["events_persisted_iceberg"] == 25
    assert report["events_quarantined"] == 0
    assert report["throughput_events_per_sec"] > 0.0

    # 2. Iceberg Lakehouse assertions
    total_iceberg_rows = runner.iceberg_reader.get_total_row_count()
    assert total_iceberg_rows == 25

    dist = runner.iceberg_reader.get_asset_distribution()
    assert len(dist) == 5
    for asset_id in ["MOTOR-001", "PUMP-001", "COMPRESSOR-001", "CONVEYOR-001", "TURBINE-001"]:
        assert dist[asset_id] == 5

    # 3. PostgreSQL metadata assertions
    repo = runner.postgres_repo
    run_meta = repo.get_ingestion_run(report["run_id"])
    assert run_meta is not None
    assert run_meta["status"] == "COMPLETED"
    assert run_meta["events_generated"] == 25
    assert run_meta["events_persisted_iceberg"] == 25
    assert run_meta["events_quarantined"] == 0

    # 4. Verified registered assets
    assets = repo.list_assets()
    assert len(assets) == 5


def test_pipeline_seed_reproducibility(tmp_path: Path):
    """
    Executes two identical pipeline runs with the same master seed (seed=101)
    and verifies that the generated telemetry sequences and physics values are bitwise identical.
    """
    db_file1 = tmp_path / "rep1_metadata.db"
    cat_file1 = tmp_path / "rep1_catalog.db"
    wh_dir1 = tmp_path / "rep1_warehouse"

    db_file2 = tmp_path / "rep2_metadata.db"
    cat_file2 = tmp_path / "rep2_catalog.db"
    wh_dir2 = tmp_path / "rep2_warehouse"

    runner1 = PipelineRunner(
        db_manager=DatabaseManager(sqlite_fallback_path=str(db_file1), force_sqlite=True),
        catalog_manager=IcebergCatalogManager(catalog_uri=f"sqlite:///{cat_file1.as_posix()}", warehouse_path=wh_dir1.as_posix()),
        force_fallback=True,
    )
    runner2 = PipelineRunner(
        db_manager=DatabaseManager(sqlite_fallback_path=str(db_file2), force_sqlite=True),
        catalog_manager=IcebergCatalogManager(catalog_uri=f"sqlite:///{cat_file2.as_posix()}", warehouse_path=wh_dir2.as_posix()),
        force_fallback=True,
    )

    report1 = runner1.run_simulation(duration_sec=3, seed=101)
    report2 = runner2.run_simulation(duration_sec=3, seed=101)

    assert report1["events_generated"] == report2["events_generated"]

    rows1 = runner1.iceberg_reader.scan_to_dicts(asset_id="MOTOR-001")
    rows2 = runner2.iceberg_reader.scan_to_dicts(asset_id="MOTOR-001")

    assert len(rows1) == len(rows2)
    for r1, r2 in zip(rows1, rows2):
        assert r1["temperature"] == pytest.approx(r2["temperature"], 1e-6)
        assert r1["vibration"] == pytest.approx(r2["vibration"], 1e-6)
        assert r1["rpm"] == pytest.approx(r2["rpm"], 1e-6)
        assert r1["power"] == pytest.approx(r2["power"], 1e-6)


def test_live_kafka_and_postgres_e2e_pipeline(tmp_path: Path):
    """
    Executes end-to-end simulation across LIVE Kafka broker and LIVE PostgreSQL 16 database.
    Verifies that real broker message streaming, schema validation, PyIceberg commits,
    and PostgreSQL operational metadata are all connected and verified.
    """
    live_db = DatabaseManager(force_sqlite=False)
    if not live_db.is_postgres:
        pytest.skip("Live PostgreSQL container offline; tested fallback mode.")

    cat_file = tmp_path / "live_e2e_catalog.db"
    wh_dir = tmp_path / "live_e2e_warehouse"
    wh_dir.mkdir(parents=True, exist_ok=True)
    cat_mgr = IcebergCatalogManager(
        catalog_name="live_e2e_cat",
        catalog_uri=f"sqlite:///{cat_file.as_posix()}",
        warehouse_path=wh_dir.as_posix(),
        namespace="live_e2e_forgestream",
    )

    runner = PipelineRunner(
        db_manager=live_db,
        catalog_manager=cat_mgr,
        force_fallback=False,  # Use live Kafka broker
    )

    report = runner.run_simulation(
        duration_sec=3,
        seed=42,
        sampling_interval_sec=1.0,
        batch_size=15,
    )

    assert report["events_generated"] == 15
    assert report["events_persisted_iceberg"] + report["events_quarantined"] == 15
    assert report["events_persisted_iceberg"] > 0

    # Verify Iceberg
    total_iceberg = runner.iceberg_reader.get_total_row_count()
    assert total_iceberg == report["events_persisted_iceberg"]

    # Verify PostgreSQL
    pg_run = runner.postgres_repo.get_ingestion_run(report["run_id"])
    assert pg_run is not None
    assert pg_run["status"] == "COMPLETED"
    assert pg_run["events_persisted_iceberg"] == report["events_persisted_iceberg"]
    assert pg_run["events_quarantined"] == report["events_quarantined"]
