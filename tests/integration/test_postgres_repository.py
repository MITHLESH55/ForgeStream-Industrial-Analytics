"""
Integration Tests for PostgreSQL / SQLite Operational Metadata Repository.
Verifies relational schema creation, asset CRUD, maintenance tracking,
ingestion run metrics, and quarantine persistence.
"""

from datetime import datetime, timezone
import pytest
from pathlib import Path
from forgestream.postgres.connection import DatabaseManager
from forgestream.postgres.repository import PostgresRepository
from forgestream.validation.quarantine import QuarantinedRecord


@pytest.fixture
def test_repo(tmp_path: Path) -> PostgresRepository:
    """Provides a fresh isolated SQLite metadata repository for testing."""
    test_db_path = tmp_path / "test_metadata.db"
    db_mgr = DatabaseManager(sqlite_fallback_path=str(test_db_path), force_sqlite=True)
    repo = PostgresRepository(db_manager=db_mgr)
    return repo


def test_asset_registry_lifecycle(test_repo: PostgresRepository):
    """Verifies asset upsertion, retrieval, and listing."""
    # Insert new asset
    test_repo.upsert_asset(
        asset_id="MOTOR-TEST-001",
        asset_type="MOTOR",
        model_name="Siemens 1LA7 15kW Test",
        rated_rpm=1750.0,
        rated_load=100.0,
        rated_voltage=460.0,
        rated_current=24.5,
        baseline_temperature=52.0,
        baseline_vibration=1.45,
        baseline_pressure=0.0,
        criticality="HIGH",
    )

    asset = test_repo.get_asset("MOTOR-TEST-001")
    assert asset is not None
    assert asset["asset_id"] == "MOTOR-TEST-001"
    assert asset["asset_type"] == "MOTOR"
    assert asset["criticality"] == "HIGH"
    assert float(asset["rated_rpm"]) == 1750.0

    # Update existing asset
    test_repo.upsert_asset(
        asset_id="MOTOR-TEST-001",
        asset_type="MOTOR",
        model_name="Siemens 1LA7 15kW Test (Refurbished)",
        rated_rpm=1800.0,
        rated_load=100.0,
        rated_voltage=460.0,
        rated_current=25.0,
        baseline_temperature=50.0,
        baseline_vibration=1.2,
        baseline_pressure=0.0,
        criticality="CRITICAL",
    )

    updated = test_repo.get_asset("MOTOR-TEST-001")
    assert updated["model_name"] == "Siemens 1LA7 15kW Test (Refurbished)"
    assert updated["criticality"] == "CRITICAL"
    assert float(updated["rated_rpm"]) == 1800.0

    # List assets
    all_assets = test_repo.list_assets()
    assert len(all_assets) == 1
    assert all_assets[0]["asset_id"] == "MOTOR-TEST-001"


def test_maintenance_history_tracking(test_repo: PostgresRepository):
    """Verifies recording maintenance work orders."""
    test_repo.upsert_asset(
        asset_id="PUMP-TEST-001",
        asset_type="PUMP",
        model_name="Grundfos CR32",
        rated_rpm=2950.0,
        rated_load=100.0,
        rated_voltage=400.0,
        rated_current=30.0,
        baseline_temperature=58.0,
        baseline_vibration=1.8,
        baseline_pressure=12.0,
    )

    performed_time = datetime(2026, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
    test_repo.record_maintenance(
        maintenance_id="MAINT-20260315-01",
        asset_id="PUMP-TEST-001",
        maintenance_type="BEARING_REPLACEMENT",
        technician_id="TECH-409",
        performed_at=performed_time,
        scenario_id="SCENARIO_002_BEARING_DEGRADATION",
        description="Replaced DE roller bearings and re-greased.",
        cost_usd=450.0,
    )

    history = test_repo.get_asset_maintenance_history("PUMP-TEST-001")
    assert len(history) == 1
    assert history[0]["maintenance_id"] == "MAINT-20260315-01"
    assert history[0]["maintenance_type"] == "BEARING_REPLACEMENT"
    assert history[0]["technician_id"] == "TECH-409"
    assert float(history[0]["cost_usd"]) == 450.0


def test_ingestion_run_lifecycle_and_dq_events(test_repo: PostgresRepository):
    """Verifies ingestion run progression and associated data quality event audit logging."""
    run_id = "RUN-TEST-99"
    test_repo.start_ingestion_run(
        run_id=run_id,
        seed=42,
        asset_count=5,
        duration_sec=60,
    )

    run = test_repo.get_ingestion_run(run_id)
    assert run is not None
    assert run["status"] == "RUNNING"
    assert run["events_generated"] == 0

    # Record DQ audit events
    test_repo.record_dq_event(
        rule_code="DQ-006",
        severity="ERROR",
        run_id=run_id,
        event_id="EVT-001",
        asset_id="MOTOR-TEST-001",
        details="RPM 12000.0 exceeds maximum physical threshold 6000.0",
    )
    test_repo.record_dq_event(
        rule_code="DQ-007",
        severity="ERROR",
        run_id=run_id,
        event_id="EVT-002",
        asset_id="MOTOR-TEST-001",
        details="Negative vibration RMS detected: -2.5 mm/s",
    )

    dq_events = test_repo.get_dq_events_for_run(run_id)
    assert len(dq_events) == 2
    assert dq_events[0]["rule_code"] == "DQ-006"
    assert dq_events[1]["rule_code"] == "DQ-007"

    # Complete run
    test_repo.complete_ingestion_run(
        run_id=run_id,
        events_generated=1000,
        events_persisted_iceberg=950,
        events_quarantined=50,
        status="COMPLETED",
    )

    completed_run = test_repo.get_ingestion_run(run_id)
    assert completed_run["status"] == "COMPLETED"
    assert completed_run["events_generated"] == 1000
    assert completed_run["events_persisted_iceberg"] == 950
    assert completed_run["events_quarantined"] == 50


def test_quarantine_record_storage(test_repo: PostgresRepository):
    """Verifies storage and retrieval of quarantined malformed records."""
    test_repo.start_ingestion_run(
        run_id="RUN-TEST-01",
        seed=42,
        asset_count=1,
        duration_sec=10,
    )

    q_rec = QuarantinedRecord(
        quarantine_id="Q-TEST-001",
        event_id="EVT-BAD-001",
        asset_id="TURBINE-001",
        violated_rules=["DQ-005", "DQ-007"],
        reasons=["NaN value in temperature", "Negative vibration: -1.2"],
        raw_payload={"event_id": "EVT-BAD-001", "temperature": "NaN", "vibration": -1.2},
    )

    test_repo.record_quarantine(quarantine_record=q_rec, run_id="RUN-TEST-01")

    quarantined = test_repo.get_quarantined_records(limit=10)
    assert len(quarantined) == 1
    assert quarantined[0]["quarantine_id"] == "Q-TEST-001"
    assert quarantined[0]["asset_id"] == "TURBINE-001"
    assert "DQ-005" in quarantined[0]["violated_rules"]
    assert "DQ-007" in quarantined[0]["violated_rules"]


def test_live_postgresql_backend_if_available():
    """Verifies connectivity and table operations on live PostgreSQL 16 backend."""
    live_db = DatabaseManager(force_sqlite=False)
    if not live_db.is_postgres:
        pytest.skip("Live PostgreSQL container not reachable; tested fallback mode.")

    repo = PostgresRepository(db_manager=live_db)

    # 1. Assets
    assets = repo.list_assets()
    assert len(assets) >= 5

    # 2. Ingestion Runs
    run_id = f"RUN-PG-TEST-{int(datetime.now().timestamp())}"
    repo.start_ingestion_run(run_id=run_id, seed=99, asset_count=5, duration_sec=10)
    run_meta = repo.get_ingestion_run(run_id)
    assert run_meta is not None
    assert run_meta["status"] == "RUNNING"

    # 3. DQ Events
    repo.record_dq_event(
        rule_code="DQ-001",
        severity="ERROR",
        run_id=run_id,
        event_id="EVT-LIVE-PG-001",
        asset_id="MOTOR-001",
        details="Live PG verification DQ event",
    )
    dq_events = repo.get_dq_events_for_run(run_id)
    assert len(dq_events) >= 1

    # 4. Quarantine Events
    q_rec = QuarantinedRecord(
        quarantine_id=f"Q-PG-{int(datetime.now().timestamp())}",
        event_id="EVT-LIVE-PG-001",
        asset_id="MOTOR-001",
        violated_rules=["DQ-001"],
        reasons=["Missing required field"],
        raw_payload={"event_id": "EVT-LIVE-PG-001"},
    )
    repo.record_quarantine(quarantine_record=q_rec, run_id=run_id)

    # 5. Complete Ingestion Run
    repo.complete_ingestion_run(
        run_id=run_id,
        events_generated=10,
        events_persisted_iceberg=9,
        events_quarantined=1,
        status="COMPLETED",
    )
    completed = repo.get_ingestion_run(run_id)
    assert completed["status"] == "COMPLETED"

    # 6. Experiments
    exp_id = f"EXP-PG-{int(datetime.now().timestamp())}"
    repo.record_experiment(
        experiment_id=exp_id,
        name="Live PostgreSQL Verification",
        scenario_id="SCENARIO_001_NORMAL_OPERATION",
        seed=99,
        metrics_summary={"live_postgres": True, "driver": "psycopg 3.3.6"},
    )
    exp = repo.get_experiment(exp_id)
    assert exp is not None
    assert exp["experiment_id"] == exp_id


def test_experiment_run_recording(test_repo: PostgresRepository):
    """Verifies experiment run registration with JSON summary metrics."""
    metrics_data = {
        "total_events": 5000,
        "anomalies_detected": 124,
        "f1_score": 0.982,
        "mean_latency_ms": 1.45,
    }

    test_repo.record_experiment(
        experiment_id="EXP-2026-001",
        name="Phase 1 Baseline Smoke Experiment",
        scenario_id="SCENARIO_001_NORMAL_OPERATION",
        seed=42,
        metrics_summary=metrics_data,
    )

    exp = test_repo.get_experiment("EXP-2026-001")
    assert exp is not None
    assert exp["name"] == "Phase 1 Baseline Smoke Experiment"
    assert exp["seed"] == 42
    assert exp["metrics_summary"]["f1_score"] == 0.982
    assert exp["metrics_summary"]["total_events"] == 5000
