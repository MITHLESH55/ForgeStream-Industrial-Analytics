"""
Unified Command-Line Interface for ForgeStream.
Provides commands for smoke verification, simulation runs, Lakehouse verification, and benchmarking.
"""

import argparse
import json
from pathlib import Path
import sys
import time
from typing import Optional
from forgestream.config import settings
from forgestream.observability.logging import get_logger
from forgestream.pipeline.runner import PipelineRunner
from forgestream.iceberg.reader import IcebergHistoricalReader
from forgestream.postgres.repository import PostgresRepository

logger = get_logger("cli")


def run_smoke_test(force_fallback: bool = False) -> int:
    """
    Executes an end-to-end smoke test validating all Phase 1 components:
    Simulator -> Validator -> Kafka -> Ingestion Worker -> Iceberg -> Postgres -> Readback.
    Returns 0 on PASS, 1 on FAIL.
    """
    print("=" * 70)
    print(" ForgeStream Phase 1: Streaming Infrastructure Smoke Test")
    print("=" * 70)

    try:
        runner = PipelineRunner(force_fallback=force_fallback)
        print("[1/5] Initialized operational database and Iceberg catalog... [OK]")

        print("[2/5] Running deterministic 10-second multi-asset simulation (seed=42)...")
        report = runner.run_simulation(duration_sec=10, seed=42)
        print(f"      -> Events generated: {report['events_generated']}")
        print(f"      -> Events persisted to Iceberg: {report['events_persisted_iceberg']}")
        print(f"      -> Events quarantined: {report['events_quarantined']}")
        print(f"      -> Throughput: {report['throughput_events_per_sec']} events/sec")
        print("[2/5] Telemetry generation and streaming... [OK]")

        print("[3/5] Verifying Apache Iceberg lakehouse readback...")
        reader = runner.iceberg_reader
        total_iceberg = reader.get_total_row_count()
        assert total_iceberg >= report["events_persisted_iceberg"], f"Iceberg row count mismatch: {total_iceberg} < {report['events_persisted_iceberg']}"
        dist = reader.get_asset_distribution()
        print(f"      -> Total Iceberg table rows: {total_iceberg}")
        print(f"      -> Asset distribution: {dist}")
        print("[3/5] Apache Iceberg lakehouse storage... [OK]")

        print("[4/5] Verifying PostgreSQL / SQLite operational metadata...")
        repo = runner.postgres_repo
        assets = repo.list_assets()
        assert len(assets) >= 5, f"Expected at least 5 registered assets, found {len(assets)}"
        run_record = repo.get_ingestion_run(report["run_id"])
        assert run_record is not None, "Ingestion run metadata not found"
        assert run_record["status"] == "COMPLETED", f"Expected COMPLETED status, got {run_record['status']}"
        print(f"      -> Registered assets in metadata: {len(assets)}")
        print(f"      -> Ingestion run record verified: {run_record['run_id']} (Status: {run_record['status']})")
        print("[4/5] Operational metadata repository... [OK]")

        print("[5/5] Checking Data Quality & Quarantine pipeline...")
        # Inject and verify a test quarantine event
        val_result = runner.validator.validate({
            "event_id": "SMOKE-BAD-001",
            "asset_id": "MOTOR-001",
            "vibration": -5.0,  # Violates DQ-007
        })
        assert not val_result.is_valid, "Expected validation failure on negative vibration"
        print("      -> Data Quality rule engine evaluation verified: DQ-007 detected [OK]")

        print("=" * 70)
        print(" SMOKE TEST RESULT: ALL PHASE 1 SUBSYSTEMS PASSED [SUCCESS]")
        print("=" * 70)
        return 0

    except Exception as e:
        print(f"\n[ERROR] Smoke test failed with exception: {e}")
        logger.exception("Smoke test error")
        return 1


def run_simulation_cli(args: argparse.Namespace) -> int:
    """Executes a configurable simulation run from CLI."""
    runner = PipelineRunner(force_fallback=args.force_fallback)
    print(f"Starting ForgeStream simulation run (duration={args.duration}s, seed={args.seed})...")
    report = runner.run_simulation(
        duration_sec=args.duration,
        seed=args.seed,
        sampling_interval_sec=args.interval,
        batch_size=args.batch_size,
    )
    print(json.dumps(report, indent=2))

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Saved run report to {out_path}")
    return 0


def run_verify_cli(args: argparse.Namespace) -> int:
    """Queries and verifies Iceberg lakehouse and Postgres operational store."""
    reader = IcebergHistoricalReader()
    repo = PostgresRepository()

    print("=" * 60)
    print(" ForgeStream Phase 1: Lakehouse & Metadata Verification")
    print("=" * 60)

    total_rows = reader.get_total_row_count()
    dist = reader.get_asset_distribution()
    snapshots = reader.get_snapshots_summary()
    assets = repo.list_assets()
    quarantined = repo.get_quarantined_records(limit=args.limit)

    print(f"Lakehouse Table Rows : {total_rows}")
    print(f"Snapshots Committed  : {len(snapshots)}")
    print(f"Asset Distribution   : {dist}")
    print(f"Registered Assets    : {len(assets)}")
    print(f"Quarantined Records  : {len(quarantined)}")

    if args.limit > 0 and total_rows > 0:
        print(f"\n--- Sample Lakehouse Records (First {args.limit}) ---")
        samples = reader.scan_to_dicts(limit=args.limit)
        for i, s in enumerate(samples, 1):
            print(f"[{i}] {s.get('asset_id')} | t={s.get('event_time')} | T={s.get('temperature')}C | Vib={s.get('vibration')}mm/s | RPM={s.get('rpm')} | P={s.get('power')}kW")

    print("=" * 60)
    return 0


def main():
    parser = argparse.ArgumentParser(
        prog="forgestream",
        description="ForgeStream: Real-Time Distributed Industrial Analytics Platform (Phase 1 CLI)",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # smoke
    smoke_parser = subparsers.add_parser("smoke", help="Execute complete Phase 1 smoke test suite")
    smoke_parser.add_argument("--force-fallback", action="store_true", help="Force local fallback mode")

    # run
    run_parser = subparsers.add_parser("run", help="Run deterministic industrial telemetry simulation")
    run_parser.add_argument("--duration", type=int, default=30, help="Simulation duration in seconds")
    run_parser.add_argument("--seed", type=int, default=42, help="Deterministic PRNG master seed")
    run_parser.add_argument("--interval", type=float, default=1.0, help="Sampling interval in seconds")
    run_parser.add_argument("--batch-size", type=int, default=100, help="Ingestion batch size")
    run_parser.add_argument("--output-json", type=str, help="File path to save JSON results")
    run_parser.add_argument("--force-fallback", action="store_true", help="Force local fallback mode")

    # verify
    verify_parser = subparsers.add_parser("verify", help="Inspect and verify Iceberg Lakehouse & Postgres Metadata")
    verify_parser.add_argument("--limit", type=int, default=5, help="Number of sample records to inspect (default: 5)")

    args = parser.parse_args()

    if args.command == "smoke":
        sys.exit(run_smoke_test(force_fallback=args.force_fallback))
    elif args.command == "run":
        sys.exit(run_simulation_cli(args))
    elif args.command == "verify":
        sys.exit(run_verify_cli(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
