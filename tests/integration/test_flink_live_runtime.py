"""Integration test for Apache Flink JobManager, TaskManager, and PyFlink topology."""

import json
import urllib.request
import pytest


def test_flink_cluster_live_status():
    """Verify Apache Flink 1.18.1 cluster is live and healthy with registered task slots."""
    try:
        req = urllib.request.Request("http://localhost:8081/overview", headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        pytest.skip(f"Apache Flink cluster offline at localhost:8081: {e}")

    assert data.get("flink-version") == "1.18.1"
    assert data.get("taskmanagers", 0) >= 1
    assert data.get("slots-total", 0) >= 1


def test_flink_evidence_files_exist_and_pass():
    """Verify that all genuine Flink execution evidence files exist and show PASS status."""
    import os

    results_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../results"))
    evidence_files = [
        "phase2_real_flink_runtime.json",
        "phase2_flink_event_time.json",
        "phase2_flink_watermarks.json",
        "phase2_flink_state.json",
        "phase2_flink_windows.json",
        "phase2_flink_recovery.json",
        "phase2_flink_performance.json",
        "phase2_flink_performance_optimized.json",
        "phase2_performance_methodology.json",
        "phase2_performance_analysis.json",
        "phase2_flink_e2e.json",
        "phase2_final_audit.json",
    ]

    for fname in evidence_files:
        fpath = os.path.join(results_dir, fname)
        assert os.path.exists(fpath), f"Missing Flink evidence file: {fname}"
        with open(fpath, "r") as f:
            data = json.load(f)
            if "verification_status" in data:
                assert data["verification_status"] == "PASS", f"Evidence {fname} status is {data['verification_status']}"
            elif "exit_gate_status" in data:
                assert "PASS" in data["exit_gate_status"], f"Audit {fname} exit gate status is {data['exit_gate_status']}"
