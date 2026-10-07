# ForgeStream Phase 4: Production Runbook & Operations Guide

## Executive Overview
This runbook provides step-by-step procedures for deploying, operating, monitoring, troubleshooting, and recovering ForgeStream Phase 4 Lakehouse Serving, Apache Trino, and Grafana infrastructure.

---

## Service Deployment & Startup

### 1. Launch All Distributed Services
```bash
docker compose -f docker-compose.yml up -d
```

### 2. Verify Service Health Status
```bash
# Verify all containers are healthy (healthy state)
docker compose ps

# Check Trino Coordinator REST health endpoint
curl -s http://localhost:8085/v1/info | jq .

# Check Grafana health
curl -s http://localhost:3000/api/health | jq .
```

---

## Operating Procedures

### 1. Execute Real-Time Telemetry & Serving Pipeline
To run an operational batch through the Lakehouse Serving Service:
```python
from forgestream.serving.service import LakehouseServingService
from forgestream.prognostics.inference import PrognosticInferenceEngine

# Initialize serving service
service = LakehouseServingService()
engine = PrognosticInferenceEngine(model_version="v3.0.0-champion")

# Ingest and process telemetry
summary = service.process_telemetry_batch(events, ml_inference_engine=engine)
print(f"Processed {summary['events_processed']} events across {summary['assets_updated']} assets.")
```

### 2. Execute Analytical Queries via Trino Client
```python
from forgestream.serving.trino_client import TrinoClient

client = TrinoClient(host="localhost", port=8085, catalog="postgres", schema="public")
result = client.execute_query("SELECT COUNT(*) AS total FROM asset_current_state")
print(f"Total assets: {result.rows[0][0]}")
```

### 3. Run Benchmark and Verification Suite
```bash
# Execute Phase 4 Performance Benchmarking
python scripts/benchmark_phase4_performance.py

# Generate Machine-Readable Evidence Files
python scripts/generate_phase4_evidence.py

# Run Full 4-Phase Test Suite
pytest tests/unit/ tests/integration/ tests/e2e/ -v
```

---

## Troubleshooting Guide

### Issue 1: Trino Coordinator Returns 503 Service Unavailable
- **Cause**: Trino is still in discovery initialization or JVM warmup.
- **Remedy**: Check container logs `docker logs forgestream-trino-phase4`. Allow up to 20 seconds for `start_period` healthcheck to stabilize.

### Issue 2: PostgreSQL Catalog Table Not Found in Trino
- **Cause**: Search path or schema mismatch.
- **Remedy**: Ensure SQL queries explicitly specify the catalog and schema: `postgres.public.asset_current_state`.

### Issue 3: Grafana Dashboard Shows "No Data"
- **Cause**: `asset_current_state` table has not yet been populated with streaming telemetry.
- **Remedy**: Execute `python scripts/run_phase4_demo.py` to seed operational state and refresh Grafana at `http://localhost:3000`.

---

## Recovery & Maintenance

### Clean Reset of Operational State
```bash
# Connect to PostgreSQL and truncate operational tables
docker exec -i forgestream-postgres-phase1 psql -U forgestream -d forgestream -c "
    TRUNCATE TABLE maintenance_priority_queue CASCADE;
    TRUNCATE TABLE asset_prediction_history CASCADE;
    TRUNCATE TABLE asset_alert_history CASCADE;
    TRUNCATE TABLE asset_current_state CASCADE;
    TRUNCATE TABLE fleet_kpi_snapshots CASCADE;
"
```
