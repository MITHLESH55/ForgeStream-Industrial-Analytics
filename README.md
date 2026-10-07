# ForgeStream: Real-Time Distributed Analytics Platform

> **Industrial Asset Health Monitoring and Predictive Maintenance**
> *Academic Context: B.Tech Computer Science and Engineering (Semester VII) — Big Data Analytics (CA-3)*  
> *Symbiosis Institute of Technology, Pune*

---

## 1. Executive Summary

**ForgeStream** is an end-to-end real-time distributed streaming analytics, lakehouse, and machine learning platform engineered for high-frequency industrial equipment telemetry.

The platform spans 4 fully implemented and verified phases:
- **Phase 1: Data Foundation & Streaming Infrastructure**: Physics-correlated telemetry simulator across 5 asset types and 8 degradation scenarios, Apache Kafka KRaft message bus, 13-rule Data Quality & Quarantine engine, Apache Iceberg (PyIceberg + PyArrow) lakehouse, and PostgreSQL 16 operational repository.
- **Phase 2: Real-Time Stream Processing & Asset Health Intelligence**: Event-time streaming engine with watermarking (5s bounded out-of-orderness), keyed state ($N=120$ circular buffers), tumbling & sliding windows, 3-level explainable anomaly detection, hysteresis state machine health modeling, and containerized Apache Flink 1.18.1 distributed runtime.
- **Phase 3: Predictive Maintenance ML & Remaining Useful Life (RUL)**: Apache Spark MLlib feature engineering pipelines (25 features), zero-lookahead target leakage audit, Random Forest binary failure classifier ($PR\text{-}AUC = 0.7666, \text{Recall} = 0.8512$), Random Forest RUL regressor ($R^2 = 0.3980, \text{RMSE} = 30.74\text{h}$), and MLflow model tracking & governance.
- **Phase 4: Lakehouse Serving, Trino Analytics & Operational Dashboards**: Hybrid dual-storage serving engine (PostgreSQL 16 operational store + Apache Iceberg historical store), deterministic mathematical Operational KPI & Maintenance Ranking Engine ($S_{priority}$), Apache Trino (v438) distributed SQL query coordinator with 21 modular analytical queries across 7 files, and Grafana 10 declarative Operations Center dashboard (16 panels across 5 rows).

---

## 2. Phase 1 Architecture

```text
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │                         INDUSTRIAL ASSET SIMULATOR                              │
 │  • 5 Assets (Motor, Pump, Compressor, Conveyor, Turbine)                        │
 │  • 8 Scenarios (Normal, Bearing, Thermal, Pressure, Electrical, Dropped, OOO)   │
 │  • First-Principles Equations: P = √3·V·I·cos(φ), Vibration ∝ (RPM/RPM_rated)²   │
 └────────────────────────────────────────┬────────────────────────────────────────┘
                                          │
                                          ▼
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │                           APACHE KAFKA STREAMING BUS                            │
 │  • Topics: industrial-telemetry, maintenance-events, asset-alerts, metrics     │
 │  • Partition Keying: asset_id (Preserves temporal ordering per machine)         │
 │  • Resilience: Socket probe + in-memory fallback for offline testability       │
 └────────────────────────────────────────┬────────────────────────────────────────┘
                                          │
                                          ▼
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │                        13-RULE DATA QUALITY ENGINE                              │
 │  • DQ-001..003: Required fields, data types, timestamp ISO-8601 validation     │
 │  • DQ-004..008: Physical boundary checks (RPM, Negative Vibration, Current)     │
 │  • DQ-009..013: Deduplication (Event ID, Logical), Out-of-Order & Delay tracking│
 └───────────────────┬─────────────────────────────────────────┬───────────────────┘
                     │ Valid (100%)                            │ Defective
                     ▼                                         ▼
 ┌───────────────────────────────────────┐ ┌───────────────────────────────────────┐
 │        APACHE ICEBERG LAKEHOUSE       │ │         POSTGRESQL METADATA           │
 │  • PyIceberg SqlCatalog + PyArrow     │ │  • Operational Tables: assets, runs,  │
 │  • Partition: Identity(asset_id)      │ │    data_quality_events, quarantine    │
 │  • Compression: zstd (level 3)        │ │  • DLQ Serialization & Audit Trail   │
 └───────────────────────────────────────┘ └───────────────────────────────────────┘
```

---

## 3. Quickstart & Installation

### Prerequisites
- Python 3.11+ / Python 3.12+ / Python 3.14+
- (Optional) Docker & Docker Compose (for running live Kafka and PostgreSQL containers)

### Installation
```bash
# Clone repository
git clone https://github.com/forgestream/forgestream.git
cd forgestream

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -e .
```

### Environment Configuration
Copy the example environment file:
```bash
cp .env.example .env
```

---

## 4. CLI Usage & Operations

ForgeStream includes a unified command-line interface `forgestream`:

```bash
# 1. Run full subsystem smoke test (validates Simulator, Kafka, DQ, Iceberg, Postgres)
python -m forgestream.cli smoke

# 2. Execute a live simulation pipeline run
python -m forgestream.cli run --duration 60 --seed 42 --batch-size 50

# 3. Verify Apache Iceberg table state and snapshot lineage
python -m forgestream.cli verify --limit 10

# 4. Generate all academic evidence artifacts
python scripts/generate_phase1_evidence.py

# 5. Execute Phase 4 Lakehouse Serving & Trino Demo
python scripts/run_phase4_demo.py

# 6. Run Phase 4 Performance Benchmarking
python scripts/benchmark_phase4_performance.py

# 7. Generate Phase 4 Machine-Readable Verification Artifacts
python scripts/generate_phase4_evidence.py
```

---

## 5. Test Suite Execution

ForgeStream provides a comprehensive multi-phase test suite (Unit, Integration, and E2E):

```bash
# Run full project test suite across all 4 phases
pytest tests/ -v

# Run Phase 4 specific test suites
pytest tests/unit/test_phase4_serving.py -v
pytest tests/integration/test_phase4_integration.py -v
pytest tests/e2e/test_phase4_e2e.py -v
```

---

## 6. Project Directory Structure

```text
ForgeStream/
├── forgestream/                      # Core platform package
│   ├── config.py                     # Central configuration management
│   ├── cli.py                        # Unified CLI (smoke, run, verify)
│   ├── simulator/                    # Industrial physics simulation engine
│   │   ├── asset_models.py           # 5 Equipment specifications & baselines
│   │   ├── physics.py                # Mathematical correlation formulation
│   │   ├── scenarios.py              # 8 Degradation & anomaly profiles
│   │   └── generator.py              # Deterministic PRNG telemetry generator
│   ├── schemas/                      # Telemetry & metadata schemas
│   │   └── telemetry_schema.py       # Pydantic v2 models & ground-truth isolation
│   ├── kafka/                        # Streaming bus interface
│   │   ├── topics.py                 # Topic definitions and provisioning
│   │   ├── producer.py               # Resilient producer with retry & metrics
│   │   └── consumer.py               # Telemetry consumer with offset tracking
│   ├── validation/                   # Data quality verification engine
│   │   ├── quality_rules.py          # 13 discrete DQ validation rules
│   │   ├── validator.py              # Validation orchestrator
│   │   └── quarantine.py             # Quarantine routing and serialization
│   ├── iceberg/                      # Pure Python Lakehouse storage
│   │   ├── catalog.py                # PyIceberg SqlCatalog factory
│   │   ├── tables.py                 # 17-column Iceberg schema & partition spec
│   │   ├── writer.py                 # PyArrow batching writer & ACID commits
│   │   └── reader.py                 # Lakehouse inspection & predicate scanner
│   ├── postgres/                     # Operational metadata repository
│   │   ├── schema.sql                # 6 Relational DDL schemas
│   │   ├── connection.py             # Engine manager with SQLite fallback
│   │   └── repository.py             # Typed repository CRUD operations
│   ├── streaming/                    # Phase 2 Real-time stream processing
│   │   ├── timestamps.py             # Event-time timestamp extraction
│   │   ├── watermarks.py             # Bounded out-of-orderness watermarks
│   │   ├── state.py                  # Keyed circular telemetry buffers
│   │   ├── windows.py                # Tumbling & sliding window analytics
│   │   ├── anomaly.py                # 3-level explainable anomaly detector
│   │   ├── health.py                 # Hysteresis state machine & health score
│   │   ├── alerts.py                 # Debounced alerting & recovery engine
│   │   ├── job.py                    # Reference stream processing engine
│   │   └── flink_job.py              # PyFlink DataStream distributed job
│   ├── ml/                           # Phase 3 ML prognostics & MLlib pipelines
│   │   ├── feature_pipeline.py       # 25-feature MLlib transformation pipeline
│   │   ├── labeling.py               # Zero-lookahead RUL & failure labeling
│   │   ├── leakage_auditor.py        # Strict whitelist & correlation scanner
│   │   ├── models/                   # Spark MLlib classification & regression
│   │   ├── tracking.py               # MLflow experiment tracking
│   │   ├── governance.py             # Promotion gates & champion registry
│   │   └── serving/                  # Downstream inference scoring contract
│   ├── serving/                      # Phase 4 Lakehouse serving & Trino analytics
│   │   ├── schemas.py                # Versioned Pydantic serving schemas
│   │   ├── kpis.py                   # Deterministic Operational KPI engine
│   │   ├── store.py                  # Dual-storage manager (PostgreSQL + Iceberg)
│   │   ├── service.py                # Serving orchestrator
│   │   └── trino_client.py           # Distributed Trino REST API client
│   └── observability/                # Telemetry & diagnostic logging
│       ├── logging.py                # Structured JSON logging
│       └── metrics.py                # In-memory metrics aggregator
├── grafana/                          # Declarative Grafana 10 provisioning
│   ├── provisioning/
│   │   ├── datasources/postgres.yml  # PostgreSQL datasource connection
│   │   └── dashboards/dashboards.yml # Automatic dashboard provider
│   └── dashboards/
│       └── forgestream_operations_center.json # 16-Panel Operations Center
├── trino/                            # Apache Trino coordinator configuration
│   └── etc/                          # node.properties, jvm.config, config.properties
├── sql/phase4/                       # 7 Modular analytical SQL scripts (21 queries)
├── tests/                            # Comprehensive multi-phase test suite
│   ├── unit/                         # Unit tests (Phase 1-4)
│   ├── integration/                  # Integration tests (Phase 1-4)
│   └── e2e/                          # End-to-end tests (Phase 1-4)
├── results/                          # Machine-readable verification JSON evidence
├── docs/                             # Engineering, architecture, and academic docs
│   ├── phase4/                       # Phase 4 comprehensive documentation (8 files)
│   └── report-evidence/              # CA-3 compliance matrix & evidence reports
├── docker-compose.yml                # Kafka, Postgres, Flink, Trino, Grafana stack
├── PROJECT_STATUS.md                 # Implementation roadmap and status
└── requirements.txt                  # Python dependencies
```

---

## 7. Academic Verification & Reference Run

The official **Phase 1 Reference Run** executed 1,000 events across 5 industrial assets with master seed `42`:
- **Generated Events**: 1,000
- **Persisted to Iceberg**: 1,000 (100.0% validation pass rate)
- **Quarantined Events**: 0
- **Snapshots Created**: 10 atomic PyIceberg commits
- **Effective Ingestion Throughput**: 1,515.12 events/sec (sub-millisecond commit latency)
- **Data Quality Effectiveness**: 100.0% detection rate across all 13 injected defect rules

For full audit evidence, see [`docs/report-evidence/phase1_evidence.md`](docs/report-evidence/phase1_evidence.md).
