<div align="center">
⚙️ ForgeStream
Real-Time Distributed Analytics Platform for Industrial Asset Health Monitoring & Predictive Maintenance

An end-to-end streaming, lakehouse, and machine learning platform for high-frequency industrial equipment telemetry.

Show Image Show Image Show Image Show Image Show Image Show Image Show Image Show Image Show Image Show Image

B.Tech Computer Science & Engineering (Semester VII) — Big Data Analytics (CA-3) Symbiosis Institute of Technology, Pune

</div>
📑 Table of Contents
Overview
Key Features
System Architecture
Phase-by-Phase Breakdown
Phase 1 — Data Foundation & Streaming Infrastructure
Phase 2 — Real-Time Stream Processing & Asset Health Intelligence
Phase 3 — Predictive Maintenance ML & Remaining Useful Life
Phase 4 — Lakehouse Serving, Trino Analytics & Dashboards
Technology Stack
Quickstart & Installation
CLI Usage & Operations
Test Suite
Project Directory Structure
Academic Verification & Results
Documentation
Author & Acknowledgements
🔭 Overview

ForgeStream is an end-to-end real-time distributed streaming analytics, lakehouse, and machine learning platform engineered for high-frequency industrial equipment telemetry. It simulates physics-correlated sensor data from industrial assets, validates and stores it in an ACID lakehouse, processes it in real time to derive explainable health intelligence, predicts failures and Remaining Useful Life (RUL) with machine learning, and serves the results through distributed SQL and an operations dashboard.

The platform spans 4 fully implemented and verified phases:

Phase	Theme	Highlights
1	Data Foundation & Streaming Infrastructure	5 asset types, 8 degradation scenarios, Kafka (KRaft), 13-rule DQ & quarantine engine, Iceberg lakehouse, PostgreSQL 16
2	Real-Time Stream Processing & Asset Health Intelligence	Event-time processing, 5 s watermarks, keyed state, tumbling & sliding windows, 3-level explainable anomaly detection, Flink 1.18.1
3	Predictive Maintenance ML & RUL	25 MLlib features, zero-lookahead leakage audit, Random Forest classifier & regressor, MLflow governance
4	Lakehouse Serving, Trino Analytics & Dashboards	Dual-storage serving, deterministic KPI & maintenance ranking, Trino v438 (21 queries), Grafana 10 (16 panels)
✨ Key Features
🏭 Physics-correlated simulation — first-principles equations (e.g. P = √3·V·I·cos(φ), Vibration ∝ (RPM/RPM_rated)²) across 5 asset types and 8 scenarios, with a deterministic seeded PRNG for reproducibility.
📡 Resilient streaming bus — Apache Kafka (KRaft) keyed by asset_id to preserve per-machine temporal ordering, with a socket probe and in-memory fallback for offline testability.
🛡️ 13-rule Data Quality engine — schema, boundary, deduplication, out-of-order and delay checks with quarantine routing and an audit trail.
🧊 ACID lakehouse — Apache Iceberg (PyIceberg + PyArrow) with identity partitioning on asset_id, zstd compression and snapshot lineage.
⏱️ Event-time stream processing — bounded out-of-orderness watermarks (5 s), keyed circular buffers (N = 120), tumbling and sliding windows.
🔍 Explainable health intelligence — 3-level anomaly detector, hysteresis state machine and debounced alerting with recovery.
🤖 Governed ML — zero-lookahead labeling, strict leakage auditing, MLflow tracking and promotion gates.
📊 Hybrid serving & analytics — PostgreSQL (operational) + Iceberg (historical), queried through Trino and visualized in Grafana.
🏗️ System Architecture
End-to-End Platform View
Phase 4 · Serving & Analytics
Phase 3 · Predictive ML
Phase 2 · Stream Processing
Phase 1 · Data Foundation
valid
defective
Industrial Asset Simulator5 assets · 8 scenarios
Apache KafkaKRaft
13-Rule DQ Engine
Apache IcebergLakehouse
PostgreSQL 16Metadata & Quarantine
Flink 1.18.1Event-time · Windows ·State
3-Level Anomaly Detection
Health State Machine& Alerting
Spark MLlib25 Features
Leakage Auditor
RF ClassifierRF RUL Regressor
MLflowGovernance
KPI & Priority Engine
PostgreSQL + IcebergDual Store
Trino v43821 queries
Grafana 1016 panels
Data Flow Sequence
Serving / Trino / Grafana
ML Pipeline
Flink Job
PostgreSQL
Iceberg
DQ Validator
Kafka
Simulator
Serving / Trino / Grafana
ML Pipeline
Flink Job
PostgreSQL
Iceberg
DQ Validator
Kafka
Simulator
alt
[Record passes all 13 rules]
[Record is defective]
Publish telemetry (key = asset_id)
1
Consume with offset tracking
2
Batched ACID commit (PyArrow)
3
Quarantine + audit event
4
Event-time stream
5
Watermark, window, detect anomalies
6
Health scores & alerts
7
Historical telemetry
8
Features → leakage audit → train
9
Failure probability & RUL
10
KPIs, ranking, dashboards
11
🧩 Phase-by-Phase Breakdown
Phase 1 — Data Foundation & Streaming Infrastructure

A physics-correlated telemetry simulator across 5 asset types (Motor, Pump, Compressor, Conveyor, Turbine) and 8 degradation scenarios (Normal, Bearing, Thermal, Pressure, Electrical, Dropped, Out-of-Order, and more), feeding an Apache Kafka (KRaft) message bus, a 13-rule Data Quality & Quarantine engine, an Apache Iceberg lakehouse, and a PostgreSQL 16 operational repository.

Valid
Defective
<b> INDUSTRIAL ASSETSIMULATOR </b>5 Assets: Motor · Pump ·Compressor · Conveyor ·Turbine8 Scenarios: Normal ·Bearing · Thermal · Pressure· Electrical · Dropped · OOOP = √3·V·I·cos φ | Vibration∝ (RPM/RPM_rated)²
<b> APACHE KAFKASTREAMING BUS </b>Topics: industrial-telemetry ·maintenance-events ·asset-alerts · metricsPartition key: asset_id(per-machine ordering)Socket probe + in-memoryfallback
<b> 13-RULE DATAQUALITY ENGINE </b>DQ-001..003 Required fields· types · ISO-8601timestampsDQ-004..008 Physicalboundary checksDQ-009..013 Deduplication ·out-of-order · delaytracking
<b> APACHE ICEBERGLAKEHOUSE </b>PyIceberg SqlCatalog +PyArrowPartition: Identity(asset_id)Compression: zstd level 3
<b> POSTGRESQLMETADATA </b>assets · runs ·data_quality_events ·quarantineDLQ serialization & audittrail

Data Quality rule groups

Rule IDs	Category	Description
DQ-001 – DQ-003	Structural	Required fields, data types, ISO-8601 timestamp validation
DQ-004 – DQ-008	Physical	Boundary checks (RPM, negative vibration, current, etc.)
DQ-009 – DQ-013	Temporal / Integrity	Event-ID and logical deduplication, out-of-order and delay tracking
Phase 2 — Real-Time Stream Processing & Asset Health Intelligence

An event-time streaming engine with 5 s bounded out-of-orderness watermarks, keyed state (N = 120 circular buffers), tumbling and sliding windows, 3-level explainable anomaly detection, and a hysteresis state machine for health modeling. A reference engine (job.py) and a distributed PyFlink DataStream job (flink_job.py) run on a containerized Apache Flink 1.18.1 cluster.

TelemetryStream
Event-timeTimestamps
Watermarks5 s bounded OOO
Keyed StateN = 120 circular buffer
Tumbling &Sliding Windows
3-Level ExplainableAnomaly Detector
Hysteresis State Machine+ Health Score
Debounced Alerts& Recovery
asset-alertstopic

Health state machine (conceptual)

score below enter-threshold(sustained)
score keeps falling
score above exit-threshold(sustained)
score above exit-threshold(sustained)
Healthy
Degraded
Critical
Hysteresis prevents stateflapping:enter and exit thresholdsdiffer
Phase 3 — Predictive Maintenance ML & Remaining Useful Life

Apache Spark MLlib pipelines compute 25 engineered features from lakehouse telemetry. A zero-lookahead labeler and a strict leakage auditor (whitelist + correlation scanner) protect model validity. Two Random Forest models are trained and governed through MLflow.

pass
pass
IcebergTelemetry
Feature Pipeline25 MLlib features
Zero-lookaheadRUL & Failure Labels
Leakage Auditorwhitelist + correlation scan
Random ForestFailure Classifier
Random ForestRUL Regressor
MLflow Tracking
GovernancePromotion Gates
ChampionRegistry
Inference ScoringContract

Model performance

Model	Task	Metrics
Random Forest Classifier	Binary failure prediction	PR-AUC = 0.7666, Recall = 0.8512
Random Forest Regressor	Remaining Useful Life	R² = 0.3980, RMSE = 30.74 h
Phase 4 — Lakehouse Serving, Trino Analytics & Dashboards

A hybrid dual-storage serving engine (PostgreSQL 16 operational store + Apache Iceberg historical store), a deterministic Operational KPI & Maintenance Ranking Engine producing a maintenance priority score (S_priority), an Apache Trino v438 distributed SQL coordinator with 21 analytical queries across 7 SQL files, and a declaratively provisioned Grafana 10 Operations Center (16 panels across 5 rows).

Phase 2Health & Alerts
Serving Orchestratorservice.py
Phase 3Failure Prob. & RUL
KPI EngineS_priority ranking
Dual-Storage Managerstore.py
PostgreSQL 16Operational / hot
Apache IcebergHistorical / cold
Trino v43821 queries · 7 files
Grafana 10Operations Center16 panels · 5 rows
Trino REST Clienttrino_client.py
🧰 Technology Stack
Layer	Technology
Simulation	Python, deterministic PRNG, Pydantic v2
Messaging	Apache Kafka (KRaft mode)
Stream processing	Apache Flink 1.18.1 (PyFlink DataStream)
Lakehouse	Apache Iceberg, PyIceberg (SqlCatalog), PyArrow, zstd
Operational DB	PostgreSQL 16 (SQLite fallback for offline runs)
Machine learning	Apache Spark MLlib, MLflow
Distributed SQL	Trino v438
Visualization	Grafana 10 (declarative provisioning)
Testing	pytest (unit · integration · E2E)
Containerization	Docker & Docker Compose
🚀 Quickstart & Installation
Prerequisites
Python 3.11+
(Optional) Docker & Docker Compose — to run the live Kafka, PostgreSQL, Flink, Trino, and Grafana containers. Without Docker, the platform falls back to an in-memory Kafka substitute and SQLite.
Installation
bash
# Clone repository
git clone https://github.com/forgestream/forgestream.git
cd forgestream

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -e .
Environment Configuration
bash
cp .env.example .env
Launch the Full Stack (optional)
bash
docker compose up -d
docker-compose.yml
Kafka
Flink 1.18.1
PostgreSQL 16
Grafana 10
Trino v438
💻 CLI Usage & Operations

ForgeStream ships with a unified command-line interface, forgestream:

bash
# 1. Full subsystem smoke test (Simulator, Kafka, DQ, Iceberg, Postgres)
python -m forgestream.cli smoke

# 2. Execute a live simulation pipeline run
python -m forgestream.cli run --duration 60 --seed 42 --batch-size 50

# 3. Verify Iceberg table state and snapshot lineage
python -m forgestream.cli verify --limit 10

# 4. Generate all Phase 1 academic evidence artifacts
python scripts/generate_phase1_evidence.py

# 5. Execute the Phase 4 Lakehouse Serving & Trino demo
python scripts/run_phase4_demo.py

# 6. Run Phase 4 performance benchmarking
python scripts/benchmark_phase4_performance.py

# 7. Generate Phase 4 machine-readable verification artifacts
python scripts/generate_phase4_evidence.py
Command	Purpose
cli smoke	Validates every Phase 1 subsystem end to end
cli run	Runs a live simulation → Kafka → DQ → Iceberg pipeline
cli verify	Inspects Iceberg snapshots and lineage
scripts/generate_phase1_evidence.py	Produces Phase 1 evidence for the CA-3 report
scripts/run_phase4_demo.py	Demonstrates serving + Trino queries
scripts/benchmark_phase4_performance.py	Benchmarks Phase 4 performance
scripts/generate_phase4_evidence.py	Writes JSON evidence to results/
🧪 Test Suite

A comprehensive multi-phase suite covers unit, integration, and end-to-end layers.

bash
# Run the full project test suite across all 4 phases
pytest tests/ -v

# Phase 4 specific suites
pytest tests/unit/test_phase4_serving.py -v
pytest tests/integration/test_phase4_integration.py -v
pytest tests/e2e/test_phase4_e2e.py -v
Unit Teststests/unit
Integration Teststests/integration
End-to-End Teststests/e2e
📂 Project Directory Structure
text
ForgeStream/
├── forgestream/                      # Core platform package
│   ├── config.py                     # Central configuration management
│   ├── cli.py                        # Unified CLI (smoke, run, verify)
│   ├── simulator/                    # Industrial physics simulation engine
│   │   ├── asset_models.py           # 5 equipment specifications & baselines
│   │   ├── physics.py                # Mathematical correlation formulation
│   │   ├── scenarios.py              # 8 degradation & anomaly profiles
│   │   └── generator.py              # Deterministic PRNG telemetry generator
│   ├── schemas/
│   │   └── telemetry_schema.py       # Pydantic v2 models & ground-truth isolation
│   ├── kafka/                        # Streaming bus interface
│   │   ├── topics.py                 # Topic definitions and provisioning
│   │   ├── producer.py               # Resilient producer with retry & metrics
│   │   └── consumer.py               # Consumer with offset tracking
│   ├── validation/                   # Data quality verification engine
│   │   ├── quality_rules.py          # 13 discrete DQ validation rules
│   │   ├── validator.py              # Validation orchestrator
│   │   └── quarantine.py             # Quarantine routing and serialization
│   ├── iceberg/                      # Pure-Python lakehouse storage
│   │   ├── catalog.py                # PyIceberg SqlCatalog factory
│   │   ├── tables.py                 # 17-column Iceberg schema & partition spec
│   │   ├── writer.py                 # PyArrow batching writer & ACID commits
│   │   └── reader.py                 # Lakehouse inspection & predicate scanner
│   ├── postgres/                     # Operational metadata repository
│   │   ├── schema.sql                # 6 relational DDL schemas
│   │   ├── connection.py             # Engine manager with SQLite fallback
│   │   └── repository.py             # Typed repository CRUD operations
│   ├── streaming/                    # Phase 2 · real-time stream processing
│   │   ├── timestamps.py             # Event-time timestamp extraction
│   │   ├── watermarks.py             # Bounded out-of-orderness watermarks
│   │   ├── state.py                  # Keyed circular telemetry buffers
│   │   ├── windows.py                # Tumbling & sliding window analytics
│   │   ├── anomaly.py                # 3-level explainable anomaly detector
│   │   ├── health.py                 # Hysteresis state machine & health score
│   │   ├── alerts.py                 # Debounced alerting & recovery engine
│   │   ├── job.py                    # Reference stream processing engine
│   │   └── flink_job.py              # PyFlink DataStream distributed job
│   ├── ml/                           # Phase 3 · ML prognostics & MLlib
│   │   ├── feature_pipeline.py       # 25-feature MLlib transformation pipeline
│   │   ├── labeling.py               # Zero-lookahead RUL & failure labeling
│   │   ├── leakage_auditor.py        # Strict whitelist & correlation scanner
│   │   ├── models/                   # Spark MLlib classification & regression
│   │   ├── tracking.py               # MLflow experiment tracking
│   │   ├── governance.py             # Promotion gates & champion registry
│   │   └── serving/                  # Downstream inference scoring contract
│   ├── serving/                      # Phase 4 · lakehouse serving & Trino
│   │   ├── schemas.py                # Versioned Pydantic serving schemas
│   │   ├── kpis.py                   # Deterministic operational KPI engine
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
│       └── forgestream_operations_center.json   # 16-panel Operations Center
├── trino/etc/                        # node.properties, jvm.config, config.properties
├── sql/phase4/                       # 7 modular analytical SQL scripts (21 queries)
├── tests/                            # Multi-phase test suite
│   ├── unit/                         # Unit tests (Phase 1–4)
│   ├── integration/                  # Integration tests (Phase 1–4)
│   └── e2e/                          # End-to-end tests (Phase 1–4)
├── results/                          # Machine-readable verification JSON evidence
├── docs/                             # Engineering, architecture & academic docs
│   ├── phase4/                       # Phase 4 documentation (8 files)
│   └── report-evidence/              # CA-3 compliance matrix & evidence reports
├── docker-compose.yml                # Kafka, Postgres, Flink, Trino, Grafana stack
├── PROJECT_STATUS.md                 # Implementation roadmap and status
└── requirements.txt                  # Python dependencies
📈 Academic Verification & Results
Phase 1 Reference Run

The official reference run executed 1,000 events across 5 industrial assets with master seed 42.

Metric	Result
Generated events	1,000
Persisted to Iceberg	1,000 (100.0 % validation pass rate)
Quarantined events	0
Snapshots created	10 atomic PyIceberg commits
Effective ingestion throughput	1,515.12 events/sec (sub-millisecond commit latency)
DQ detection effectiveness	100.0 % across all 13 injected defect rules
Phase 3 Model Metrics
Model	Metric	Value
Failure classifier	PR-AUC	0.7666
Failure classifier	Recall	0.8512
RUL regressor	R²	0.3980
RUL regressor	RMSE	30.74 h
Phase 4 Deliverables
Component	Count
Trino analytical queries	21 (across 7 SQL files)
Grafana dashboard panels	16 (across 5 rows)
Phase 4 documentation files	8

📎 Full audit evidence: docs/report-evidence/phase1_evidence.md

📚 Documentation
Resource	Location
Implementation roadmap & status	PROJECT_STATUS.md
Phase 1 audit evidence	docs/report-evidence/phase1_evidence.md
CA-3 compliance matrix	docs/report-evidence/
Phase 4 documentation	docs/phase4/
Machine-readable results	results/
🎓 Author & Acknowledgements

Author: MITHLESH YADAV — B.Tech CSE (Semester VII) 
Institution: Symbiosis Institute of Technology, Pune

Built with Apache Kafka, Apache Flink, Apache Iceberg, Apache Spark, MLflow, Trino, PostgreSQL, and Grafana.

<div align="center">

ForgeStream — from raw telemetry to maintenance decisions.

</div>
