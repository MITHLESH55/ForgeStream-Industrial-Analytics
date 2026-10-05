# ForgeStream: Real-Time Distributed Analytics Platform

> **Industrial Asset Health Monitoring and Predictive Maintenance**
> *Academic Context: B.Tech Computer Science and Engineering (Semester VII) — Big Data Analytics (CA-3)*  
> *Symbiosis Institute of Technology, Pune*

---

## 1. Executive Summary

**ForgeStream** is a real-time distributed streaming analytics and lakehouse platform engineered for high-frequency industrial equipment telemetry. In modern manufacturing and industrial facilities, critical assets such as induction motors, centrifugal pumps, screw compressors, conveyors, and gas turbines experience progressive physical degradation (e.g., bearing raceway fatigue, stator winding overheating, impeller cavitation, dynamic unbalance).

Phase 1 establishes the **Data Foundation and Streaming Infrastructure**, providing:
- **Physics-Correlated Industrial Simulator**: Deterministic generation across 5 asset types and 8 operational/degradation scenarios with strict ground-truth metadata isolation.
- **Apache Kafka Streaming Bus**: Low-latency message backbone with topic provisioning, resilient producers with automatic fallback, and partition keying on `asset_id`.
- **13-Rule Data Quality Engine**: Strict validation layer evaluating syntax, semantics, physical bounds, timestamp integrity, duplicate detection, and sequence ordering, routing defective records to quarantine.
- **Pure Python Apache Iceberg Lakehouse**: JVM-free Python Iceberg integration powered by PyIceberg 0.12.0 and PyArrow 25.0.1, featuring atomic Iceberg snapshot commits, metadata lineage, and identity partitioning.
- **PostgreSQL Operational Metadata Store**: Relational repository tracking asset registries, maintenance logs, ingestion runs, validation audits, and quarantine records.

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
```

---

## 5. Test Suite Execution

ForgeStream provides a comprehensive test suite with 45 unit, integration, and E2E tests:

```bash
# Run all tests
pytest tests/ -v

# Run unit tests (physics, determinism, scenarios, validation rules)
pytest tests/unit/ -v

# Run integration tests (Kafka, PyIceberg catalog, Postgres repository)
pytest tests/integration/ -v

# Run end-to-end tests (full pipeline lifecycle, fault injection & quarantine)
pytest tests/e2e/ -v
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
│   └── observability/                # Telemetry & diagnostic logging
│       ├── logging.py                # Structured JSON logging
│       └── metrics.py                # In-memory metrics aggregator
├── tests/                            # 45 Automated test suites
│   ├── unit/                         # Physics, determinism, scenario tests
│   ├── integration/                  # Storage & streaming component tests
│   └── e2e/                          # End-to-end pipeline & fault injection
├── results/                          # Machine-readable benchmark evidence
│   ├── phase1_reference_run.json     # Official 1,000-event benchmark output
│   ├── phase1_data_quality.json      # 13-rule validation campaign results
│   ├── phase1_smoke_test.json        # Subsystem health verification matrix
│   └── phase1_resource_usage.json    # Resource footprint & throughput profile
├── docs/                             # Engineering & academic documentation
│   ├── architecture/
│   │   ├── phase1.md                 # Detailed Phase 1 architecture
│   │   └── DECISIONS.md              # Architecture Decision Records (ADRs)
│   └── report-evidence/
│       ├── phase1_evidence.md        # Academic report evidence summary
│       └── CA3_EVIDENCE_MATRIX.md    # CA-3 course rubric compliance matrix
├── docker-compose.yml                # Kafka & PostgreSQL infrastructure
├── VERSION_LOCK.md                   # Exact dependency and tool version pins
├── PROJECT_STATUS.md                 # Phase-by-phase implementation status
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
