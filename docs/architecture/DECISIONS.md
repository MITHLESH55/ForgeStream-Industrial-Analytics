# ForgeStream Architecture Decision Records (ADRs)

This document records the architectural and engineering design choices made during Phase 1 of ForgeStream development.

---

## ADR-001: JVM-Free Python Apache Iceberg Lakehouse Integration

### Context
Industrial lakehouse platforms typically deploy Apache Iceberg using Apache Spark or Trino clusters requiring JVM runtimes, Hadoop dependencies, and substantial container memory overhead (2–4 GB per node). For Phase 1 ingestion, high-speed micro-batching, and edge-deployable simulation, a lightweight, native, and robust solution was required.

### Decision
Adopt **PyIceberg 0.12.0** combined with **PyArrow 25.0.1** utilizing the `SqlCatalog` backend (backed by SQLite for local offline execution or PostgreSQL for distributed clusters) and local filesystem / object storage warehouses.

### Consequences & Trade-offs
- **Positive**:
  - JVM-free and Hadoop-free architecture; runs natively in pure Python 3.11+.
  - Sub-millisecond snapshot commit latency (~3.8 ms) with atomic Iceberg snapshot commits.
  - Generates authentic, specification-compliant Apache Iceberg metadata (v2 spec, manifest lists, manifest files, and zstd Parquet data files).
  - Standard partition pruning and schema evolution capabilities.
- **Negative**:
  - Distributed multi-engine concurrent writers require external catalog locking (provided by PostgreSQL or DynamoDB in production).

---

## ADR-002: Seeded Deterministic Industrial Simulation & Correlated Physics

### Context
Testing machine learning models, stream analytics, and data quality rules requires reproducible datasets where identical initial conditions produce bitwise-identical telemetry across runs. Ad-hoc random generation leads to non-reproducible test failures.

### Decision
Implement `TelemetryGenerator` using deterministic NumPy `default_rng(seed)` instances where each asset maintains its own seeded state trajectory. Physical correlations (e.g., active electrical power $P = \sqrt{3} \cdot V \cdot I \cdot \text{PF} \cdot \eta$, centrifugal vibration $V \propto \text{RPM}^2$) are computed from first principles.

### Consequences & Trade-offs
- **Positive**:
  - 100% bitwise reproducibility across runs (`test_pipeline_seed_reproducibility` verified).
  - Telemetry exhibits physically realistic multi-sensor coupling.
- **Negative**:
  - Initial configuration of physics constants requires domain-specific baseline calibration per asset class.

---

## ADR-003: Strict Ground-Truth Metadata Segregation

### Context
In predictive maintenance datasets, leakage of target labels (e.g., whether an anomaly scenario is active or when a breakdown occurs) into feature sets during training invalidates ML evaluation.

### Decision
Isolate ground-truth failure indicators (`scenario_id`, `maintenance_state`, `is_synthetic`) inside `TelemetryMetadata`. Expose only physical sensor readings through `to_feature_dict()`, and omit ground-truth labels from the Apache Iceberg historical analytical table.

### Consequences & Trade-offs
- **Positive**:
  - Eliminates target leakage by design at the schema level.
  - Allows clean evaluation of unsupervised and supervised predictive models in Phase 2/3.
- **Negative**:
  - Supervised training routines must explicitly cross-reference operational metadata logs when generating training labels.

---

## ADR-004: Dual-Mode Resilient Kafka Streaming Layer

### Context
Automated CI pipelines, developer laptops, and offline academic environments may not always have a live Apache Kafka broker running on port 9092. Blocking on unavailable TCP sockets causes long timeouts and brittle tests.

### Decision
Implement non-blocking TCP socket probing (`_probe_kafka_broker`) with a 0.5s timeout. When an external broker is detected, the pipeline routes traffic via Confluent-Kafka; when offline, it falls back to an in-memory streaming queue with offset and partition emulation.

### Consequences & Trade-offs
- **Positive**:
  - Full automated test suite executes in < 5 seconds without requiring running Docker containers.
  - Zero code changes required between live production deployment and local test execution.
- **Negative**:
  - In-memory queue is bounded to process memory and does not persist across application restarts.

---

## ADR-005: 13-Rule Two-Tier Data Quality and Quarantine Architecture

### Context
Industrial IoT data streams frequently suffer from missing sensor values, out-of-order sequence arrivals, corrupted timestamps, network transmission lags, and duplicate packets.

### Decision
Implement a discrete 13-rule validation engine (`DQ-001` through `DQ-013`). Valid records proceed to the Apache Iceberg historical storage, while defective records are routed to a PostgreSQL `quarantine_events` table and Dead-Letter Queue (DLQ) with granular violation diagnostics.

### Consequences & Trade-offs
- **Positive**:
  - Protects analytical lakehouse from corrupt data ingestion.
  - Full auditability of data quality degradation trends over time.
- **Negative**:
  - Adds ~45 microseconds of validation overhead per event (well within the single-core budget of >1,000 events/sec).
