# ForgeStream Phase 4: Lakehouse Serving, Trino Analytics & Operational Dashboards Architecture

## 1. System Overview & End-to-End Architecture

ForgeStream Phase 4 represents the final operationalization and analytical serving phase of the platform. It turns the upstream data ingestion (Phase 1), distributed stream processing (Phase 2), and machine learning prognostics (Phase 3) into an interactive, distributed analytical query layer and operational command center.

```
                                  ┌─────────────────────────────────────────┐
                                  │      Industrial Asset Telemetry         │
                                  │      (5 Equipment Classes, Physics)     │
                                  └────────────────────┬────────────────────┘
                                                       │
                                                       ▼
                                  ┌─────────────────────────────────────────┐
                                  │           Apache Kafka (KRaft)          │
                                  │   Topic: `forgestream.telemetry.raw`    │
                                  └────────────────────┬────────────────────┘
                                                       │
                                                       ▼
                                  ┌─────────────────────────────────────────┐
                                  │        Apache Flink 1.18.1 Streaming    │
                                  │  - Event-time & Watermarking (5s)       │
                                  │  - 3-Tier Anomaly Detection (L1/L2/L3)  │
                                  │  - Dynamic Health Scoring H(t) [0.0,1.0]│
                                  │  - Sliding/Tumbling Window Analytics    │
                                  └──────────────┬──────────────────┬───────┘
                                                 │                  │
                      ┌──────────────────────────┘                  └──────────────────────────┐
                      ▼                                                                        ▼
   ┌─────────────────────────────────────┐                                  ┌─────────────────────────────────────┐
   │        Apache Iceberg Lakehouse     │                                  │      Phase 3 ML Prognostics         │
   │  - `historical_telemetry`           │                                  │  - Spark MLlib Champion RF Classif  │
   │  - `asset_current_state`            │                                  │  - Spark MLlib Champion RF Regress  │
   │  - Partitioned Parquet Storage      │                                  │  - Failure Probability P_fail (24h) │
   │  - ACID Snapshots & Time-Travel     │                                  │  - Remaining Useful Life RUL (120h) │
   └──────────────────┬──────────────────┘                                  └──────────────────┬──────────────────┘
                      │                                                                        │
                      └──────────────────────────┬─────────────────────────────────────────────┘
                                                 │
                                                 ▼
                                  ┌─────────────────────────────────────────┐
                                  │    PHASE 4 LAKEHOUSE SERVING LAYER      │
                                  │   (`forgestream.serving.service`)       │
                                  │  - Dual-Storage Persistence Orchestrator│
                                  │  - Mathematical Operational KPI Engine  │
                                  │  - Prescriptive Maintenance Ranker      │
                                  └──────────────┬──────────────────┬───────┘
                                                 │                  │
                      ┌──────────────────────────┘                  └──────────────────────────┐
                      ▼                                                                        ▼
   ┌─────────────────────────────────────┐                                  ┌─────────────────────────────────────┐
   │    Operational Store (PostgreSQL 16)│                                  │    Distributed Query Engine (Trino) │
   │  - `asset_current_state` (Upserts)  │                                  │  - Apache Trino 438 (Port 8085)     │
   │  - `maintenance_priority_queue`     │                                  │  - Federated SQL Catalog Execution  │
   │  - `fleet_kpi_snapshots`            │                                  │  - 21 Analytical SQL Queries        │
   │  - `asset_prediction_history`       │                                  │  - Cross-Catalog Joins (TPCH/PG)    │
   └──────────────────┬──────────────────┘                                  └──────────────────┬──────────────────┘
                      │                                                                        │
                      └──────────────────────────┬─────────────────────────────────────────────┘
                                                 │
                                                 ▼
                                  ┌─────────────────────────────────────────┐
                                  │   GRAFANA 10 OPERATIONS CENTER          │
                                  │   (Port 3000, Declarative Provisioning) │
                                  │  - ROW 1: Executive Fleet KPIs (6 Stats)│
                                  │  - ROW 2: Asset Health & Sensor Registry│
                                  │  - ROW 3: Predictive Maintenance Queue  │
                                  │  - ROW 4: Temporal Trajectories & Alerts│
                                  │  - ROW 5: Asset Diagnostics & Outliers  │
                                  └─────────────────────────────────────────┘
```

---

## 2. Four Operational Planes

### 2.1 Data Plane
- **Responsibility**: Reliable, high-throughput simulation and ingestion of high-frequency industrial IoT sensor data.
- **Components**: Multi-asset physics simulator (`forgestream.simulator`), Kafka KRaft broker (`localhost:9092`), PyFlink streaming workers.
- **Guarantees**: Monotonic timestamps, bounded out-of-orderness tolerance (5.0s), strictly schema-validated telemetry events (`forgestream.schemas.telemetry_schema`).

### 2.2 Serving Plane
- **Responsibility**: State materialization, schema transformation, and dual-storage persistence bridging low-latency operational access and large-scale lakehouse history.
- **Components**: `LakehouseServingService`, `ServingStoreManager`, PostgreSQL 16 dual-storage relational store, Apache Iceberg PyIceberg catalog (`forgestream_catalog`).
- **Guarantees**: Atomic updates to asset current state, sub-second batch ingestion processing ($p_{50} = 117.43\text{ms}, p_{99} = 155.74\text{ms}$), deterministic maintenance prioritization.

### 2.3 Analytical Plane
- **Responsibility**: Distributed, federated, massively parallel processing (MPP) SQL querying across operational and historical datasets.
- **Components**: Apache Trino v438 coordinator (`localhost:8085`), PostgreSQL connector (`trino/etc/catalog/postgres.properties`), TPCH benchmark connector (`trino/etc/catalog/tpch.properties`).
- **Guarantees**: ANSI-SQL compliance, sub-200ms query execution across 21 analytical templates, zero data duplication for ad-hoc aggregations.

### 2.4 Operational Plane
- **Responsibility**: Live monitoring, visual situational awareness, and prescriptive action dispatching for plant floor operators and reliability engineers.
- **Components**: Grafana 10 (`localhost:3000`), declarative provisioning (`grafana/provisioning`), 16-panel Operations Center dashboard.
- **Guarantees**: Real-time visual updates, clear color-coded health state indicators (`HEALTHY` = Green, `WATCH` = Yellow, `DEGRADED` = Orange, `CRITICAL` = Red), actionable work order recommendations.

---

## 3. Technology Responsibilities & Trade-Offs

| Technology | Role in ForgeStream | Why Chosen | Operational Trade-off |
|---|---|---|---|
| **Apache Kafka** | Real-time event streaming bus | High throughput (>30k ev/s), KRaft mode (no ZooKeeper), partitioned topic ordering. | Retains data for bounded retention window; not intended for complex ad-hoc queries. |
| **Apache Flink** | Real-time stream analytics & health scoring | Native event-time processing, low-latency keyed state, robust watermarking. | Python gRPC daemon IPC boundary on single container limits throughput to ~580 ev/s. |
| **Apache Iceberg** | Lakehouse historical storage format | Schema evolution, partition pruning, ACID snapshot isolation, open table standard. | Metadata write overhead per commit; optimized for analytical batch reads, not high-frequency single-row point updates. |
| **Apache Spark MLlib** | Distributed ML training & batch inference | Scalable pipeline APIs, zero-leakage feature assembly, native evaluation metrics. | JVM memory footprint requires careful executor/driver memory tuning. |
| **PostgreSQL 16** | Operational serving & transactional state | Low-latency point lookups ($< 5\text{ms}$), ACID transactions, primary key upserts. | Vertical scaling limits; not suitable for petabyte-scale raw telemetry storage. |
| **Apache Trino (v438)** | Distributed MPP SQL query engine | ANSI-SQL federation across PostgreSQL, Iceberg, and TPCH catalogs; separation of compute from storage. | Memory-bound execution; requires coordinator JVM allocation tuning. |
| **Grafana 10** | Operational command center | Rich native time-series & relational panels, declarative YAML/JSON provisioning. | Read-only presentation layer; depends on underlying database query latency. |

---

## 4. Separation of Historical vs. Operational Data

ForgeStream Phase 4 enforces a strict architectural boundary between **Operational State** and **Historical Lakehouse Analytics**:

```
                         ┌─────────────────────────────────────────────────────────┐
                         │              Incoming Evaluated Stream                  │
                         └────────────────────────────┬────────────────────────────┘
                                                      │
                       ┌──────────────────────────────┴──────────────────────────────┐
                       ▼                                                             ▼
    ┌─────────────────────────────────────────┐                   ┌─────────────────────────────────────────┐
    │       OPERATIONAL SERVING STORE         │                   │        HISTORICAL LAKEHOUSE             │
    │         (PostgreSQL 16 Engine)          │                   │       (Apache Iceberg / Parquet)        │
    ├─────────────────────────────────────────┤                   ├─────────────────────────────────────────┤
    │ * Latest asset snapshot (1 row / asset) │                   │ * Immutable append-only historical log  │
    │ * Active unacknowledged alerts          │                   │ * Multi-month degradation trajectories  │
    │ * Dynamic maintenance work order queue  │                   │ * Model training & feature backtesting  │
    │ * High-frequency dashboard polling      │                   │ * Long-term compliance & failure audits │
    │ * Single-digit millisecond latency      │                   │ * Optimized for columnar compression    │
    └─────────────────────────────────────────┘                   └─────────────────────────────────────────┘
```

---

## 5. End-to-End Data Contracts

The pipeline maintains rigorous Pydantic-validated contracts at every boundary:

1. **`TelemetryEvent` (Phase 1)**: Raw sensor readings (`temperature`, `vibration`, `pressure`, `load`, `rpm`, `power`, `voltage`, `current`, `timestamp`, `operating_mode`).
2. **`AssetAlertEvent` (Phase 2)**: Real-time streaming alert events with severity, health score, hysteresis transitions, reason codes, and triggering values.
3. **`AssetPredictionEvent` (Phase 3)**: Prognostic inference record containing `failure_probability`, `predicted_failure_risk`, `predicted_rul_hours`, `maintenance_priority`, and `top_contributing_features`.
4. **`AssetCurrentState` (Phase 4)**: Unified operational state snapshot merging latest telemetry, health state, and ML predictions.
5. **`MaintenancePriorityItem` (Phase 4)**: Ranked maintenance work order with composite urgency score $S_{priority}$ and explainable recommended action.
6. **`FleetHealthSummary` (Phase 4)**: Aggregated executive snapshot ($FHS$, health state distribution, high-risk rate, average RUL).

---

## 6. Query and Dashboard Flow

1. **Telemetry & Stream Evaluation**: Telemetry is processed by `StreamProcessingJob` to evaluate health scores and anomalies.
2. **Prognostic Inference**: ML models compute failure probability ($P_{fail} \in [0.0, 1.0]$) and RUL ($RUL \in [0.0, 120.0\text{h}]$).
3. **Serving Ingestion**: `LakehouseServingService` updates `asset_current_state`, appends to `asset_prediction_history` and `asset_alert_history`, and executes `OperationalKPIEngine`.
4. **Trino Query Execution**: Ad-hoc analytical queries access `postgres.public.*` or `lakehouse.forgestream.*` via Trino's REST/JDBC interface.
5. **Grafana Rendering**: Grafana polls `postgres:5432` at configurable refresh intervals (default: 10s) using parameterized SQL queries to update the 16 Operations Center panels.

---

## 7. Fault-Tolerance, Failure Handling & Reproducibility

- **Database Connection Retries**: `ServingStoreManager` automatically detects database connectivity and supports SQLite in-memory / file fallback if PostgreSQL is temporarily unavailable.
- **Trino Polling Resilience**: `TrinoClient` handles asynchronous execution polling with configurable timeouts, capturing exact error diagnostics upon query failure.
- **Deterministic Simulation**: All simulation runs support fixed random seeds (`seed=42`) ensuring bitwise reproducible evaluation datasets.
- **Auditability**: Every serving batch emits structured execution metadata, timing logs, and machine-readable evidence files in `results/`.

---

## 8. Security Considerations

- **Credential Isolation**: All database passwords and service credentials are configured via environment variables and `.env` overrides; zero hardcoded secrets exist in the repository.
- **Network Boundaries**: Trino and PostgreSQL communicate over isolated Docker container networks (`postgres:5432`); host access is routed through dedicated host port bindings (`5433`, `8085`, `3000`).
- **Read-Only Provisioning**: Grafana datasource definitions are provisioned as non-editable configuration artifacts, preventing unauthorized runtime tampering.
