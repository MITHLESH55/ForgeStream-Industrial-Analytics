# ForgeStream Project Status

> **Current Phase**: Phase 3 — Predictive Maintenance ML & Remaining Useful Life (RUL)  
> **Status**: **PASS — Verified with Real Apache Spark MLlib, MLflow Tracking, Target Leakage Audits, and Automated Tests**  
> **Academic Context**: B.Tech CSE (Semester VII), Big Data Analytics (CA-3), Symbiosis Institute of Technology, Pune  
> **Last Updated**: October 2026

---

## Phase Implementation Roadmap

| Phase | Description | Status | Test Coverage | Verification Artifacts |
| :---: | :--- | :---: | :---: | :--- |
| **Phase 1** | **Data Foundation & Streaming Infrastructure** | **COMPLETE (VERIFIED)** | **48/48 Tests Passed** | `results/live_*.json`, `results/phase1_*.json` |
| **Phase 2** | **Real-Time Stream Processing & Asset Health Intelligence** | **COMPLETE (VERIFIED)** | **23/23 Tests Passed** | `results/phase2_*.json` (17 artifacts) |
| **Phase 3** | **Predictive Maintenance ML & Remaining Useful Life (RUL)** | **COMPLETE (VERIFIED)** | **97 passed, 0 failed, 4 skipped** (4 optional external-service checks) | `results/phase3_*.json` (17 artifacts), `results/figures/` (8 figures) |
| **Phase 4** | Lakehouse Serving, Trino Analytics & Operational Dashboards | Planned | — | — |

---

## Phase 3 Machine Learning & Prognostics Summary

ForgeStream Phase 3 establishes the predictive analytics and prognostics layer:
1. **Target Formulation & Leakage Isolation**:
   - Continuous piecewise linear RUL: $y_{\text{RUL}}(t) = \min(T_{\text{max}}, \max(0.0, T_{\text{fail}} - t))$ with $T_{\text{max}} = 120.0\text{h}$.
   - Binary failure risk: $y_{\text{fail}}(t) = 1$ if $(T_{\text{fail}} - t) \le 24.0\text{h}$ else $0$.
   - Target Leakage & Causality Audit: **PASS (Zero Detected Leakage under Strict Whitelist and Correlation Scan)** across 25 operational features and 50 multi-asset runs ($30,000$ samples).
2. **Apache Spark MLlib Pipelines (local[*] Multi-Threaded Execution)**:
   - Feature Pipeline: 25 features (`StringIndexer` $\to$ `VectorAssembler` $\to$ `StandardScaler`).
   - Class Imbalance Weighting: $w_0 = 0.5882, w_1 = 3.3333$ computed strictly on training partition ($21,000$ records).
   - Champion Classification (Random Forest): **PR-AUC = 0.7666, Recall = 0.8512, ROC-AUC = 0.8491**.
   - Champion RUL Regression (Random Forest): **$R^2 = 0.3980$, RMSE = $30.74\text{h}$, $\text{Acc}_{\pm25\%} = 53.55\%$**.
3. **MLflow Tracking & Governance**:
   - SQLite tracking database (`sqlite:///data/mlflow.db`) with 4 logged runs, metrics across splits, and serialized model artifacts.
   - Deterministic Champion Promotion: Both Random Forest Classifier and Regressor promoted to `CHAMPION` (`v3.0.0-champion`) under project promotion gates.
4. **Downstream Serving Contract**:
   - Versioned Pydantic contract `AssetPredictionEvent` with top feature attributions, operational priority tiers, and sub-millisecond per-sample scoring latency.


---

## Stream Processing Architecture & Engine Provenance

ForgeStream Phase 2 provides dual streaming engine implementations with clear separation of runtime provenance:

1. **Real Apache Flink / PyFlink Distributed Engine (`forgestream.streaming.flink_job`)**:
   - **Runtime**: Apache Flink 1.18.1 cluster (JobManager + TaskManager containerized in Docker)
   - **Python Environment**: Python 3.10.12 with PyFlink 1.18.1, `flink-sql-connector-kafka-3.1.0-1.18.jar`, `flink-json-1.18.1.jar`
   - **Stream Execution**: Genuine PyFlink DataStream API topology with Kafka Source -> WatermarkStrategy (5s bounded out-of-orderness) -> Keyed by `asset_id` -> `ForgeStreamKeyedProcessFunction` with Flink managed `ValueState` -> Kafka Sinks (`forgestream.telemetry.alerts`, `forgestream.telemetry.windows`)
   - **State & Checkpointing**: `EXACTLY_ONCE` state backend snapshots to `file:///opt/forgestream/data/checkpoints` (Kafka Sink uses `AT_LEAST_ONCE` delivery)
   - **Measured Performance (Optimized Steady-State)**: **579.77 events/sec**, $p_{50} = 1.466\text{ms}$, $p_{95} = 2.329\text{ms}$, $p_{99} = 3.105\text{ms}$
   - **SLA Gate Status**:
     - **Latency SLA ($p_{95} < 10\text{ms}$)**: **PASS** ($2.329\text{ms} < 10\text{ms}$)
     - **Throughput SLA ($> 4,000\text{ ev/s}$)**: **NOT MET** on PyFlink single container (579.77 ev/s achieved; limited by Apache Beam / PyFlink gRPC inter-process socket serialization between JVM TaskManager and Python worker daemon)

2. **Deterministic Local Reference Engine (`forgestream.streaming.job`)**:
   - **Runtime**: Native Python event-time streaming engine (Windows Host Python 3.14)
   - **Role**: High-speed, local deterministic reference and testing engine implementing Flink-aligned event-time, bounded circular buffers ($N=120$), tumbling/sliding windows, and hysteresis alert state machine
   - **Measured Performance**: **4,567.18 events/sec**, $p_{50} = 0.130\text{ms}$, $p_{95} = 0.266\text{ms}$, $p_{99} = 0.479\text{ms}$

3. **Kafka KRaft Infrastructure Capacity**:
   - **Producer Throughput**: **32,929.35 events/sec** ($0.304\text{s}$ for 10,000 records)
   - **Consumer Throughput**: **39,061.26 events/sec** ($0.256\text{s}$ for 10,000 records)
   - **Bottleneck Identification**: Kafka broker is high-throughput (>30,000 ev/s); PyFlink's cross-process IPC boundary is the throughput constraint.

---

## Phase 2 Real Apache Flink Verification Matrix

| Subsystem | Execution Target | Real Flink Implementation | Status | Evidence File |
| :--- | :--- | :--- | :---: | :--- |
| **Flink Cluster Runtime** | Apache Flink 1.18.1 JobManager (8081) + TaskManager (4 slots) | REST API verification, task slot registration | **PASS** | `results/phase2_real_flink_runtime.json` |
| **Kafka Ingestion & Sinks** | Kafka Broker (`kafka:29092`), `forgestream.telemetry.raw` | `KafkaSource` + `KafkaSink` (`AT_LEAST_ONCE`) | **PASS** | `results/phase2_flink_e2e.json` |
| **Event-Time & Watermarks** | JSON timestamp parsing -> Monotonic event time | `WatermarkStrategy.for_bounded_out_of_orderness(5s)` + `TimestampAssigner` | **PASS** | `results/phase2_flink_event_time.json`<br>`results/phase2_flink_watermarks.json` |
| **Flink Managed Keyed State** | Partitioned per `asset_id` | Flink `ValueStateDescriptor` managing `AssetStreamingState` in Flink StateBackend | **PASS** | `results/phase2_flink_state.json` |
| **Window Analytics in Flink** | 5s Tumbling & 30s Sliding (5s slide) | Event-time window evaluation within KeyedProcessFunction | **PASS** | `results/phase2_flink_windows.json` |
| **3-Level Anomaly Detection** | L1 Bounds, L2 Contextual Z-Score/Slope, L3 Physics Correlations | Explainable rule execution inside Flink operators | **PASS** | `results/phase2_flink_state.json` |
| **Asset Health & Alerts** | Health score $H(t) \in [0.0, 1.0]$ with hysteresis | State machine transitions (`HEALTHY` $\leftrightarrow$ `WATCH` $\leftrightarrow$ `DEGRADED` $\leftrightarrow$ `CRITICAL`) | **PASS** | `results/phase2_flink_state.json` |
| **Checkpoint & Recovery** | State preservation & fault-tolerance | Flink `EXACTLY_ONCE` state snapshot to filesystem + verified state recovery | **PASS** | `results/phase2_flink_recovery.json` |
| **Real Flink Latency SLA** | Latency SLA Target ($p_{95} < 10\text{ms}$) | Measured $p_{50}=1.466\text{ms}$, $p_{95}=2.329\text{ms}$, $p_{99}=3.105\text{ms}$ | **PASS** | `results/phase2_flink_performance_optimized.json` |
| **Real Flink Throughput SLA** | Throughput SLA Target ($> 4,000\text{ ev/s}$) | Measured 579.77 ev/s (PyFlink gRPC loopback IPC bottleneck on 1 container) | **NOT MET** | `results/phase2_performance_analysis.json`<br>`results/phase2_performance_methodology.json` |
| **Phase 2 Functional Gate** | Functional Flink stream processing verification | All functional streaming intelligence components operational | **PASS** | `results/phase2_final_audit.json` |

---

## Phase 1 Subsystem Verification Matrix

| Subsystem | Live Infrastructure Target | Local Fallback Target | Verification Status | Evidence File |
| :--- | :--- | :--- | :---: | :--- |
| **Apache Kafka Bus** | KRaft Single Broker (`localhost:9092`), 5 topics, 5 partitions | In-memory thread-safe queue with offset & partition emulation | **PASS — verified with real infrastructure** | `results/live_kafka_verification.json` |
| **PostgreSQL Metadata** | PostgreSQL 16 Alpine (`localhost:5433`), `psycopg 3.3.6`, 6 DDL tables | SQLite 3 (`data/metadata.db`) via SQLAlchemy 2.0 | **PASS — verified with real infrastructure** | `results/live_postgresql_verification.json` |
| **Apache Iceberg Lakehouse** | PyIceberg 0.12.0 `SqlCatalog` + PyArrow 25.0.1 (`forgestream.historical_telemetry`) | SQLite / local filesystem warehouse (`data/warehouse`) | **PASS — verified with real infrastructure** | `results/phase1_reference_run.json` |
| **Data Quality Engine** | 13 Discrete Rules (`DQ-001` through `DQ-013`) + DLQ forward | In-memory validator + SQLite quarantine | **PASS — verified with real infrastructure** | `results/phase1_data_quality.json` |
| **Simulator & Physics** | 5 Asset Classes, 8 Degradation Scenarios, Correlated Physics | Deterministic NumPy PRNG (seed=42) | **PASS — verified with real infrastructure** | `results/phase1_smoke_test.json` |
| **Automated Test Suite** | 71 Unit, Integration, and E2E Pytest cases | All passing in < 6 seconds | **PASS — verified with real infrastructure** | `pytest tests/ -v` |
| **Git Repository** | Clean initial commit on `main` branch with safe `.gitignore` | No secrets, databases, or runtime caches tracked | **PASS — verified with real infrastructure** | `git log -1` |

---

## Phase 2 Technical Deliverables Summary

1. **PyFlink Stream Processing Topology (`forgestream/streaming/flink_job.py`)**:
   - `FlinkTelemetryTimestampAssigner`: extracts epoch milliseconds from incoming event payload.
   - `ForgeStreamKeyedProcessFunction`: Flink `KeyedProcessFunction` storing `AssetStreamingState` in Flink `ValueState`.
   - `build_flink_datastream_pipeline`: builds full pipeline connecting Kafka Source, Watermark Strategy, Keyed Stream, Keyed Process Function, and Kafka Sink.

2. **Deterministic Local Reference Engine (`forgestream/streaming/job.py`)**:
   - Complete local standalone implementation of Flink-aligned semantics for rapid developer testing and reference benchmark execution.

3. **Domain Intelligence Engines**:
   - `StreamingAnomalyDetector`: 3-level explainable anomaly detection (L1 Physical Bounds, L2 Contextual Z-score & Thermal rate of rise, L3 Multimodal Physical Correlations like bearing progression and cavitation).
   - `AssetHealthModel`: continuous health index $H(t) \in [0.0, 1.0]$ with asymmetric hysteresis state transitions.
   - `AlertQualityEngine`: production alert lifecycle management (cooldown timer, duplicate suppression, state transition alerts, recovery notifications).
   - `WindowAccumulator`: 5s tumbling and 30s sliding event-time window summarizers.
   - `BoundedOutOfOrdernessWatermarkGenerator`: 5s out-of-orderness tolerance with 30s allowed lateness policy.

4. **Automated Verification Harness (`scripts/verify_real_flink_pipeline.py`)**:
   - 6 automated verification suites running against live Apache Flink cluster, outputting structured JSON artifacts to `results/`.
