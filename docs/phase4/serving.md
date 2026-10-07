# ForgeStream Phase 4: Lakehouse Serving Model & Store Specification

## 1. Executive Summary

The ForgeStream Serving Layer (`forgestream.serving`) orchestrates the materialization and delivery of analytical and operational datasets. It integrates raw sensor streams, real-time health intelligence (Phase 2), and machine learning prognostics (Phase 3) into queryable relational and lakehouse tables.

---

## 2. Serving Dataset Catalog & Schemas

ForgeStream defines six core logical serving datasets:

```
+-----------------------------+-------------------------------------------------------------------------+
| Serving Dataset             | Storage Engine & Workload Purpose                                       |
+-----------------------------+-------------------------------------------------------------------------+
| asset_current_state         | PostgreSQL (Operational Upsert) & Iceberg (Parquet Snapshot)            |
| asset_prediction_history    | PostgreSQL (Recent Query Log) & Iceberg (Append-Only Historical Ledger) |
| asset_alert_history         | PostgreSQL (Active Alert Queue) & Iceberg (Audit History)               |
| asset_telemetry_summary     | PostgreSQL (Window Aggregations) & Iceberg (Long-Term Aggregates)       |
| maintenance_priority_queue  | PostgreSQL (Real-Time Ranked Queue for Operators)                       |
| fleet_kpi_snapshots         | PostgreSQL (Executive Time-Series Snapshots)                            |
+-----------------------------+-------------------------------------------------------------------------+
```

### 2.1 `asset_current_state`
- **Purpose**: Provides a unified, single-row-per-asset snapshot of current operational conditions, physical sensor metrics, evaluated health state, failure risk, and RUL.
- **Primary Key**: `asset_id` (`VARCHAR(64)`).
- **Update Strategy**: Idempotent SQL UPSERT (Insert on new asset, Update on existing asset).
- **Schema**:
  | Column Name | SQL Data Type | Description |
  |---|---|---|
  | `asset_id` | `VARCHAR(64)` | Unique equipment identifier (Primary Key) |
  | `asset_type` | `VARCHAR(32)` | Equipment classification (`GAS_TURBINE`, `CENTRIFUGAL_PUMP`, etc.) |
  | `operating_mode` | `VARCHAR(32)` | Current operating regime (`NORMAL`, `HEAVY`, `DEGRADED`, `SHUTDOWN`) |
  | `health_state` | `VARCHAR(32)` | Discrete health state (`HEALTHY`, `WATCH`, `DEGRADED`, `CRITICAL`) |
  | `health_score` | `DOUBLE PRECISION` | Continuous health index $H(t) \in [0.0, 1.0]$ |
  | `failure_probability`| `DOUBLE PRECISION` | ML predicted failure probability within 24h horizon |
  | `predicted_failure_risk` | `INT` | Binary classification prediction ($1 = \text{High Risk}$, $0 = \text{Normal}$) |
  | `predicted_rul_hours` | `DOUBLE PRECISION` | Continuous predicted Remaining Useful Life in hours $[0.0, 120.0]$ |
  | `maintenance_priority`| `VARCHAR(32)` | Urgency category (`LOW`, `MEDIUM`, `HIGH`, `EMERGENCY`) |
  | `anomaly_level` | `INT` | Highest active anomaly level (0 = None, 1 = Bounds, 2 = Context, 3 = Physics) |
  | `active_alert_count` | `INT` | Count of unacknowledged active alerts |
  | `temperature` | `DOUBLE PRECISION` | Latest temperature in °C |
  | `vibration` | `DOUBLE PRECISION` | Latest vibration RMS in mm/s |
  | `pressure` | `DOUBLE PRECISION` | Latest pressure in bar |
  | `load` | `DOUBLE PRECISION` | Latest load percentage in % |
  | `rpm` | `DOUBLE PRECISION` | Latest rotational speed in RPM |
  | `power` | `DOUBLE PRECISION` | Latest electrical power in kW |
  | `last_event_time` | `TIMESTAMP WITH TZ` | UTC timestamp of latest ingested sensor reading |
  | `last_prediction_time` | `TIMESTAMP WITH TZ`| UTC timestamp of latest ML prognostic scoring |
  | `model_version` | `VARCHAR(64)` | Identifier of champion model used for scoring |
  | `updated_at` | `TIMESTAMP WITH TZ` | System modification timestamp |

### 2.2 `asset_prediction_history`
- **Purpose**: Immutable ledger recording every prognostic inference event emitted by Phase 3 ML models.
- **Primary Key**: `prediction_id` (`VARCHAR(64)`).
- **Update Strategy**: Append-only batch insert.
- **Partitioning (Iceberg)**: Partitioned by `asset_id` identity transform and daily event time.
- **Key Columns**: `prediction_id`, `asset_id`, `asset_type`, `timestamp`, `event_time`, `failure_probability`, `predicted_failure_risk`, `predicted_rul_hours`, `maintenance_priority`, `top_features_summary`, `model_version`, `inference_latency_ms`.

### 2.3 `asset_alert_history`
- **Purpose**: Operational event log tracking threshold violations, state machine transitions, and automated recovery events.
- **Primary Key**: `alert_id` (`VARCHAR(64)`).
- **Update Strategy**: Append-only batch insert.
- **Key Columns**: `alert_id`, `asset_id`, `asset_type`, `severity`, `health_state`, `health_score`, `is_state_transition`, `is_recovery`, `reason_codes`, `triggering_values`, `maintenance_priority`, `timestamp`, `event_time`.

### 2.4 `maintenance_priority_queue`
- **Purpose**: Dynamically computed and ranked maintenance work order queue for plant operators.
- **Primary Key**: `asset_id` (`VARCHAR(64)`).
- **Update Strategy**: Complete replacement per evaluation cycle (Atomic transaction: `DELETE FROM maintenance_priority_queue; INSERT INTO maintenance_priority_queue (...)`).
- **Key Columns**: `asset_id`, `ranking` (1 = Highest Urgency), `asset_type`, `criticality`, `health_state`, `health_score`, `failure_probability`, `predicted_rul_hours`, `maintenance_priority`, `priority_score` ($0.0-100.0$), `recommended_action`, `evaluated_at`.

### 2.5 `fleet_kpi_snapshots`
- **Purpose**: High-level time-series snapshots of executive KPIs used for temporal drift and fleet health monitoring.
- **Primary Key**: `snapshot_id` (`VARCHAR(64)`).
- **Update Strategy**: Append-only periodic insert.
- **Key Columns**: `snapshot_id`, `timestamp`, `total_assets`, `healthy_assets`, `watch_assets`, `degraded_assets`, `critical_assets`, `high_risk_assets`, `near_failure_assets`, `fleet_health_score`, `avg_predicted_rul_hours`, `failure_risk_rate`, `active_alerts_total`, `emergency_maintenance_count`, `high_maintenance_count`.

---

## 3. Relationship to Phase 2 (Streaming Intelligence)

Phase 4 serving directly consumes the output of Phase 2 streaming operators:
1. **Health Score $H(t)$**: The continuous health index computed by the Flink/Python streaming job is mapped to `asset_current_state.health_score`.
2. **Hysteresis State Transitions**: Transitions (`HEALTHY` $\leftrightarrow$ `WATCH` $\leftrightarrow$ `DEGRADED` $\leftrightarrow$ `CRITICAL`) generate records in `asset_alert_history`.
3. **Multi-Sensor Windows**: 30-second sliding windows and 5-second tumbling windows feed `asset_telemetry_summary`.

---

## 4. Relationship to Phase 3 (Prognostics & Serving Contract)

Phase 4 serving maintains strict backwards-compatibility with the Phase 3 `AssetPredictionEvent` contract:
- **Champion Models**: Evaluates predictions using Random Forest Classifier (PR-AUC 0.7666, Recall 0.8512) and Random Forest Regressor ($R^2 = 0.3980$, RMSE 30.74h).
- **Explainability**: Top contributing feature importances (e.g., vibration, temperature slope, load ratio) are serialized as JSON into `top_features_summary`.
- **Model Versioning**: Stamped with `v3.0.0-champion` across all tables.

---

## 5. Timestamp and Asset Key Semantics

- **Asset Keys**: Natural keys following the format `<EQUIPMENT_TYPE>-<INDEX>` (e.g., `TURBINE-001`, `PUMP-002`).
- **Timestamps**: All timestamps are stored in UTC ISO-8601 standard format (`YYYY-MM-DDTHH:MM:SS.mmmmmm+00:00`) with matching double precision epoch floats for sub-millisecond sorting and time-travel querying.
