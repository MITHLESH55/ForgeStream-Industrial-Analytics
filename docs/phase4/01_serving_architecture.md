# ForgeStream Phase 4: Lakehouse Serving Architecture

## Executive Overview
ForgeStream Phase 4 implements a hybrid dual-store Lakehouse Serving Layer designed to bridge low-latency operational applications (real-time dashboards, alerting, prescriptive maintenance queues) with scalable historical big data analytics.

```
+-----------------------------------------------------------------------------------+
|                        FORGESTREAM LAKEHOUSE SERVING LAYER                        |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  [Streaming Telemetry & Prognostics Engine]                                       |
|     |                                                                             |
|     +---> LakehouseServingService (forgestream.serving.service)                   |
|              |                                                                    |
|              +---> Operational KPI & Ranking Engine (kpis.py)                     |
|              |                                                                    |
|              +---> Dual-Storage Serving Store Manager (store.py)                  |
|                       |                                                           |
|                       +---> [Relational Store: PostgreSQL 16] (Port 5433)         |
|                       |        * asset_current_state                              |
|                       |        * asset_prediction_history                         |
|                       |        * asset_alert_history                              |
|                       |        * maintenance_priority_queue                       |
|                       |        * fleet_kpi_snapshots                              |
|                       |                                                           |
|                       +---> [Analytical Store: Apache Iceberg Tables] (Parquet)   |
|                                * lakehouse.forgestream.asset_current_state        |
|                                * lakehouse.forgestream.asset_prediction_history   |
|                                * lakehouse.forgestream.asset_alert_history        |
|                                * lakehouse.forgestream.fleet_kpi_snapshots        |
|                                                                                   |
+-----------------------------------------------------------------------------------+
```

---

## Dual-Storage Design Justification

| Storage Dimension | PostgreSQL 16 Serving Store | Apache Iceberg Lakehouse Store |
|---|---|---|
| **Primary Workload** | Low-latency operational point lookups, single-stat Grafana KPI metrics, dynamic maintenance queue upserts. | Large-scale multi-month trend analysis, complex cohort aggregations, machine learning feature extraction. |
| **Write Pattern** | High-concurrency upserts and append batches ($< 100\text{ ms}$). | Partitioned Parquet batches with Zstandard compression and snapshot metadata isolation. |
| **Query Engine Interface** | Direct PostgreSQL JDBC / SQLAlchemy connection pool. | Apache Trino (v438) distributed SQL query engine. |
| **Data Retention** | Real-time sliding window snapshot (current state, active alerts, latest 30-day history). | Perpetual immutable lakehouse storage with Iceberg time-travel capabilities. |

---

## Serving Data Models & Schemas

### 1. `AssetCurrentState`
Captures the authoritative, real-time operational status of an individual industrial asset:
- `asset_id` (`VARCHAR(64)` PRIMARY KEY): Unique equipment identifier.
- `asset_type` (`VARCHAR(32)`): Equipment category (`GAS_TURBINE`, `CENTRIFUGAL_PUMP`, `RECIP_COMPRESSOR`, `STEAM_GEN`, `HYDRAULIC_PRESS`).
- `operating_mode` (`VARCHAR(32)`): Operating regime (`NORMAL`, `HEAVY`, `DEGRADED`, `SHUTDOWN`).
- `health_state` (`VARCHAR(32)`): Hysteresis state machine classification (`HEALTHY`, `WATCH`, `DEGRADED`, `CRITICAL`).
- `health_score` (`DOUBLE PRECISION`): Normalized continuous health metric $[0.0, 1.0]$.
- `failure_probability` (`DOUBLE PRECISION`): ML prognostic failure probability $[0.0, 1.0]$.
- `predicted_failure_risk` (`INT`): Binary risk indicator ($\tau \ge 0.50$).
- `predicted_rul_hours` (`DOUBLE PRECISION`): Estimated Remaining Useful Life in hours.
- `maintenance_priority` (`VARCHAR(32)`): Urgency tier (`LOW`, `MEDIUM`, `HIGH`, `EMERGENCY`).
- `anomaly_level` (`INT`): Highest active anomaly level (0, 1, 2, 3).
- `temperature`, `vibration`, `pressure`, `load`, `rpm`, `power`: Latest calibrated sensor readings.
- `last_event_time`, `last_prediction_time`: Monotonic UTC timestamps.

### 2. `AssetPredictionHistory`
Historical ledger of ML prognostic inference events for degradation trajectory tracking.

### 3. `AssetAlertHistory`
Historical record of state transition alerts, threshold violations, and automated recovery notifications.

### 4. `MaintenancePriorityItem` & `maintenance_priority_queue`
Dynamic ordered queue ranking all fleet assets by urgency score:
$$S_{priority} = 100 \times \left(0.45 \cdot P_{fail} + 0.35 \cdot \left(1.0 - \frac{\text{RUL}}{120.0}\right) + 0.20 \cdot C_{asset}\right)$$

### 5. `FleetHealthSummary` & `fleet_kpi_snapshots`
Time-series snapshots of executive KPIs aggregating fleet health score, health distribution, high-risk counts, and average RUL.
