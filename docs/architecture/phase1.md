# ForgeStream Phase 1 Architecture: Data Foundation & Streaming Infrastructure

## 1. System Overview

ForgeStream Phase 1 provides the end-to-end data foundation and real-time streaming pipeline for industrial equipment telemetry. Industrial plants generate continuous streams of high-frequency sensor readings (vibration, temperature, fluid pressure, rotational velocity, current, voltage, and electrical power). The platform ingests, validates, routes, stores, and audits this telemetry with mathematical rigor and ACID consistency.

```text
+------------------------+      +-----------------------+      +-------------------------+
| Industrial Simulator   | ---> | Apache Kafka (KRaft)  | ---> | Data Quality Validator  |
| • 5 Equipment Models   |      | • 5 Partitioned Topics|      | • 13 Discrete DQ Rules  |
| • Seeded Determinism   |      | • Key = asset_id      |      | • Strict Bounds & Types |
+------------------------+      +-----------------------+      +------------+------------+
                                                                            |
                                               +----------------------------+---------------------------+
                                               | (Valid: is_valid == True)                              | (Defect: is_valid == False)
                                               v                                                        v
                                +------------------------------+                         +------------------------------+
                                | Pure Python Apache Iceberg   |                         | PostgreSQL Metadata Store    |
                                | • SqlCatalog + PyArrow       |                         | • quarantine_events / DLQ    |
                                | • Partition: Identity(asset) |                         | • data_quality_events audit  |
                                | • zstd Parquet Historical    |                         | • ingestion_runs lifecycle   |
                                +------------------------------+                         +------------------------------+
```

---

## 2. Industrial Equipment Simulator

### 2.1 Equipment Models & Rated Baselines
The simulator models five critical industrial equipment classes:
1. **Induction Motor (`MOTOR-001`)**: 1,750 RPM rated, 400V, 55A, 45 kW nominal load.
2. **Centrifugal Slurry Pump (`PUMP-001`)**: 2,900 RPM rated, 400V, 45A, 6.5 bar operating discharge pressure.
3. **Screw Air Compressor (`COMPRESSOR-001`)**: 3,600 RPM rated, 400V, 95A, 8.0 bar system pressure.
4. **Heavy Bulk Conveyor (`CONVEYOR-001`)**: 120 RPM rated, 400V, 25A, 12 kW mechanical drive.
5. **High-Pressure Gas Turbine (`TURBINE-001`)**: 5,400 RPM rated, 6.6 kV, 180A, 1,200 kW base generation.

### 2.2 Mathematical Physics Correlations
The simulator implements physical equations to prevent unphysical telemetry generation:

1. **3-Phase Electrical Active Power**:
   $$P(t) = \sqrt{3} \cdot V(t) \cdot I(t) \cdot \text{PF} \cdot \eta \times 10^{-3} \quad (\text{kW})$$
   where $V(t)$ is RMS line-to-line voltage, $I(t)$ is line current, $\text{PF} \approx 0.88$ is power factor, and $\eta \approx 0.92$ is motor efficiency.

2. **Rotational Vibration Energy**:
   $$V_{\text{rms}}(t) = V_{\text{baseline}} \cdot \left(\frac{\text{RPM}(t)}{\text{RPM}_{\text{rated}}}\right)^2 \cdot (1 + \text{severity}_{\text{fault}})$$
   Centrifugal forces scale quadratically with rotational speed.

3. **Impeller Discharge Pressure (Pump Affinity Laws)**:
   $$P_{\text{discharge}}(t) = P_{\text{baseline}} \cdot \left(\frac{\text{RPM}(t)}{\text{RPM}_{\text{rated}}}\right)^{1.8} \cdot \left(\frac{\text{Load}(t)}{100}\right)^{0.5}$$

4. **Thermal Dissipation Dynamics**:
   $$T(t) = T_{\text{ambient}} + \Delta T_{\text{nominal}} \cdot \left(\frac{I(t)}{I_{\text{rated}}}\right)^2 + \text{fault\_thermal\_accumulation}(t)$$

### 2.3 Degradation Scenarios & Ground-Truth Isolation
The simulator defines 8 operational scenarios:
- `SCENARIO_001_NORMAL_OPERATION`: Nominal baseline with stochastic Gaussian noise.
- `SCENARIO_002_BEARING_DEGRADATION`: High-frequency vibration spikes and localized heating.
- `SCENARIO_003_OVERHEATING`: Stator thermal runaway exceeding $120^\circ\text{C}$.
- `SCENARIO_004_PRESSURE_ANOMALY`: Severe pressure oscillation and impeller cavitation.
- `SCENARIO_005_ELECTRICAL_INSTABILITY`: Phase voltage imbalance and current surges.
- `SCENARIO_006_SENSOR_MISSING_DATA`: Intermittent sensor dropouts.
- `SCENARIO_007_DELAYED_EVENTS`: Network transmission lags.
- `SCENARIO_008_OUT_OF_ORDER_EVENTS`: Clock skew and reordered sequence arrival.

**Target Leakage Isolation Guarantee**:
Ground-truth failure indicators (`scenario_id`, `maintenance_state`, `is_synthetic`) are encapsulated inside `TelemetryMetadata` and strictly excluded from the analytical feature dictionary (`to_feature_dict()`), ensuring zero data leakage into predictive models.

---

## 3. Streaming Bus: Apache Kafka (KRaft Mode)

### 3.1 Topic Definitions & Partitioning
ForgeStream provisions 5 dedicated streaming topics:
| Topic Name | Partitions | Retention | Keying Strategy | Purpose |
| :--- | :---: | :---: | :---: | :--- |
| `forgestream.telemetry.v1` | 5 | 7 days | `asset_id` | High-frequency physical sensor telemetry |
| `forgestream.maintenance.v1`| 3 | 30 days | `asset_id` | Maintenance work orders & component replacements |
| `forgestream.alerts.v1` | 3 | 14 days | `asset_id` | Threshold breach alarms & operational alerts |
| `forgestream.models.v1` | 1 | 30 days | `model_id` | RUL score broadcasts & model weights |
| `forgestream.metrics.v1` | 1 | 7 days | `service_name`| System health & ingestion telemetry |

### 3.2 Resilience & Connectivity Strategy
The producer and consumer implement non-blocking socket probing (`_probe_kafka_broker`). When an external Kafka broker is reachable on port 9092, events flow via Confluent-Kafka with TCP linger (`linger.ms=5`) and batch compression (`snappy`). When running in offline developer mode or isolated unit test environments, the client transparently shifts to an in-memory queue, preserving 100% functional testability.

---

## 4. 13-Rule Data Quality Verification Engine

Every ingested record is evaluated by the `DataQualityValidator` against 13 deterministic validation rules:

| Rule Code | Rule Name | Category | Enforcement Condition |
| :--- | :--- | :--- | :--- |
| **DQ-001** | `REQUIRED_FIELDS_MISSING` | Schema | All 14 mandatory fields present (event_id, asset_id, rpm, temp, etc.) |
| **DQ-002** | `INVALID_DATA_TYPES` | Schema | Type validation (float numeric fields, string IDs, valid ISO datetime) |
| **DQ-003** | `INVALID_TIMESTAMP` | Temporal | $0.0 \le t \le t_{\text{now}} + 3600\text{s}$, valid ISO-8601 string parse |
| **DQ-004** | `UNSUPPORTED_OPERATING_MODE` | Domain | Mode $\in \{\text{NORMAL, IDLE, STARTUP, HIGH\_LOAD, MAINTENANCE, DEGRADED}\}$ |
| **DQ-005** | `IMPOSSIBLE_NUMERIC_VALUE` | Physical | No `NaN`, `Inf`, and temperature $\le 1500^\circ\text{C}$ |
| **DQ-006** | `OUT_OF_RANGE_RPM` | Physical | $0.0 \le \text{RPM} \le 50,000.0$ |
| **DQ-007** | `NEGATIVE_VIBRATION` | Physical | $\text{Vibration RMS} \ge 0.0\text{ mm/s}$ |
| **DQ-008** | `NEGATIVE_ELECTRICAL` | Physical | $\text{Current} \ge 0.0\text{A}, \text{Voltage} \ge 0.0\text{V}, \text{Power} \ge 0.0\text{ kW}$ |
| **DQ-009** | `DUPLICATE_EVENT_ID` | Integrity | $H(\text{event\_id}) \notin \mathcal{S}_{\text{seen}}$ within deduplication window |
| **DQ-010** | `DUPLICATE_LOGICAL_EVENT` | Integrity | $(\text{asset\_id}, \text{timestamp}) \notin \mathcal{S}_{\text{logical}}$ |
| **DQ-011** | `MALFORMED_JSON` | Syntax | Valid JSON object syntax parseable to dictionary |
| **DQ-012** | `DELAYED_EVENT` | Streaming | $\Delta t_{\text{arrival}} - t_{\text{event}} \le t_{\text{watermark\_threshold}}$ |
| **DQ-013** | `OUT_OF_ORDER_EVENT` | Streaming | Sequence monotonicity: $S_{n} \ge S_{n-1}$ per `asset_id` |

---

## 5. Apache Iceberg Historical Lakehouse Layer

### 5.1 Zero-JVM PyIceberg Architecture
ForgeStream utilizes PyIceberg 0.12.0 and PyArrow 25.0.1 configured with `SqlCatalog`. This eliminates JVM/Hadoop overhead while ensuring standard Apache Iceberg Lakehouse capabilities:
- **Table Name**: `forgestream.historical_telemetry`
- **Partitioning**: Identity transform on `asset_id` (creating partitioned directories `asset_id=MOTOR-001/`, etc.)
- **Format**: Apache Parquet with zstd compression (level 3)
- **ACID Commits**: Snapshot manifest lists and JSON metadata specifications with commit tracking.

### 5.2 Schema Specification (17 Fields)
The formal Iceberg schema binds strictly to:
`event_id` (string), `asset_id` (string), `asset_type` (string), `timestamp` (double), `event_time` (timestamptz), `ingestion_time` (timestamptz), `temperature` (double), `vibration` (double), `pressure` (double), `rpm` (double), `current` (double), `voltage` (double), `power` (double), `load` (double), `operating_mode` (string), `sequence_number` (long), `schema_version` (string).

---

## 6. PostgreSQL Operational Metadata Store

PostgreSQL manages operational persistence across 6 ACID relational tables:
1. `assets`: Master asset registry and nominal mechanical/electrical ratings.
2. `maintenance_history`: Work order records, component replacements, and cost audits.
3. `ingestion_runs`: Execution logs tracking events generated, persisted, quarantined, and wall-clock latencies.
4. `data_quality_events`: Granular diagnostic audit trail for every rule violation.
5. `quarantine_events`: Dead-letter storage of defective payloads with serialized error diagnostics.
6. `experiment_runs`: Model training runs and benchmark lineage.
