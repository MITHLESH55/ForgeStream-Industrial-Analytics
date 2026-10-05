# ForgeStream Phase 2 Architecture: Real-Time Stream Processing & Asset Health Intelligence

## 1. System Architecture Overview

Phase 2 introduces the distributed, stateful, event-time stream processing layer for ForgeStream. Building on Phase 1's data foundation (physics-correlated simulator, Kafka KRaft broker on `localhost:9092`, PostgreSQL 16 on `localhost:5433`, and PyIceberg lakehouse), Phase 2 consumes live telemetry from Kafka topic `industrial-telemetry` and performs real-time windowed feature engineering, multi-level explainable anomaly detection, asset health indexing, and alert debouncing.

```text
+---------------------------------------------------------------------------------------------------+
|                                INDUSTRIAL ASSET SIMULATOR (Phase 1)                                |
|  • 5 Equipment Models: Induction Motor, Slurry Pump, Screw Compressor, Conveyor, Gas Turbine       |
|  • 8 Degradation Scenarios: Normal, Bearing Wear, Overheating, Cavitation, Electrical, Lag, OOO   |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v  (Topic: industrial-telemetry, Key: asset_id)
+---------------------------------------------------------------------------------------------------+
|                                   APACHE KAFKA (KRaft Mode)                                       |
|  • 5 Topics: industrial-telemetry, maintenance-events, asset-alerts, model-events, system-metrics |
|  • Partition Key: asset_id (Guarantees per-machine FIFO sequencing)                                |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
|                        FORGESTREAM REAL-TIME STREAM PROCESSING ENGINE                             |
|                                                                                                   |
|  1. Event-Time & Watermarks:                                                                      |
|     • Event-Time Extractor: TelemetryEvent.timestamp (Epoch seconds)                              |
|     • Bounded-Out-Of-Orderness Watermark Generator (Delay = 5.0s, Allowed Lateness = 30.0s)       |
|                                                                                                   |
|  2. Keyed Stream by asset_id -> Keyed State with Bounded TTL:                                    |
|     • Circular Buffers: Rolling sensor history (Temp, Vibration, Pressure, Current, Power, Load)  |
|     • State TTL: 24-hour expiration, Bounded O(1) memory per asset                                |
|                                                                                                   |
|  3. Streaming Feature Engineering:                                                                |
|     • Rolling Mean & Std Dev, Thermal Rise Rate (dT/dt °C/min), Vibration Slope (dV/dt mm/s/min)  |
|     • Current-to-Rated Ratio, Load Mismatch, Pressure Instability Index                           |
|                                                                                                   |
|  4. Windowed Analytics:                                                                           |
|     • Short-Term Tumbling Window (5s): Instantaneous spike & excursion detection                  |
|     • Medium-Term Sliding Window (30s, slide 5s): Trend evaluation & thermal escalation           |
|     • Long-Term Degradation Window (120s, slide 10s): Baseline drift & mechanical wear            |
|                                                                                                   |
|  5. 3-Level Explainable Anomaly Detection:                                                        |
|     • Level 1 (Engineering Bounds): Physical limits, ISO 10816 Zone D vibration trip              |
|     • Level 2 (Contextual Deviation): Rolling z-score > 3.0, thermal rise without load increase   |
|     • Level 3 (Multi-Signal Physics Correlation): Coupled faults (e.g. cavitation, bearing wear)  |
|                                                                                                   |
|  6. Deterministic Asset Health Model:                                                             |
|     • Normalized Health Score H(t) in [0.0, 1.0] derived from weighted physical sub-scores        |
|     • Hysteresis State Machine: HEALTHY (0.85-1.0), WATCH (0.65-0.84),                             |
|                                 DEGRADED (0.40-0.64), CRITICAL (<0.40)                            |
|                                                                                                   |
|  7. Alert Quality & Debounce Engine:                                                              |
|     • State transition detection, duplicate suppression, 30s cooldown, recovery events            |
|                                                                                                   |
|  8. Operational Metrics:                                                                          |
|     • Real-time throughput, watermark lag, late arrival tracking, p95 latency histograms          |
+-----------------------------------+-----------------------------------+---------------------------+
                                    |                                   |
                                    v (Topic: asset-alerts)             v (Topic: system-metrics)
+---------------------------------------------------+ +---------------------------------------------+
|               KAFKA: asset-alerts                 | |            KAFKA: system-metrics            |
|  • Asset ID, Severity, Health Score, State        | |  • Ingestion Rate, Processing Rate          |
|  • Anomaly Type, Structured Reason Codes          | |  • Watermark Lag, Late Events Count         |
|  • Triggering Sensor Values & Supporting Features | |  • Anomaly Count, Alerts Emitted, Latency   |
+---------------------------------------------------+ +---------------------------------------------+
```

---

## 2. Event-Time Processing & Watermark Strategy

Industrial sensor telemetry is subject to network transmission jitter, edge buffering delays, and clock drift. To guarantee deterministic temporal analytics independent of processing arrival times, ForgeStream implements strict **event-time semantics**.

### 2.1 Timestamp Extraction
The event timestamp is extracted directly from `TelemetryEvent.timestamp` (epoch seconds with microsecond resolution).

### 2.2 Bounded Out-of-Orderness Watermark Generation
Watermarks quantify the progress of event time. ForgeStream implements a bounded out-of-orderness generator:
$$W(t) = \max_{e \in \text{Observed}}(t_e) - t_{\text{delay}}$$
where $t_{\text{delay}} = 5.0\text{ seconds}$, derived from observed maximum network lag in Phase 1's `SCENARIO_007_DELAYED_EVENTS`.

### 2.3 Lateness Handling & Routing Policy
- **On-Time Events** ($t_{\text{event}} \ge W(t)$): Processed immediately in the primary streaming pipeline.
- **Late Events within Allowed Lateness** ($W(t) - 30\text{s} \le t_{\text{event}} < W(t)$): Accepted, updating sliding window accumulators and keyed asset state.
- **Excessively Late Events** ($t_{\text{event}} < W(t) - 30\text{s}$): Diverted to the late-event side output, counted in system metrics, and routed to quarantine.

---

## 3. Keyed State & Memory Architecture

Every streaming operation is keyed by `asset_id`, ensuring strict temporal isolation and parallel execution per physical asset.

### 3.1 AssetStateStore Schema
Each asset maintains a bounded, in-memory state object:
- `asset_id` (string), `asset_type` (string), `last_event_time` (float)
- **Bounded Circular Buffers** ($N=120$ samples, representing 2 minutes of continuous history at 1.0 Hz):
  - `temp_buffer`, `vib_buffer`, `pres_buffer`, `current_buffer`, `power_buffer`, `load_buffer`
- **Health State Tracking**:
  - `current_health_score` (float: 0.0 - 1.0)
  - `current_health_state` (`HEALTHY`, `WATCH`, `DEGRADED`, `CRITICAL`)
  - `state_entered_time` (float)
- **Fault Persistence Tracking**:
  - `consecutive_anomalies` (int)
  - `active_anomaly_codes` (list of strings)
  - `fault_counters` (dict mapping fault code to consecutive event count)
- **Alert History**:
  - `last_alert_state` (string)
  - `last_alert_time` (float)
  - `total_alerts_emitted` (int)

### 3.2 State Bounding & TTL Eviction
- **Buffer Bound**: Circular arrays with fixed capacity $N=120$ guarantee $O(1)$ memory per asset.
- **Time-To-Live (TTL)**: Inactive asset states expire after 24 hours of zero telemetry.

---

## 4. Streaming Feature Engineering

Features are computed on-the-fly from incoming events and rolling state buffers:

| Feature Name | Formulation / Logic | Window Scope | Purpose |
| :--- | :--- | :---: | :--- |
| `rolling_mean_temp` | $\mu_T = \frac{1}{k}\sum_{i=1}^k T_i$ | 30s Sliding | Thermal baseline tracking |
| `rolling_std_temp` | $\sigma_T = \sqrt{\frac{1}{k}\sum (T_i - \mu_T)^2}$ | 30s Sliding | Thermal stability indicator |
| `thermal_rise_rate` | $\frac{dT}{dt} = \frac{60 \cdot \sum (t_i - \bar{t})(T_i - \bar{T})}{\sum (t_i - \bar{t})^2}$ (°C/min) | 30s Sliding | Rapid overheating detection |
| `rolling_mean_vib` | $\mu_V = \frac{1}{k}\sum_{i=1}^k V_i$ (mm/s) | 30s Sliding | Mechanical vibration baseline |
| `vibration_slope` | $\frac{dV}{dt} = \frac{60 \cdot \sum (t_i - \bar{t})(V_i - \bar{V})}{\sum (t_i - \bar{t})^2}$ (mm/s/min) | 30s Sliding | Progressive mechanical wear |
| `current_load_ratio` | $R_I = \frac{I(t)}{I_{\text{rated}} \cdot (\text{Load}(t) / 100)}$ | Instantaneous | Electrical overloading efficiency |
| `pressure_instability` | $\sigma_P = \text{std}(\text{Pressure}_{10s})$ (bar) | 10s Tumbling | Fluid pump cavitation / compressor surging |
| `power_factor_discrepancy` | $\Delta \text{PF} = |P(t) - \sqrt{3} V(t) I(t) \text{PF}_{\text{nom}} \eta \times 10^{-3}|$ | Instantaneous | Electrical phase imbalance |

---

## 5. 3-Level Explainable Anomaly Detection

To eliminate unexplainable black-box alarms, ForgeStream implements a hierarchical 3-level detection framework:

```text
Level 1: Deterministic Engineering Bounds
  ├── Extreme Temperature Trip (Temp > Rated + 30°C)
  ├── ISO 10816 Zone D Vibration Trip (Vibration > 12.0 mm/s)
  └── Overcurrent Trip (Current > 1.35 * Rated)
        │
        ▼ (If nominal, evaluate Level 2)
Level 2: Contextual & Rate-of-Change Deviations
  ├── Thermal Escalation (dT/dt > 4.0 °C/min at constant load)
  ├── Vibration Z-Score Breach (|V - μ_V| / σ_V > 3.0)
  └── Pressure Oscillation Spike (σ_P > 2.0 * Baseline)
        │
        ▼ (If nominal, evaluate Level 3)
Level 3: Multi-Signal Physics Correlations
  ├── Bearing Degradation: Rising Vibration + High-Frequency Jitter + Local Thermal Rise
  ├── Impeller Cavitation: Pressure Oscillation + Current Drop + Vibration Spike
  └── Electrical Instability: Current Surge + Voltage Sag + High PF Discrepancy
```

Each detected anomaly outputs structured **reason codes** detailing triggering thresholds and sensor values.

---

## 6. Deterministic Asset Health Model & Alert Suppression

### 6.1 Asset Health Index Formulation
The asset health score $H(t) \in [0.0, 1.0]$ aggregates weighted physical sub-scores:
$$H(t) = 1.0 - \left(w_{\text{vib}} \cdot D_{\text{vib}} + w_{\text{temp}} \cdot D_{\text{temp}} + w_{\text{pres}} \cdot D_{\text{pres}} + w_{\text{elec}} \cdot D_{\text{elec}} + w_{\text{persist}} \cdot D_{\text{persist}}\right)$$
where:
- $w_{\text{vib}} = 0.35$ (Mechanical vibration degradation)
- $w_{\text{temp}} = 0.25$ (Thermal stress)
- $w_{\text{pres}} = 0.15$ (Fluid/pressure dynamics)
- $w_{\text{elec}} = 0.15$ (Electrical parameters)
- $w_{\text{persist}} = 0.10$ (Fault persistence multiplier)

### 6.2 Hysteresis State Machine
To prevent rapid flapping between operational health states, state transitions require passing asymmetric recovery thresholds:

| Current State | Next State Trigger | Recovery Threshold (Returning to previous) |
| :--- | :--- | :--- |
| `HEALTHY` | $H(t) < 0.85 \implies$ `WATCH` | $H(t) \ge 0.90 \implies$ `HEALTHY` |
| `WATCH` | $H(t) < 0.65 \implies$ `DEGRADED` | $H(t) \ge 0.72 \implies$ `WATCH` |
| `DEGRADED` | $H(t) < 0.40 \implies$ `CRITICAL` | $H(t) \ge 0.50 \implies$ `DEGRADED` |

### 6.3 Alert Debouncing & Cooldown
- **State Transition Emission**: Alerts are emitted when an asset transitions across health boundaries (e.g. `HEALTHY` $\to$ `WATCH`).
- **Cooldown Window**: In-state alert repetition is suppressed for $t_{\text{cooldown}} = 30.0\text{ seconds}$.
- **Severity Escalation**: A new alert is emitted immediately if severity escalates (e.g., `WARNING` $\to$ `CRITICAL`) regardless of cooldown.
- **Recovery Notification**: When an asset recovers to a superior state, a structured recovery event is published to `asset-alerts`.

---

## 7. Stream Outputs & Schema Definitions

### 7.1 Topic `asset-alerts` Schema
```json
{
  "alert_id": "ALERT-MOTOR-001-1791205000",
  "asset_id": "MOTOR-001",
  "asset_type": "MOTOR",
  "event_time": "2026-10-05T12:30:00.000Z",
  "severity": "CRITICAL",
  "health_state": "CRITICAL",
  "health_score": 0.342,
  "anomaly_type": "BEARING_DEGRADATION",
  "reason_codes": [
    "VIBRATION_ZONE_D_EXCEEDED",
    "THERMAL_RISE_ACCELERATING",
    "PERSISTENT_FAULT_COUNT_5"
  ],
  "triggering_values": {
    "vibration": 14.2,
    "temperature": 94.5,
    "thermal_rise_rate": 5.2
  },
  "maintenance_priority": "EMERGENCY",
  "schema_version": "2.0.0"
}
```

### 7.2 Topic `system-metrics` Schema
```json
{
  "metric_id": "METRIC-STREAM-1791205000",
  "timestamp": "2026-10-05T12:30:00.000Z",
  "window_duration_sec": 5.0,
  "events_ingested": 50,
  "events_processed": 50,
  "events_late": 0,
  "events_quarantined": 0,
  "anomalies_detected": 3,
  "alerts_emitted": 1,
  "active_assets": 5,
  "watermark_lag_sec": 0.042,
  "processing_latency_p95_ms": 3.4,
  "schema_version": "2.0.0"
}
```

---

## 8. Dual Engine Execution & Provenance Model

ForgeStream Phase 2 maintains two distinct, verified streaming execution pathways:

1. **Real Apache Flink / PyFlink Distributed Engine (`forgestream.streaming.flink_job`)**:
   - **Runtime**: Apache Flink 1.18.1 cluster (JobManager + TaskManager containerized in Docker).
   - **Topology**: PyFlink DataStream API topology with Kafka Source, event-time timestamp assigners, bounded out-of-orderness watermarks (5s delay), keyed streams on `asset_id`, `ForgeStreamKeyedProcessFunction` with Flink managed `ValueState`, and Kafka Sinks.
   - **State Backend & Delivery Semantics**: Flink `EXACTLY_ONCE` state backend snapshots to `file:///opt/forgestream/data/checkpoints`; Kafka Sink configured with `AT_LEAST_ONCE` delivery guarantee.
   - **Measured Performance (Optimized Steady-State)**: 579.77 events/sec, $p_{50} = 1.466\text{ms}$, $p_{95} = 2.329\text{ms}$, $p_{99} = 3.105\text{ms}$.
   - **SLA Evaluation**:
     - Latency SLA ($p_{95} < 10\text{ms}$): **PASS** ($2.329\text{ms}$).
     - Throughput SLA ($> 4,000\text{ ev/s}$): **NOT MET** on PyFlink single container (579.77 ev/s achieved; bounded by Apache Beam / PyFlink gRPC inter-process socket serialization between JVM TaskManager and Python worker daemon).
   - **Evidence**: `results/phase2_real_flink_runtime.json`, `results/phase2_flink_*.json`, `results/phase2_performance_analysis.json`, `results/phase2_final_audit.json`.

2. **Deterministic Local Reference Engine (`forgestream.streaming.job`)**:
   - **Runtime**: Native Python event-time streaming coordinator on Windows Host (Python 3.14).
   - **Role**: High-speed, local deterministic reference and testing engine implementing Flink-aligned event-time, bounded circular buffers ($N=120$), tumbling/sliding windows, and hysteresis alert state machine.
   - **Performance**: 4,567.18 events/sec, $p_{50} = 0.130\text{ms}$, $p_{95} = 0.266\text{ms}$, $p_{99} = 0.479\text{ms}$.
   - **Evidence**: `results/phase2_event_time_verification.json`, `results/phase2_performance.json`.

3. **Kafka KRaft Infrastructure Capacity**:
   - **Producer Throughput**: 32,929.35 events/sec.
   - **Consumer Throughput**: 39,061.26 events/sec.

