# ForgeStream Phase 2 Streaming & Window/State Technical Design

## 1. End-to-End Streaming Architecture

```mermaid
flowchart TD
    subgraph Data_Generation[Phase 1 Data Foundation]
        SIM[Industrial Asset Simulator<br/>5 Models, 8 Scenarios, Seed 42]
    end

    subgraph Kafka_Bus[Apache Kafka Streaming Bus]
        T_IN[Topic: industrial-telemetry<br/>Key: asset_id, 5 Partitions]
        T_ALT[Topic: asset-alerts<br/>Key: asset_id, 3 Partitions]
        T_MET[Topic: system-metrics<br/>Key: service_name, 1 Partition]
    end

    subgraph Stream_Engine[ForgeStream Streaming Engine]
        K_SRC[Kafka Consumer Source]
        WM_EXT[Timestamp Extractor &<br/>Bounded-Out-Of-Orderness Watermark]
        KEY_BY[KeyBy asset_id]
        
        subgraph Keyed_Operators[Keyed Stream Operators]
            STATE[AssetStateStore<br/>Rolling Buffers N=120<br/>Fault Counters, TTL 24h]
            FEAT[Feature Engineering<br/>Mean, Std, dT/dt, dV/dt, Ratios]
            WIN[Window Operators<br/>5s Tumbling & 30s Sliding]
            ANOM[3-Level Anomaly Detector<br/>Bounds, Z-Scores, Physics Correlations]
            HEALTH[Asset Health Index<br/>Hysteresis State Machine]
            ALERT_DEB[Alert Quality & Cooldown<br/>Duplicate Suppression]
        end
        
        MET_COL[Streaming Metrics Collector<br/>Latency p95, Lag, Throughput]
    end

    SIM -->|JSON Telemetry| T_IN
    T_IN -->|Batch Poll| K_SRC
    K_SRC --> WM_EXT
    WM_EXT --> KEY_BY
    KEY_BY --> STATE
    STATE --> FEAT
    FEAT --> WIN
    WIN --> ANOM
    ANOM --> HEALTH
    HEALTH --> ALERT_DEB
    ALERT_DEB -->|Filtered Alerts| T_ALT
    MET_COL -->|Operational Metrics| T_MET
```

---

## 2. Event-Time & Watermark Lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant Sim as Industrial Simulator
    participant Kafka as Kafka (industrial-telemetry)
    participant Engine as Stream Engine (Watermark Generator)
    participant Window as 30s Sliding Window
    participant SideOut as Late Event Side Output

    Sim->>Kafka: Event 1 (t=100s)
    Sim->>Kafka: Event 2 (t=102s)
    Kafka->>Engine: Ingest Event 1 (t=100s) -> Watermark W=95s
    Engine->>Window: Process Event 1 (On-Time: 100s >= 95s)
    Kafka->>Engine: Ingest Event 2 (t=102s) -> Watermark W=97s
    Engine->>Window: Process Event 2 (On-Time: 102s >= 97s)
    
    Sim->>Kafka: Event 3 Late Arrival (t=98s, delay=6s)
    Kafka->>Engine: Ingest Event 3 (t=98s, W=97s)
    Note over Engine: Check Allowed Lateness:<br/>t=98s >= (W - 30s = 67s) -> ACCEPTED LATE
    Engine->>Window: Update Window with Late Event 3
    
    Sim->>Kafka: Event 4 Excessively Late (t=60s, delay=45s)
    Kafka->>Engine: Ingest Event 4 (t=60s, W=97s)
    Note over Engine: Check Allowed Lateness:<br/>t=60s < (W - 30s = 67s) -> EXPIRED
    Engine->>SideOut: Route Event 4 to Late Quarantine Side Output
```

---

## 3. Asset Health State Transitions & Hysteresis

```mermaid
stateDiagram-v2
    [*] --> HEALTHY : Initial State (H=1.0)
    
    HEALTHY --> WATCH : H(t) < 0.85
    WATCH --> HEALTHY : H(t) >= 0.90 (Recovery)
    
    WATCH --> DEGRADED : H(t) < 0.65
    DEGRADED --> WATCH : H(t) >= 0.72 (Recovery)
    
    DEGRADED --> CRITICAL : H(t) < 0.40
    CRITICAL --> DEGRADED : H(t) >= 0.50 (Recovery)
    
    note right of HEALTHY
        Normal operation.
        All signals within baseline.
        Zero alerts emitted.
    end note

    note right of WATCH
        Early deviation observed.
        Warning alert emitted (cooldown 30s).
        Maintenance: Scheduled Inspection.
    end note

    note right of DEGRADED
        Persistent degradation.
        High alert emitted.
        Maintenance: Urgent Overhaul.
    end note

    note right of CRITICAL
        Active fault / catastrophic risk.
        Emergency trip alert emitted.
        Maintenance: Immediate Shutdown.
    end note
```

---

## 4. Keyed State Model & Memory Footprint

### 4.1 Memory Allocation & Buffer Layout
Each asset partition manages an isolated `AssetStateStore` holding:
- `rolling_temp_buffer`: Fixed float array of size $N=120$ (~960 bytes)
- `rolling_vib_buffer`: Fixed float array of size $N=120$ (~960 bytes)
- `rolling_pres_buffer`: Fixed float array of size $N=120$ (~960 bytes)
- `rolling_current_buffer`: Fixed float array of size $N=120$ (~960 bytes)
- `rolling_power_buffer`: Fixed float array of size $N=120$ (~960 bytes)
- `rolling_load_buffer`: Fixed float array of size $N=120$ (~960 bytes)
- `timestamps_buffer`: Fixed float array of size $N=120$ (~960 bytes)
- **Total State Size per Asset**: $< 20\text{ KB}$
- **Total In-Memory Footprint for 1,000 Assets**: $< 20\text{ MB}$

### 4.2 State Recovery & Snapshotting
- Periodic state snapshots serialize in-memory `AssetStateStore` dictionaries to disk / memory checkpoints.
- On engine restart, the latest snapshot is loaded and streaming resumes seamlessly from the committed Kafka consumer group offset.

---

## 5. Performance Measurement Methodology

Performance metrics are captured continuously at 1.0-second intervals and emitted to `system-metrics`:
- **Ingestion Throughput ($R_{\text{in}}$)**: Number of events read from Kafka per second.
- **Processing Throughput ($R_{\text{proc}}$)**: Number of events fully evaluated through feature extraction, windowing, and anomaly detection per second.
- **End-to-End Latency ($L_{\text{e2e}}$)**: Time elapsed from sensor event timestamp to alert emission ($t_{\text{sink}} - t_{\text{event}}$).
- **Processing Latency ($L_{\text{proc}}$)**: Time elapsed from Kafka poll to sink dispatch ($t_{\text{dispatch}} - t_{\text{poll}}$). Measured at 50th, 95th, and 99th percentiles.
- **Watermark Lag ($\Delta_{\text{WM}}$)**: Discrepancy between maximum observed event time and the current watermark ($t_{\text{max}} - W(t)$).
