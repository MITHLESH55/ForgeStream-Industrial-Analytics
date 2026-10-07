# ForgeStream Phase 4: Architectural Limitations & Operational Boundaries

## 1. Executive Summary

This document details the architectural boundaries, hardware and deployment constraints, and operational assumptions inherent in the ForgeStream Phase 4 serving and analytics implementation.

---

## 2. Infrastructure & Distributed Engine Boundaries

### 2.1 Single-Node Trino Deployment
- **Constraint**: In the current development and demonstration environment, Apache Trino v438 runs as a unified single-node coordinator (`coordinator=true`, `node-scheduler.include-coordinator=true`).
- **Impact**: While query federation and ANSI-SQL execution are identical to production clusters, query parallelization is bounded by local CPU core allocation (4-8 cores) and a 1GB max query memory pool rather than distributed multi-worker horizontal scaling.
- **Production Recommendation**: Deploy Trino across dedicated coordinator and worker nodes orchestrated via Kubernetes (Trino Helm Chart), scaling workers based on CPU and memory watermark metrics.

### 2.2 PostgreSQL Serving Store Concurrency
- **Constraint**: High-frequency upserts to `asset_current_state` utilize PostgreSQL `ON CONFLICT DO UPDATE` semantics.
- **Impact**: Under extreme write concurrency (> 5,000 upserts/sec per table), row-level locks and write-ahead log (WAL) contention can introduce write latency spikes.
- **Production Recommendation**: Implement write buffering via Redis Streams or Kafka Connect JDBC Sink with micro-batching (e.g., 250ms batching windows).

### 2.3 Iceberg Commit Cadence vs Sub-Second Streaming
- **Constraint**: Apache Iceberg relies on optimistic concurrency control and atomic metadata JSON commits.
- **Impact**: Appending data every few milliseconds causes excessive small Parquet files and metadata bloat. Iceberg is architecturally optimized for 30-second to 5-minute micro-batch commits.
- **Mitigation**: ForgeStream uses the dual-write pattern where PostgreSQL absorbs sub-second operational upserts, while Iceberg receives micro-batched historical append snapshots.

---

## 3. Visualization & Transport Constraints

### 3.1 Grafana Polling Architecture
- **Constraint**: Grafana 10 dashboard panels execute scheduled polling queries (10-second default refresh) over the PostgreSQL JDBC proxy.
- **Impact**: Visual updates are discrete rather than continuous push streams. High dashboard viewer counts directly increase query load on PostgreSQL.
- **Production Recommendation**: Integrate Grafana Live (WebSockets) or Prometheus/TimescaleDB streaming ingestion for sub-second telemetry panels, reserving PostgreSQL for state tables.

### 3.2 Windows WSL2 Network Bridging
- **Constraint**: Running containerized Trino, PostgreSQL, and Grafana on Windows 11 WSL2 introduces cross-network port virtualization.
- **Mitigation**: Explicit port assignments (PostgreSQL on host 5433 -> container 5432, Trino on host 8085 -> container 8080, Grafana on host 3000 -> container 3000) prevent port conflicts with local Windows developer services.

---

## 4. Analytical Model Assumptions & Policy Boundaries

- **Static Operational Thresholds**: High-risk failure classification ($P_{fail} \ge 0.50$) and emergency thresholds ($P_{fail} \ge 0.80$ or $\text{RUL} \le 10\text{h}$) represent ForgeStream demonstration policy parameters, not universal industrial standards.
- **Prescriptive Action Text**: Recommended actions in the maintenance queue are generated via deterministic heuristic rule engines mapped to equipment classes. In live plant deployments, prescriptive recommendations integrate with enterprise CMMS / SAP PM systems with dynamic spare parts inventory verification.
