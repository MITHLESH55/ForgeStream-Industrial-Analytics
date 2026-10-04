-- ============================================================================
-- ForgeStream Operational Metadata Schema DDL (Phase 1)
-- ============================================================================

-- 1. Industrial Asset Registry
CREATE TABLE IF NOT EXISTS assets (
    asset_id VARCHAR(64) PRIMARY KEY,
    asset_type VARCHAR(32) NOT NULL,
    model_name VARCHAR(128) NOT NULL,
    rated_rpm DOUBLE PRECISION NOT NULL,
    rated_load DOUBLE PRECISION NOT NULL,
    rated_voltage DOUBLE PRECISION NOT NULL,
    rated_current DOUBLE PRECISION NOT NULL,
    baseline_temperature DOUBLE PRECISION NOT NULL,
    baseline_vibration DOUBLE PRECISION NOT NULL,
    baseline_pressure DOUBLE PRECISION NOT NULL,
    criticality VARCHAR(16) NOT NULL DEFAULT 'MEDIUM',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Maintenance Work Order & Technician History
CREATE TABLE IF NOT EXISTS maintenance_history (
    maintenance_id VARCHAR(64) PRIMARY KEY,
    asset_id VARCHAR(64) NOT NULL REFERENCES assets(asset_id) ON DELETE CASCADE,
    maintenance_type VARCHAR(32) NOT NULL,
    scenario_id VARCHAR(64),
    description TEXT,
    technician_id VARCHAR(64) NOT NULL,
    performed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    cost_usd DOUBLE PRECISION DEFAULT 0.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Telemetry Ingestion Runs & Batches
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    seed BIGINT NOT NULL,
    asset_count INT NOT NULL,
    duration_sec INT NOT NULL,
    events_generated BIGINT NOT NULL DEFAULT 0,
    events_persisted_iceberg BIGINT NOT NULL DEFAULT 0,
    events_quarantined BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'RUNNING',
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE
);

-- 4. Data Quality Aggregated Audit Events
CREATE TABLE IF NOT EXISTS data_quality_events (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(64) REFERENCES ingestion_runs(run_id) ON DELETE CASCADE,
    event_id VARCHAR(64),
    asset_id VARCHAR(64),
    rule_code VARCHAR(64) NOT NULL,
    severity VARCHAR(16) NOT NULL DEFAULT 'ERROR',
    details TEXT,
    evaluated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. Defective Records Quarantine Store
CREATE TABLE IF NOT EXISTS quarantine_events (
    quarantine_id VARCHAR(64) PRIMARY KEY,
    run_id VARCHAR(64) REFERENCES ingestion_runs(run_id) ON DELETE SET NULL,
    event_id VARCHAR(64),
    asset_id VARCHAR(64),
    violated_rules TEXT NOT NULL,
    reasons TEXT NOT NULL,
    raw_payload TEXT NOT NULL,
    quarantined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 6. Reference Experiment Runs & Benchmark Records
CREATE TABLE IF NOT EXISTS experiment_runs (
    experiment_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    scenario_id VARCHAR(64) NOT NULL,
    seed BIGINT NOT NULL,
    metrics_summary TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for high-frequency queries
CREATE INDEX IF NOT EXISTS idx_assets_type ON assets(asset_type);
CREATE INDEX IF NOT EXISTS idx_maint_asset ON maintenance_history(asset_id);
CREATE INDEX IF NOT EXISTS idx_dq_run ON data_quality_events(run_id);
CREATE INDEX IF NOT EXISTS idx_quarantine_asset ON quarantine_events(asset_id);
CREATE INDEX IF NOT EXISTS idx_quarantine_run ON quarantine_events(run_id);
