-- =============================================================================
-- Flux OT Platform — PostgreSQL + TimescaleDB Schema
-- =============================================================================
-- Requires: PostgreSQL 15+, TimescaleDB 2.x
-- Apply with: psql -U fluxot -d fluxot -f schema.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Extensions
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ---------------------------------------------------------------------------
-- Custom types / enums
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE skid_type_enum AS ENUM (
        'BELT_RIP_MONITOR',
        'ROAD_CROSSING',
        'GENERIC'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE skid_status_enum AS ENUM (
        'ONLINE',
        'OFFLINE',
        'MAINTENANCE',
        'ALARM'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE alert_severity_enum AS ENUM (
        'INFO',
        'WARNING',
        'ALARM',
        'CRITICAL'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE command_status_enum AS ENUM (
        'PENDING',
        'SENT',
        'ACKNOWLEDGED',
        'COMPLETED',
        'FAILED',
        'TIMEOUT'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ---------------------------------------------------------------------------
-- Table: sites
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sites (
    id          UUID          PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        VARCHAR(255)  NOT NULL UNIQUE,
    location    VARCHAR(500),
    timezone    VARCHAR(64)   NOT NULL DEFAULT 'Australia/Perth',
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Keep updated_at fresh automatically
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sites_updated_at ON sites;
CREATE TRIGGER trg_sites_updated_at
    BEFORE UPDATE ON sites
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ---------------------------------------------------------------------------
-- Table: skids
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS skids (
    id                UUID             PRIMARY KEY DEFAULT uuid_generate_v4(),
    site_id           UUID             NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    skid_type         skid_type_enum   NOT NULL,
    skid_name         VARCHAR(255)     NOT NULL,
    firmware_version  VARCHAR(64),
    last_seen         TIMESTAMPTZ,
    status            skid_status_enum NOT NULL DEFAULT 'OFFLINE',
    config_json       JSONB,
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_skids_site_name UNIQUE (site_id, skid_name)
);

CREATE INDEX IF NOT EXISTS ix_skids_site_id  ON skids(site_id);
CREATE INDEX IF NOT EXISTS ix_skids_status   ON skids(status);

-- ---------------------------------------------------------------------------
-- Table: telemetry_records  (TimescaleDB hypertable)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telemetry_records (
    id           BIGSERIAL    NOT NULL,
    skid_id      UUID         NOT NULL REFERENCES skids(id) ON DELETE CASCADE,
    timestamp    TIMESTAMPTZ  NOT NULL,
    tag_name     VARCHAR(255) NOT NULL,
    value_float  DOUBLE PRECISION,
    value_bool   BOOLEAN,
    value_str    VARCHAR(1024),
    quality      VARCHAR(32)  NOT NULL DEFAULT 'GOOD',
    unit         VARCHAR(32),

    -- Composite PK required for hypertable
    PRIMARY KEY (id, timestamp)
);

-- Convert to TimescaleDB hypertable partitioned by timestamp, 1-day chunks
SELECT create_hypertable(
    'telemetry_records',
    'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS ix_telemetry_skid_ts
    ON telemetry_records(skid_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_telemetry_tag_ts
    ON telemetry_records(tag_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_telemetry_skid_tag_ts
    ON telemetry_records(skid_id, tag_name, timestamp DESC);

-- ---------------------------------------------------------------------------
-- Compression policy (compress chunks older than 7 days)
-- ---------------------------------------------------------------------------
ALTER TABLE telemetry_records SET (
    timescaledb.compress,
    timescaledb.compress_orderby    = 'timestamp DESC',
    timescaledb.compress_segmentby  = 'skid_id, tag_name'
);

SELECT add_compression_policy(
    'telemetry_records',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- ---------------------------------------------------------------------------
-- Data retention policy (drop raw data older than 90 days)
-- ---------------------------------------------------------------------------
SELECT add_retention_policy(
    'telemetry_records',
    INTERVAL '90 days',
    if_not_exists => TRUE
);

-- ---------------------------------------------------------------------------
-- Continuous aggregate: hourly rollup
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_telemetry_summary
WITH (timescaledb.continuous) AS
SELECT
    skid_id,
    tag_name,
    time_bucket('1 hour', timestamp)    AS bucket,
    AVG(value_float)                    AS avg_value,
    MIN(value_float)                    AS min_value,
    MAX(value_float)                    AS max_value,
    STDDEV(value_float)                 AS stddev_value,
    COUNT(*)                            AS sample_count,
    MODE() WITHIN GROUP (ORDER BY quality) AS dominant_quality
FROM telemetry_records
WHERE value_float IS NOT NULL
GROUP BY skid_id, tag_name, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'hourly_telemetry_summary',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE
);

-- Retain hourly rollup for 1 year
SELECT add_retention_policy(
    'hourly_telemetry_summary',
    INTERVAL '365 days',
    if_not_exists => TRUE
);

-- ---------------------------------------------------------------------------
-- Continuous aggregate: daily rollup
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_telemetry_summary
WITH (timescaledb.continuous) AS
SELECT
    skid_id,
    tag_name,
    time_bucket('1 day', timestamp)     AS bucket,
    AVG(value_float)                    AS avg_value,
    MIN(value_float)                    AS min_value,
    MAX(value_float)                    AS max_value,
    STDDEV(value_float)                 AS stddev_value,
    COUNT(*)                            AS sample_count
FROM telemetry_records
WHERE value_float IS NOT NULL
GROUP BY skid_id, tag_name, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'daily_telemetry_summary',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists     => TRUE
);
-- Daily rollup is retained forever (no retention policy applied)

-- ---------------------------------------------------------------------------
-- Table: alert_records
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_records (
    id               UUID               PRIMARY KEY DEFAULT uuid_generate_v4(),
    skid_id          UUID               NOT NULL REFERENCES skids(id) ON DELETE CASCADE,
    timestamp        TIMESTAMPTZ        NOT NULL,
    alert_id         VARCHAR(128)       NOT NULL,
    severity         alert_severity_enum NOT NULL,
    message          TEXT               NOT NULL,
    tag_name         VARCHAR(255),
    value_json       JSONB,
    acknowledged     BOOLEAN            NOT NULL DEFAULT FALSE,
    acknowledged_by  VARCHAR(255),
    acknowledged_at  TIMESTAMPTZ,
    resolved_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_alerts_skid_id       ON alert_records(skid_id);
CREATE INDEX IF NOT EXISTS ix_alerts_timestamp     ON alert_records(timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_alerts_severity      ON alert_records(severity);
CREATE INDEX IF NOT EXISTS ix_alerts_acknowledged  ON alert_records(acknowledged);
CREATE INDEX IF NOT EXISTS ix_alerts_resolved_at   ON alert_records(resolved_at)
    WHERE resolved_at IS NULL;

-- ---------------------------------------------------------------------------
-- Table: command_records
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS command_records (
    id               UUID                 PRIMARY KEY DEFAULT uuid_generate_v4(),
    skid_id          UUID                 NOT NULL REFERENCES skids(id) ON DELETE CASCADE,
    timestamp        TIMESTAMPTZ          NOT NULL,
    command_type     VARCHAR(128)         NOT NULL,
    parameters_json  JSONB,
    source           VARCHAR(255)         NOT NULL,
    status           command_status_enum  NOT NULL DEFAULT 'PENDING',
    result_json      JSONB,
    created_at       TIMESTAMPTZ          NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_commands_skid_id  ON command_records(skid_id);
CREATE INDEX IF NOT EXISTS ix_commands_status   ON command_records(status);
CREATE INDEX IF NOT EXISTS ix_commands_ts       ON command_records(timestamp DESC);

-- ---------------------------------------------------------------------------
-- Table: writeback_audit
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS writeback_audit (
    id               UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    skid_id          UUID         NOT NULL REFERENCES skids(id) ON DELETE CASCADE,
    timestamp        TIMESTAMPTZ  NOT NULL,
    command_id       UUID         REFERENCES command_records(id) ON DELETE SET NULL,
    target_protocol  VARCHAR(64)  NOT NULL,
    target_node      VARCHAR(255) NOT NULL,
    value_json       JSONB        NOT NULL,
    confirmed        BOOLEAN      NOT NULL DEFAULT FALSE,
    operator         VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS ix_writeback_skid_id   ON writeback_audit(skid_id);
CREATE INDEX IF NOT EXISTS ix_writeback_timestamp  ON writeback_audit(timestamp DESC);

-- ---------------------------------------------------------------------------
-- View: active_alerts
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW active_alerts AS
SELECT
    ar.*,
    sk.skid_name,
    sk.skid_type,
    sk.site_id,
    s.name  AS site_name
FROM alert_records ar
JOIN skids sk ON sk.id = ar.skid_id
JOIN sites s  ON s.id  = sk.site_id
WHERE ar.resolved_at IS NULL
ORDER BY
    CASE ar.severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'ALARM'    THEN 2
        WHEN 'WARNING'  THEN 3
        WHEN 'INFO'     THEN 4
    END,
    ar.timestamp DESC;

-- ---------------------------------------------------------------------------
-- View: site_health_summary
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW site_health_summary AS
SELECT
    s.id                                                  AS site_id,
    s.name                                                AS site_name,
    s.timezone,
    COUNT(sk.id)                                          AS total_skids,
    COUNT(sk.id) FILTER (WHERE sk.status = 'ONLINE')      AS skids_online,
    COUNT(sk.id) FILTER (WHERE sk.status = 'OFFLINE')     AS skids_offline,
    COUNT(sk.id) FILTER (WHERE sk.status = 'ALARM')       AS skids_in_alarm,
    COUNT(sk.id) FILTER (WHERE sk.status = 'MAINTENANCE') AS skids_in_maintenance,
    (
        SELECT COUNT(*)
        FROM alert_records ar
        WHERE ar.skid_id IN (SELECT id FROM skids WHERE site_id = s.id)
          AND ar.resolved_at IS NULL
    )                                                     AS active_alarms_total,
    (
        SELECT COUNT(*)
        FROM alert_records ar
        WHERE ar.skid_id IN (SELECT id FROM skids WHERE site_id = s.id)
          AND ar.resolved_at IS NULL
          AND ar.severity = 'CRITICAL'
    )                                                     AS critical_alarms,
    NOW()                                                 AS generated_at
FROM sites s
LEFT JOIN skids sk ON sk.site_id = s.id
GROUP BY s.id, s.name, s.timezone;

-- ---------------------------------------------------------------------------
-- Background job: auto-resolve alerts after 24 h if not acknowledged
-- Note: Requires pg_cron extension or TimescaleDB background jobs.
-- Using TimescaleDB scheduled job (add_job) for portability.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION auto_resolve_unacked_alerts()
RETURNS INTEGER AS $$
DECLARE
    resolved_count INTEGER;
BEGIN
    UPDATE alert_records
    SET
        resolved_at      = NOW(),
        acknowledged     = TRUE,
        acknowledged_by  = 'system:auto-resolve',
        acknowledged_at  = NOW()
    WHERE
        resolved_at IS NULL
        AND timestamp < NOW() - INTERVAL '24 hours';

    GET DIAGNOSTICS resolved_count = ROW_COUNT;

    IF resolved_count > 0 THEN
        RAISE NOTICE 'Auto-resolved % alert(s).', resolved_count;
    END IF;

    RETURN resolved_count;
END;
$$ LANGUAGE plpgsql;

-- Schedule the job to run every hour
SELECT add_job(
    'auto_resolve_unacked_alerts',
    '1 hour',
    if_not_exists => TRUE
);

-- ---------------------------------------------------------------------------
-- Grant privileges
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO fluxot;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fluxot;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO fluxot;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fluxot;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO fluxot;
