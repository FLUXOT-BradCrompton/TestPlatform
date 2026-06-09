-- =============================================================================
-- Migration 001 — Initial Schema
-- Flux OT Platform
-- =============================================================================
-- Alembic-compatible migration file.
-- Run via: alembic upgrade head
-- Or directly: psql -U fluxot -d fluxot -f 001_initial.sql
--
-- Revision: 001_initial
-- Revises:  (none — baseline)
-- Created:  2024-01-01
-- =============================================================================

-- ---------------------------------------------------------------------------
-- [BEGIN UPGRADE]
-- ---------------------------------------------------------------------------

-- Extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ---------------------------------------------------------------------------
-- Enums
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
-- Table: alembic_version (tracking)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alembic_version (
    version_num VARCHAR(32) NOT NULL,
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- ---------------------------------------------------------------------------
-- Table: sites
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sites (
    id          UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        VARCHAR(255) NOT NULL,
    location    VARCHAR(500),
    timezone    VARCHAR(64)  NOT NULL DEFAULT 'Australia/Perth',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_sites_name UNIQUE (name)
);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sites_updated_at ON sites;
CREATE TRIGGER trg_sites_updated_at
    BEFORE UPDATE ON sites
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ---------------------------------------------------------------------------
-- Table: skids
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS skids (
    id                UUID              PRIMARY KEY DEFAULT uuid_generate_v4(),
    site_id           UUID              NOT NULL,
    skid_type         skid_type_enum    NOT NULL,
    skid_name         VARCHAR(255)      NOT NULL,
    firmware_version  VARCHAR(64),
    last_seen         TIMESTAMPTZ,
    status            skid_status_enum  NOT NULL DEFAULT 'OFFLINE',
    config_json       JSONB,
    created_at        TIMESTAMPTZ       NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_skids_site
        FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE,
    CONSTRAINT uq_skids_site_name
        UNIQUE (site_id, skid_name)
);

CREATE INDEX IF NOT EXISTS ix_skids_site_id ON skids(site_id);
CREATE INDEX IF NOT EXISTS ix_skids_status  ON skids(status);

-- ---------------------------------------------------------------------------
-- Table: telemetry_records (TimescaleDB hypertable)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telemetry_records (
    id          BIGSERIAL           NOT NULL,
    skid_id     UUID                NOT NULL,
    timestamp   TIMESTAMPTZ         NOT NULL,
    tag_name    VARCHAR(255)        NOT NULL,
    value_float DOUBLE PRECISION,
    value_bool  BOOLEAN,
    value_str   VARCHAR(1024),
    quality     VARCHAR(32)         NOT NULL DEFAULT 'GOOD',
    unit        VARCHAR(32),

    PRIMARY KEY (id, timestamp),
    CONSTRAINT fk_telemetry_skid
        FOREIGN KEY (skid_id) REFERENCES skids(id) ON DELETE CASCADE
);

SELECT create_hypertable(
    'telemetry_records',
    'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS ix_telemetry_skid_ts
    ON telemetry_records(skid_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_telemetry_tag_ts
    ON telemetry_records(tag_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_telemetry_skid_tag_ts
    ON telemetry_records(skid_id, tag_name, timestamp DESC);

-- Compression
ALTER TABLE telemetry_records SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'timestamp DESC',
    timescaledb.compress_segmentby = 'skid_id, tag_name'
);

SELECT add_compression_policy(
    'telemetry_records',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'telemetry_records',
    INTERVAL '90 days',
    if_not_exists => TRUE
);

-- ---------------------------------------------------------------------------
-- Continuous aggregates
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_telemetry_summary
WITH (timescaledb.continuous) AS
SELECT
    skid_id,
    tag_name,
    time_bucket('1 hour', timestamp) AS bucket,
    AVG(value_float)                 AS avg_value,
    MIN(value_float)                 AS min_value,
    MAX(value_float)                 AS max_value,
    STDDEV(value_float)              AS stddev_value,
    COUNT(*)                         AS sample_count,
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

SELECT add_retention_policy(
    'hourly_telemetry_summary',
    INTERVAL '365 days',
    if_not_exists => TRUE
);

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_telemetry_summary
WITH (timescaledb.continuous) AS
SELECT
    skid_id,
    tag_name,
    time_bucket('1 day', timestamp) AS bucket,
    AVG(value_float)                AS avg_value,
    MIN(value_float)                AS min_value,
    MAX(value_float)                AS max_value,
    STDDEV(value_float)             AS stddev_value,
    COUNT(*)                        AS sample_count
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

-- ---------------------------------------------------------------------------
-- Table: alert_records
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_records (
    id               UUID                PRIMARY KEY DEFAULT uuid_generate_v4(),
    skid_id          UUID                NOT NULL,
    timestamp        TIMESTAMPTZ         NOT NULL,
    alert_id         VARCHAR(128)        NOT NULL,
    severity         alert_severity_enum NOT NULL,
    message          TEXT                NOT NULL,
    tag_name         VARCHAR(255),
    value_json       JSONB,
    acknowledged     BOOLEAN             NOT NULL DEFAULT FALSE,
    acknowledged_by  VARCHAR(255),
    acknowledged_at  TIMESTAMPTZ,
    resolved_at      TIMESTAMPTZ,

    CONSTRAINT fk_alerts_skid
        FOREIGN KEY (skid_id) REFERENCES skids(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_alerts_skid_id      ON alert_records(skid_id);
CREATE INDEX IF NOT EXISTS ix_alerts_timestamp    ON alert_records(timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_alerts_severity     ON alert_records(severity);
CREATE INDEX IF NOT EXISTS ix_alerts_acknowledged ON alert_records(acknowledged);
CREATE INDEX IF NOT EXISTS ix_alerts_resolved_at  ON alert_records(resolved_at)
    WHERE resolved_at IS NULL;

-- ---------------------------------------------------------------------------
-- Table: command_records
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS command_records (
    id               UUID                NOT NULL DEFAULT uuid_generate_v4(),
    skid_id          UUID                NOT NULL,
    timestamp        TIMESTAMPTZ         NOT NULL,
    command_type     VARCHAR(128)        NOT NULL,
    parameters_json  JSONB,
    source           VARCHAR(255)        NOT NULL,
    status           command_status_enum NOT NULL DEFAULT 'PENDING',
    result_json      JSONB,
    created_at       TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id),
    CONSTRAINT fk_commands_skid
        FOREIGN KEY (skid_id) REFERENCES skids(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_commands_skid_id ON command_records(skid_id);
CREATE INDEX IF NOT EXISTS ix_commands_status  ON command_records(status);
CREATE INDEX IF NOT EXISTS ix_commands_ts      ON command_records(timestamp DESC);

-- ---------------------------------------------------------------------------
-- Table: writeback_audit
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS writeback_audit (
    id               UUID         NOT NULL DEFAULT uuid_generate_v4(),
    skid_id          UUID         NOT NULL,
    timestamp        TIMESTAMPTZ  NOT NULL,
    command_id       UUID,
    target_protocol  VARCHAR(64)  NOT NULL,
    target_node      VARCHAR(255) NOT NULL,
    value_json       JSONB        NOT NULL,
    confirmed        BOOLEAN      NOT NULL DEFAULT FALSE,
    operator         VARCHAR(255),

    PRIMARY KEY (id),
    CONSTRAINT fk_writeback_skid
        FOREIGN KEY (skid_id) REFERENCES skids(id) ON DELETE CASCADE,
    CONSTRAINT fk_writeback_command
        FOREIGN KEY (command_id) REFERENCES command_records(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS ix_writeback_skid_id   ON writeback_audit(skid_id);
CREATE INDEX IF NOT EXISTS ix_writeback_timestamp ON writeback_audit(timestamp DESC);

-- ---------------------------------------------------------------------------
-- Views
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW active_alerts AS
SELECT
    ar.*,
    sk.skid_name,
    sk.skid_type,
    sk.site_id,
    s.name AS site_name
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
-- Background job: auto-resolve stale alerts
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION auto_resolve_unacked_alerts()
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    resolved_count INTEGER;
BEGIN
    UPDATE alert_records
    SET
        resolved_at     = NOW(),
        acknowledged    = TRUE,
        acknowledged_by = 'system:auto-resolve',
        acknowledged_at = NOW()
    WHERE
        resolved_at IS NULL
        AND timestamp < NOW() - INTERVAL '24 hours';

    GET DIAGNOSTICS resolved_count = ROW_COUNT;
    RETURN resolved_count;
END;
$$;

SELECT add_job(
    'auto_resolve_unacked_alerts',
    '1 hour',
    if_not_exists => TRUE
);

-- ---------------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO fluxot;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fluxot;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO fluxot;

-- ---------------------------------------------------------------------------
-- Record migration version
-- ---------------------------------------------------------------------------
INSERT INTO alembic_version (version_num)
VALUES ('001_initial')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- [END UPGRADE]
-- ---------------------------------------------------------------------------
--
-- DOWNGRADE (run in reverse to roll back):
-- DROP VIEW IF EXISTS site_health_summary;
-- DROP VIEW IF EXISTS active_alerts;
-- DROP TABLE IF EXISTS writeback_audit;
-- DROP TABLE IF EXISTS command_records;
-- DROP TABLE IF EXISTS alert_records;
-- DROP TABLE IF EXISTS telemetry_records;   -- drops hypertable + chunks
-- DROP TABLE IF EXISTS skids;
-- DROP TABLE IF EXISTS sites;
-- DROP TYPE IF EXISTS command_status_enum;
-- DROP TYPE IF EXISTS alert_severity_enum;
-- DROP TYPE IF EXISTS skid_status_enum;
-- DROP TYPE IF EXISTS skid_type_enum;
-- DROP TABLE IF EXISTS alembic_version;
-- ---------------------------------------------------------------------------
