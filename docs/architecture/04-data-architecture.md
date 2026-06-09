# Flux OT Platform — Data Architecture

**Version:** 1.0  
**Audience:** Data Engineers, Database Administrators, Analytics Teams

---

## Table of Contents

1. [Data Taxonomy](#1-data-taxonomy)
2. [TimescaleDB Schema](#2-timescaledb-schema)
3. [Kafka Topic Schema](#3-kafka-topic-schema)
4. [MQTT Message Formats](#4-mqtt-message-formats)
5. [Data Lineage](#5-data-lineage)
6. [Data Quality](#6-data-quality)
7. [Time Series Best Practices](#7-time-series-best-practices)
8. [Retention and Archival](#8-retention-and-archival)
9. [API Response Schemas](#9-api-response-schemas)
10. [Example Payloads](#10-example-payloads)

---

## 1. Data Taxonomy

Flux OT organizes all data into five distinct tiers:

| Tier | Description | Storage | Retention |
|---|---|---|---|
| **Raw Telemetry** | Individual tag readings from sensors at polling frequency (500 ms default) | `telemetry_records` hypertable | 90 days uncompressed, 2 years compressed |
| **Processed Telemetry** | Validated, quality-coded readings after edge processing | Same as raw (quality field differentiates) | Same as raw |
| **Aggregated Data** | Time-bucketed aggregates (1-min, 1-hour, 1-day) for dashboard performance | `telemetry_1min`, `telemetry_1hour` continuous aggregates | Inherits from source; 5 years for hourly |
| **Events and Alerts** | Discrete occurrences: alarms, state changes, operator actions | `alert_records`, `command_records`, `writeback_audit` | 5 years (regulatory requirement) |
| **AI Predictions** | Model inference outputs: scores, classifications, anomaly flags | `ai_predictions` | 2 years |

---

## 2. TimescaleDB Schema

### Core Tables

#### `sites`
```sql
CREATE TABLE sites (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(255) NOT NULL UNIQUE,
    location    VARCHAR(500),
    timezone    VARCHAR(64)  NOT NULL DEFAULT 'Australia/Perth',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

#### `skids`
```sql
CREATE TABLE skids (
    id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    site_id          UUID          NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    skid_type        VARCHAR(64)   NOT NULL,  -- BELT_RIP_MONITOR, ROAD_CROSSING, GENERIC
    skid_name        VARCHAR(255)  NOT NULL,
    firmware_version VARCHAR(64),
    last_seen        TIMESTAMPTZ,
    status           VARCHAR(32)   NOT NULL DEFAULT 'OFFLINE',  -- ONLINE, OFFLINE, MAINTENANCE, ALARM
    config_json      JSONB,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),

    CONSTRAINT chk_status CHECK (status IN ('ONLINE', 'OFFLINE', 'MAINTENANCE', 'ALARM'))
);
CREATE INDEX ix_skids_site_id ON skids(site_id);
CREATE INDEX ix_skids_status  ON skids(status);
```

#### `telemetry_records` (TimescaleDB Hypertable)
```sql
CREATE TABLE telemetry_records (
    id          BIGSERIAL    NOT NULL,
    skid_id     UUID         NOT NULL REFERENCES skids(id) ON DELETE CASCADE,
    timestamp   TIMESTAMPTZ  NOT NULL,
    tag_name    VARCHAR(255) NOT NULL,
    value_float DOUBLE PRECISION,
    value_bool  BOOLEAN,
    value_str   VARCHAR(1024),
    quality     VARCHAR(32)  NOT NULL DEFAULT 'GOOD',  -- GOOD, BAD, UNCERTAIN
    unit        VARCHAR(32),

    CONSTRAINT chk_quality CHECK (quality IN ('GOOD', 'BAD', 'UNCERTAIN'))
);

-- Convert to hypertable partitioned by day
SELECT create_hypertable('telemetry_records', 'timestamp',
    chunk_time_interval => INTERVAL '1 day');

-- Indexes for common query patterns
CREATE INDEX ix_telemetry_skid_ts  ON telemetry_records(skid_id, timestamp DESC);
CREATE INDEX ix_telemetry_tag_ts   ON telemetry_records(tag_name, timestamp DESC);
CREATE INDEX ix_telemetry_quality  ON telemetry_records(quality) WHERE quality != 'GOOD';

-- Compression (only float and bool columns compress well)
ALTER TABLE telemetry_records SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'skid_id,tag_name',
    timescaledb.compress_orderby   = 'timestamp DESC'
);
```

#### `alert_records`
```sql
CREATE TABLE alert_records (
    id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    skid_id          UUID          NOT NULL REFERENCES skids(id) ON DELETE CASCADE,
    timestamp        TIMESTAMPTZ   NOT NULL,
    alert_id         VARCHAR(128)  NOT NULL UNIQUE,
    severity         VARCHAR(32)   NOT NULL,  -- INFO, WARNING, ALARM, CRITICAL
    message          TEXT          NOT NULL,
    tag_name         VARCHAR(255),
    value_json       JSONB,
    acknowledged     BOOLEAN       NOT NULL DEFAULT false,
    acknowledged_by  VARCHAR(255),
    acknowledged_at  TIMESTAMPTZ,
    resolved_at      TIMESTAMPTZ
);
CREATE INDEX ix_alerts_skid_id        ON alert_records(skid_id);
CREATE INDEX ix_alerts_severity       ON alert_records(severity);
CREATE INDEX ix_alerts_acknowledged   ON alert_records(acknowledged) WHERE NOT acknowledged;
CREATE INDEX ix_alerts_resolved       ON alert_records(resolved_at) WHERE resolved_at IS NULL;
```

#### `command_records`
```sql
CREATE TABLE command_records (
    id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    skid_id          UUID          NOT NULL REFERENCES skids(id) ON DELETE CASCADE,
    timestamp        TIMESTAMPTZ   NOT NULL,
    command_type     VARCHAR(128)  NOT NULL,
    parameters_json  JSONB,
    source           VARCHAR(255)  NOT NULL,  -- username or system identifier
    status           VARCHAR(32)   NOT NULL DEFAULT 'PENDING',
    result_json      JSONB,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),

    CONSTRAINT chk_cmd_status CHECK (status IN ('PENDING','SENT','ACKNOWLEDGED','COMPLETED','FAILED','TIMEOUT'))
);
CREATE INDEX ix_commands_skid_id ON command_records(skid_id);
CREATE INDEX ix_commands_status  ON command_records(status) WHERE status NOT IN ('COMPLETED','FAILED');
```

#### `writeback_audit`
```sql
CREATE TABLE writeback_audit (
    id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    skid_id          UUID          NOT NULL REFERENCES skids(id) ON DELETE CASCADE,
    timestamp        TIMESTAMPTZ   NOT NULL,
    command_id       UUID          REFERENCES command_records(id) ON DELETE SET NULL,
    target_protocol  VARCHAR(64)   NOT NULL,  -- OPCUA, MODBUS, MQTT
    target_node      VARCHAR(255)  NOT NULL,
    value_json       JSONB         NOT NULL,
    confirmed        BOOLEAN       NOT NULL DEFAULT false,
    operator         VARCHAR(255)
);
CREATE INDEX ix_writeback_skid_id  ON writeback_audit(skid_id);
CREATE INDEX ix_writeback_ts       ON writeback_audit(timestamp DESC);
-- Note: No DELETE or UPDATE grants on this table — append-only for audit integrity
```

#### `ai_predictions`
```sql
CREATE TABLE ai_predictions (
    id              BIGSERIAL     NOT NULL,
    skid_id         UUID          NOT NULL REFERENCES skids(id) ON DELETE CASCADE,
    timestamp       TIMESTAMPTZ   NOT NULL,
    model_name      VARCHAR(128)  NOT NULL,
    model_version   VARCHAR(64)   NOT NULL,
    prediction_type VARCHAR(64)   NOT NULL,  -- RIP_SCORE, SAFETY_SCORE, ANOMALY
    score           DOUBLE PRECISION,
    label           VARCHAR(64),             -- NORMAL, SLOW_DOWN, EMERGENCY_STOP, etc.
    confidence      DOUBLE PRECISION,
    features_json   JSONB,
    acted_on        BOOLEAN       NOT NULL DEFAULT false
);
SELECT create_hypertable('ai_predictions', 'timestamp',
    chunk_time_interval => INTERVAL '1 day');
CREATE INDEX ix_ai_pred_skid_ts ON ai_predictions(skid_id, timestamp DESC);
```

### Continuous Aggregates

```sql
-- 1-minute aggregates (for Grafana time-series panels, last 24h)
CREATE MATERIALIZED VIEW telemetry_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp)                                    AS bucket,
    skid_id,
    tag_name,
    AVG(value_float)                                                       AS avg_value,
    MIN(value_float)                                                       AS min_value,
    MAX(value_float)                                                       AS max_value,
    COUNT(*)                                                               AS sample_count,
    COUNT(*) FILTER (WHERE quality = 'GOOD')                               AS good_count,
    COUNT(*) FILTER (WHERE quality = 'BAD')                                AS bad_count
FROM telemetry_records
WHERE value_float IS NOT NULL
GROUP BY 1, 2, 3;

-- 1-hour aggregates (for trend analysis, last 30 days)
CREATE MATERIALIZED VIEW telemetry_1hour
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp)                                       AS bucket,
    skid_id,
    tag_name,
    AVG(value_float)                                                       AS avg_value,
    MIN(value_float)                                                       AS min_value,
    MAX(value_float)                                                       AS max_value,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY value_float)              AS p50_value,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY value_float)              AS p95_value,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY value_float)              AS p99_value,
    COUNT(*)                                                               AS sample_count
FROM telemetry_records
WHERE value_float IS NOT NULL
GROUP BY 1, 2, 3;
```

---

## 3. Kafka Topic Schema

All Kafka messages are serialized as **UTF-8 JSON**. No schema registry is used in the current release; schema evolution is managed via backward-compatible additions (new optional fields).

### `fluxot.telemetry.belt_rip`

```json
{
  "timestamp": "2024-11-15T03:42:00.523Z",
  "site_id": "SITE_KALGOORLIE_01",
  "skid_id": "SKID_CV001",
  "skid_type": "BELT_RIP",
  "sequence_number": 18472,
  "data": {
    "Sensor.Mag1":          {"value": 51.3, "unit": "mT", "quality": "GOOD"},
    "Sensor.Mag2":          {"value": 50.8, "unit": "mT", "quality": "GOOD"},
    "Sensor.Mag3":          {"value": 52.1, "unit": "mT", "quality": "GOOD"},
    "Sensor.Mag4":          {"value": 49.7, "unit": "mT", "quality": "GOOD"},
    "Sensor.Mag5":          {"value": 51.0, "unit": "mT", "quality": "GOOD"},
    "Sensor.Mag6":          {"value": 50.4, "unit": "mT", "quality": "GOOD"},
    "Sensor.Mag7":          {"value": 52.6, "unit": "mT", "quality": "GOOD"},
    "Sensor.Mag8":          {"value": 48.2, "unit": "mT", "quality": "UNCERTAIN"},
    "Sensor.Acoustic1":     {"value": 61.5, "unit": "dB", "quality": "GOOD"},
    "Sensor.Acoustic2":     {"value": 60.8, "unit": "dB", "quality": "GOOD"},
    "Sensor.Acoustic3":     {"value": 62.2, "unit": "dB", "quality": "GOOD"},
    "Sensor.Acoustic4":     {"value": 59.1, "unit": "dB", "quality": "GOOD"},
    "Sensor.LoadCell1":     {"value": 20.3, "unit": "kN", "quality": "GOOD"},
    "Sensor.LoadCell2":     {"value": 19.8, "unit": "kN", "quality": "GOOD"},
    "Sensor.BeltSpeed":     {"value": 3.47, "unit": "m/s", "quality": "GOOD"},
    "Sensor.BeltTension":   {"value": 148.2, "unit": "kN", "quality": "GOOD"},
    "Sensor.MotorCurrent":  {"value": 84.5, "unit": "A", "quality": "GOOD"},
    "Sensor.BeltTemperature": {"value": 44.1, "unit": "°C", "quality": "GOOD"},
    "BeltStatus.Running":   {"value": true, "unit": "", "quality": "GOOD"}
  }
}
```

**Kafka message key:** `{skid_id}` (ensures all messages for a skid go to the same partition for ordering)

### `fluxot.alerts`

```json
{
  "timestamp": "2024-11-15T03:43:15.001Z",
  "site_id": "SITE_KALGOORLIE_01",
  "skid_id": "SKID_CV001",
  "skid_type": "BELT_RIP",
  "alert_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "severity": "ALARM",
  "message": "Rip detection score 0.82 exceeds SLOW_DOWN threshold (0.75). Mag sensors: cv=0.18",
  "tag_name": "RipDetectionScore",
  "value": 0.82,
  "threshold": 0.75,
  "acknowledged": false
}
```

### `fluxot.commands`

```json
{
  "timestamp": "2024-11-15T03:43:20.000Z",
  "site_id": "SITE_KALGOORLIE_01",
  "skid_id": "SKID_CV001",
  "command_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "command_type": "EMERGENCY_STOP",
  "parameters": {},
  "source": "operator_jane",
  "requires_ack": true,
  "expires_at": "2024-11-15T03:43:50.000Z"
}
```

### `fluxot.predictions.belt_rip`

```json
{
  "timestamp": "2024-11-15T03:43:25.000Z",
  "skid_id": "SKID_CV001",
  "model_name": "belt-rip-lstm-v2",
  "model_version": "7",
  "prediction_type": "RIP_SCORE",
  "score": 0.79,
  "label": "SLOW_DOWN",
  "confidence": 0.88,
  "inference_latency_ms": 23,
  "features": {
    "mag_cv_5min": 0.14,
    "acoustic_max_5min": 68.3,
    "load_diff_5min": 1.2,
    "speed_stability": 0.99
  }
}
```

---

## 4. MQTT Message Formats

### Telemetry Message

**Topic:** `fluxot/{site_id}/BELT_RIP/{skid_id}/telemetry`

```json
{
  "timestamp": "2024-11-15T03:42:00.523Z",
  "site_id": "SITE_KALGOORLIE_01",
  "skid_id": "SKID_CV001",
  "skid_type": "BELT_RIP",
  "sequence_number": 18472,
  "data": {
    "Sensor.Mag1": {
      "timestamp": "2024-11-15T03:42:00.523Z",
      "site_id": "SITE_KALGOORLIE_01",
      "skid_id": "SKID_CV001",
      "skid_type": "BELT_RIP",
      "tag_name": "Sensor.Mag1",
      "value": 51.3,
      "unit": "mT",
      "quality": "GOOD"
    }
  }
}
```

### Alert Message

**Topic:** `fluxot/{site_id}/BELT_RIP/{skid_id}/alerts`

```json
{
  "timestamp": "2024-11-15T03:43:15.001Z",
  "site_id": "SITE_KALGOORLIE_01",
  "skid_id": "SKID_CV001",
  "skid_type": "BELT_RIP",
  "alert_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "severity": "ALARM",
  "message": "Rip detection score 0.82 exceeds SLOW_DOWN threshold (0.75)",
  "tag_name": "RipDetectionScore",
  "value": 0.82,
  "threshold": 0.75,
  "acknowledged": false
}
```

### Command Message

**Topic:** `fluxot/{site_id}/BELT_RIP/{skid_id}/commands`

```json
{
  "timestamp": "2024-11-15T03:43:20.000Z",
  "site_id": "SITE_KALGOORLIE_01",
  "skid_id": "SKID_CV001",
  "command_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "command_type": "EMERGENCY_STOP",
  "parameters": {},
  "source": "operator_jane",
  "requires_ack": true
}
```

Valid `command_type` values for Belt Rip skid:

| Command Type | Parameters | Effect |
|---|---|---|
| `EMERGENCY_STOP` | `{}` | Write `TRUE` to `ns=2;s=BeltControl.EmergencyStop` |
| `SET_SPEED_SETPOINT` | `{"speed_ms": 1.75}` | Write speed to `ns=2;s=BeltControl.SpeedSetpoint` |
| `SET_MAINTENANCE_MODE` | `{"enabled": true}` | Enable/disable maintenance mode flag |
| `RESET_EMERGENCY_STOP` | `{}` | Reset E-stop latch (requires maintenance mode) |
| `REQUEST_BASELINE` | `{}` | Trigger sensor re-baselining cycle |

### Heartbeat Message

**Topic:** `fluxot/{site_id}/BELT_RIP/{skid_id}/heartbeat`

```json
{
  "timestamp": "2024-11-15T03:42:30.000Z",
  "site_id": "SITE_KALGOORLIE_01",
  "skid_id": "SKID_CV001",
  "skid_type": "BELT_RIP",
  "status": "RUNNING",
  "uptime_s": 86400,
  "connection_states": {
    "mqtt":   "CONNECTED",
    "opcua":  "CONNECTED",
    "modbus": "DISCONNECTED",
    "ai_service": "CONNECTED"
  }
}
```

### Writeback Audit Message

**Topic:** `fluxot/{site_id}/BELT_RIP/{skid_id}/writeback`

```json
{
  "timestamp": "2024-11-15T03:43:20.055Z",
  "command_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "target_protocol": "OPCUA",
  "target_node": "ns=2;s=BeltControl.EmergencyStop",
  "value": true,
  "confirmed": true
}
```

### Command Acknowledgement

**Topic:** `fluxot/{site_id}/BELT_RIP/{skid_id}/cmd-ack`

```json
{
  "timestamp": "2024-11-15T03:43:20.080Z",
  "command_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "COMPLETED",
  "message": "OPC-UA write confirmed: ns=2;s=BeltControl.EmergencyStop = True"
}
```

---

## 5. Data Lineage

```
LEVEL 0: Physical sensor (e.g., magnetic sensor)
   │  Signal: 4-20 mA analog
   ▼
LEVEL 1: PLC (Siemens S7-1500)
   │  Transformation: ADC conversion → engineering unit scaling
   │  Value: raw 16-bit integer → 51.3 mT
   │  Protocol: OPC-UA data value (variant type Float)
   ▼
LEVEL 2: Edge service (BeltSensorReader.read_all())
   │  Transformation: OPC-UA variant → Python float
   │  Quality: OPC-UA StatusCode → GOOD/BAD/UNCERTAIN
   │  Model: BeltSensorData dataclass
   ▼
Edge service (anomaly detection)
   │  Transformation: Raw values → detection score
   │  Model: TelemetryBatch (Pydantic)
   │  MQTT publish: QoS 1
   ▼
DMZ Mosquitto MQTT Broker
   │  Pass-through (no transformation)
   ▼
Platform AMQ Broker (Artemis)
   │  Pass-through
   ▼
MQTT-Kafka Bridge service
   │  Transformation: MQTT payload → Kafka record (add partition key = skid_id)
   │  Topic: fluxot.telemetry.belt_rip
   ▼
Kafka Consumer (Platform API /ingest)
   │  Transformation: JSON → TelemetryRecord ORM model
   │  Validation: type checking, range validation
   ▼
TimescaleDB telemetry_records (raw storage)
   │  No transformation; verbatim insert
   ▼
TimescaleDB continuous aggregate (telemetry_1min)
   │  Transformation: AVG/MIN/MAX per minute
   ▼
Platform API GET /telemetry/history or /latest
   │  Transformation: ORM model → Pydantic response schema
   │  Serialization: JSON
   ▼
Redis pub/sub (WebSocket fan-out)
   │  Pass-through
   ▼
Grafana Dashboard
   │  Transformation: SQL query → time-series panel
   ▼
Human operator (display)
```

---

## 6. Data Quality

### Quality Codes

All telemetry records carry one of three quality values:

| Quality | Meaning | Database Behaviour | Dashboard Display |
|---|---|---|---|
| `GOOD` | Sensor reading is valid and within calibrated range | Normal storage and aggregation | Normal |
| `UNCERTAIN` | Reading may be valid but confidence is reduced (e.g., sensor communication CRC error, value at range boundary) | Stored; excluded from statistical aggregates | Yellow/amber indicator |
| `BAD` | Reading is invalid (sensor fault, communication failure, out-of-range, ADC error) | Stored but flagged; excluded from aggregates and AI features | Red indicator; dashboard shows last good value with staleness timer |

### Handling Bad Data

**At the edge service:**
1. OPC-UA `StatusCode.is_good()` is checked for every node read
2. If `False`, `quality = "BAD"` is set on the TelemetryPoint
3. Detection algorithms skip `BAD` quality readings (substitute last-known-good value with decay)
4. 3 consecutive `BAD` readings on the same tag → raise `WARNING` alert: "Sensor {tag} quality degraded"

**In TimescaleDB aggregates:**
```sql
-- Only GOOD quality readings included in aggregates
SELECT avg(value_float) FROM telemetry_records
WHERE quality = 'GOOD' AND tag_name = 'Sensor.Mag1'
  AND timestamp > now() - INTERVAL '1 hour';
```

**On dashboards:**
- Panels show `N/A` for `BAD` quality current values
- Time-series panels interpolate across `BAD` gaps (configurable per panel)
- Sensor health table explicitly shows quality status per tag

---

## 7. Time Series Best Practices

### All Timestamps in UTC

Every timestamp throughout the Flux OT system is stored and transmitted in **UTC (Coordinated Universal Time)**:

- Edge services use `datetime.now(tz=timezone.utc)` exclusively
- TimescaleDB columns are `TIMESTAMPTZ` (timezone-aware)
- Kafka messages use ISO-8601 format with `Z` suffix: `"2024-11-15T03:42:00.523Z"`
- Grafana performs client-side timezone conversion for display

### Australian Mining Timezone Context

Australian mine sites operate in:
- **Western Australia (Pilbara, Goldfields):** `Australia/Perth` (AWST, UTC+8, no DST)
- **Northern Territory (Darwin region):** `Australia/Darwin` (ACST, UTC+9:30, no DST)
- **Queensland:** `Australia/Brisbane` (AEST, UTC+10, no DST)
- **New South Wales / Victoria:** `Australia/Sydney` (AEDT/AEST, UTC+11/+10 with DST)

The `sites` table stores the `timezone` field. Grafana panels display times in the site timezone. All alerting threshold comparisons use UTC.

### Clock Synchronisation

Edge IPC clocks must be synchronised to site GPS/GNSS time or a reliable NTP server:

```ini
# /etc/chrony.conf on edge IPC
server 192.168.1.1 iburst prefer   # Site GPS NTP server
pool au.pool.ntp.org iburst         # Fallback
makestep 1.0 3                      # Correct clock step if offset > 1s on startup
maxdistance 1.0                     # Maximum clock dispersion (1 second)
```

Maximum acceptable clock drift for telemetry: **500 ms** (one poll interval).

### Sequence Numbers

Each `TelemetryBatch` includes a `sequence_number` (monotonically increasing per skid). This allows:
- Detection of dropped messages in transit
- Ordering of out-of-order messages received via buffer flush
- Gap analysis in the telemetry history

---

## 8. Retention and Archival

### Tiered Storage Strategy

```
HOT TIER (0–90 days): TimescaleDB primary storage
  - Full resolution: every tag, every 500 ms
  - Uncompressed: days 0–7 (fastest queries)
  - Compressed: days 7–90 (70% size reduction via LZ4)
  - Query time: < 100 ms for last-hour data

WARM TIER (90 days – 2 years): TimescaleDB compressed + continuous aggregates
  - Full resolution data compressed and retained for audit/forensics
  - 1-minute aggregates retained for trend analysis
  - 1-hour aggregates retained for long-term dashboards
  - Query time: < 500 ms for multi-month trend

COLD TIER (2+ years): S3/Azure Blob Object Storage
  - TimescaleDB pg_dump exports compressed with zstd
  - Stored in site/year/month/ prefix structure
  - Retrieval: manual restore or Athena/Synapse query for ad-hoc
  - Retention: 7 years (regulatory minimum for safety-critical events)
  - Cost: ~AUD $0.025/GB/month on S3 Standard-IA
```

### Retention Policies

| Data Type | Retention Period | Regulation |
|---|---|---|
| Raw telemetry (GOOD quality) | 2 years in DB, 7 years in cold storage | Best practice |
| Raw telemetry (BAD/UNCERTAIN) | 90 days | Can be discarded after investigation window |
| Alert records | 5 years | Mines Safety and Inspection Act 1994 (WA) |
| Writeback audit records | 7 years | Safety-critical control action audit trail |
| Command records | 5 years | Operator action audit trail |
| AI predictions | 2 years | Model performance evaluation |
| Aggregated telemetry (1-min) | 5 years | Dashboard history |
| Aggregated telemetry (1-hour) | 10 years | Long-term trend analysis |

---

## 9. API Response Schemas

### GET /api/v1/telemetry/sites/{site_id}/skids/{skid_id}/latest

```json
[
  {
    "id": 1847293,
    "skid_id": "550e8400-e29b-41d4-a716-446655440001",
    "timestamp": "2024-11-15T03:42:00.523Z",
    "tag_name": "Sensor.BeltSpeed",
    "value_float": 3.47,
    "value_bool": null,
    "value_str": null,
    "quality": "GOOD",
    "unit": "m/s"
  }
]
```

### GET /api/v1/telemetry/sites/{site_id}/dashboard

```json
{
  "site_id": "550e8400-e29b-41d4-a716-446655440000",
  "site_name": "Kalgoorlie Gold Mine - Site 01",
  "total_skids": 5,
  "skids_online": 4,
  "skids_offline": 0,
  "skids_in_alarm": 1,
  "skids_in_maintenance": 0,
  "active_alarms_total": 3,
  "critical_alarms": 1,
  "warning_alarms": 2,
  "skid_summaries": [
    {
      "skid_id": "550e8400-e29b-41d4-a716-446655440001",
      "skid_name": "CV-001 Head Conveyor",
      "skid_type": "BELT_RIP_MONITOR",
      "status": "ALARM",
      "last_seen": "2024-11-15T03:42:00.523Z",
      "active_alarm_count": 2
    }
  ],
  "generated_at": "2024-11-15T03:42:05.001Z"
}
```

---

## 10. Example Payloads

### Belt Rip Telemetry — Normal Operation

```json
{
  "timestamp": "2024-11-15T03:42:00.523Z",
  "site_id": "SITE_KALGOORLIE_01",
  "skid_id": "SKID_CV001",
  "skid_type": "BELT_RIP",
  "sequence_number": 18472,
  "data": {
    "Sensor.Mag1":            {"value": 51.3, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag2":            {"value": 50.8, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag3":            {"value": 52.1, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag4":            {"value": 49.7, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag5":            {"value": 51.0, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag6":            {"value": 50.4, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag7":            {"value": 52.6, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag8":            {"value": 51.1, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Acoustic1":       {"value": 61.5, "unit": "dB",  "quality": "GOOD"},
    "Sensor.Acoustic2":       {"value": 60.8, "unit": "dB",  "quality": "GOOD"},
    "Sensor.Acoustic3":       {"value": 62.2, "unit": "dB",  "quality": "GOOD"},
    "Sensor.Acoustic4":       {"value": 59.1, "unit": "dB",  "quality": "GOOD"},
    "Sensor.LoadCell1":       {"value": 20.3, "unit": "kN",  "quality": "GOOD"},
    "Sensor.LoadCell2":       {"value": 19.8, "unit": "kN",  "quality": "GOOD"},
    "Sensor.BeltSpeed":       {"value": 3.47, "unit": "m/s", "quality": "GOOD"},
    "Sensor.BeltTension":     {"value": 148.2,"unit": "kN",  "quality": "GOOD"},
    "Sensor.MotorCurrent":    {"value": 84.5, "unit": "A",   "quality": "GOOD"},
    "Sensor.BeltTemperature": {"value": 44.1, "unit": "°C",  "quality": "GOOD"},
    "BeltStatus.Running":     {"value": true, "unit": "",    "quality": "GOOD"}
  }
}
```

### Belt Rip Telemetry — Rip Event (Anomaly Detected)

```json
{
  "timestamp": "2024-11-15T04:15:33.001Z",
  "site_id": "SITE_KALGOORLIE_01",
  "skid_id": "SKID_CV001",
  "skid_type": "BELT_RIP",
  "sequence_number": 19101,
  "data": {
    "Sensor.Mag1":            {"value": 51.2, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag2":            {"value": 50.9, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag3":            {"value": 71.8, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag4":            {"value": 28.3, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag5":            {"value": 73.2, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag6":            {"value": 26.1, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag7":            {"value": 52.0, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Mag8":            {"value": 51.4, "unit": "mT",  "quality": "GOOD"},
    "Sensor.Acoustic1":       {"value": 82.3, "unit": "dB",  "quality": "GOOD"},
    "Sensor.Acoustic2":       {"value": 79.5, "unit": "dB",  "quality": "GOOD"},
    "Sensor.Acoustic3":       {"value": 83.8, "unit": "dB",  "quality": "GOOD"},
    "Sensor.Acoustic4":       {"value": 61.0, "unit": "dB",  "quality": "GOOD"},
    "Sensor.LoadCell1":       {"value": 27.1, "unit": "kN",  "quality": "GOOD"},
    "Sensor.LoadCell2":       {"value": 14.2, "unit": "kN",  "quality": "GOOD"},
    "Sensor.BeltSpeed":       {"value": 3.51, "unit": "m/s", "quality": "GOOD"},
    "Sensor.BeltTension":     {"value": 162.7,"unit": "kN",  "quality": "GOOD"},
    "Sensor.MotorCurrent":    {"value": 101.3,"unit": "A",   "quality": "GOOD"},
    "Sensor.BeltTemperature": {"value": 52.8, "unit": "°C",  "quality": "GOOD"},
    "BeltStatus.Running":     {"value": true, "unit": "",    "quality": "GOOD"},
    "RipDetectionScore":      {"value": 0.91, "unit": "",    "quality": "GOOD"}
  }
}
```

### Road Crossing Telemetry — Vehicle Crossing in Progress

```json
{
  "timestamp": "2024-11-15T06:22:11.500Z",
  "site_id": "SITE_KALGOORLIE_01",
  "skid_id": "SKID_RC001",
  "skid_type": "ROAD_CROSSING",
  "sequence_number": 44321,
  "data": {
    "Sensor.LoopDetector1":   {"value": true,  "unit": "",      "quality": "GOOD"},
    "Sensor.LoopDetector2":   {"value": true,  "unit": "",      "quality": "GOOD"},
    "Sensor.LoopDetector3":   {"value": false, "unit": "",      "quality": "GOOD"},
    "Sensor.Radar1Speed":     {"value": 12.5,  "unit": "km/h",  "quality": "GOOD"},
    "Sensor.Radar1Class":     {"value": 3.0,   "unit": "",      "quality": "GOOD"},
    "Sensor.Radar2Present":   {"value": true,  "unit": "",      "quality": "GOOD"},
    "Crossing.Barrier1Open":  {"value": true,  "unit": "",      "quality": "GOOD"},
    "Crossing.Barrier2Open":  {"value": true,  "unit": "",      "quality": "GOOD"},
    "Crossing.TrafficLight":  {"value": "GREEN","unit": "",     "quality": "GOOD"},
    "Crossing.State":         {"value": "CROSSING_IN_PROGRESS", "unit": "", "quality": "GOOD"},
    "Crossing.QueueCount":    {"value": 2.0,   "unit": "vehicles", "quality": "GOOD"},
    "Crossing.SafetyScore":   {"value": 0.97,  "unit": "",      "quality": "GOOD"},
    "BeltStatus.Running":     {"value": false, "unit": "",      "quality": "GOOD"}
  }
}
```
