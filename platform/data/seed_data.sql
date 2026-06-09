-- =============================================================================
-- Flux OT Platform — Development Seed Data
-- =============================================================================
-- Usage: psql -U fluxot -d fluxot -f seed_data.sql
-- Prerequisites: 001_initial.sql must have been applied first.
-- =============================================================================

-- Use fixed UUIDs so seeds are idempotent and referencing is easy
-- across development environments.

-- ---------------------------------------------------------------------------
-- Sites
-- ---------------------------------------------------------------------------
INSERT INTO sites (id, name, location, timezone, created_at, updated_at)
VALUES
    (
        'a1000000-0000-0000-0000-000000000001',
        'Mt Newman Mine',
        'Newman, Western Australia, Australia (-23.3646, 119.7278)',
        'Australia/Perth',
        NOW(),
        NOW()
    ),
    (
        'a1000000-0000-0000-0000-000000000002',
        'Pilbara Operations',
        'Port Hedland, Western Australia, Australia (-20.3174, 118.6056)',
        'Australia/Perth',
        NOW(),
        NOW()
    )
ON CONFLICT (name) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Skids — 2 belt rip monitors, 2 road crossings
-- ---------------------------------------------------------------------------
INSERT INTO skids (id, site_id, skid_type, skid_name, firmware_version, status, config_json, created_at)
VALUES
    -- Mt Newman Mine — Belt Rip Monitor #1
    (
        'b2000000-0000-0000-0000-000000000001',
        'a1000000-0000-0000-0000-000000000001',
        'BELT_RIP_MONITOR',
        'BRM-NWM-001',
        '2.4.1',
        'ONLINE',
        '{
            "belt_width_mm": 1800,
            "belt_speed_nominal_ms": 4.5,
            "sensor_positions": [0, 500, 1000, 1500, 2000, 2500],
            "anomaly_threshold": 0.75,
            "magnetic_baseline_mt": 12.5,
            "scan_interval_ms": 100,
            "modbus_slave_id": 1,
            "opcua_namespace": "urn:fluxot:brm:nwm001"
        }',
        NOW() - INTERVAL '30 days'
    ),
    -- Mt Newman Mine — Road Crossing #1
    (
        'b2000000-0000-0000-0000-000000000002',
        'a1000000-0000-0000-0000-000000000001',
        'ROAD_CROSSING',
        'RXS-NWM-001',
        '1.8.3',
        'ONLINE',
        '{
            "barrier_type": "hydraulic",
            "max_barrier_travel_time_s": 12,
            "vehicle_detection_range_m": 50,
            "traffic_light_count": 4,
            "emergency_stop_inputs": ["ES-01", "ES-02"],
            "modbus_slave_id": 2,
            "opcua_namespace": "urn:fluxot:rxs:nwm001",
            "auto_close_delay_s": 30
        }',
        NOW() - INTERVAL '45 days'
    ),
    -- Pilbara Operations — Belt Rip Monitor #2
    (
        'b2000000-0000-0000-0000-000000000003',
        'a1000000-0000-0000-0000-000000000002',
        'BELT_RIP_MONITOR',
        'BRM-PLB-001',
        '2.4.1',
        'ALARM',
        '{
            "belt_width_mm": 2100,
            "belt_speed_nominal_ms": 5.2,
            "sensor_positions": [0, 600, 1200, 1800, 2400, 3000],
            "anomaly_threshold": 0.70,
            "magnetic_baseline_mt": 14.2,
            "scan_interval_ms": 100,
            "modbus_slave_id": 1,
            "opcua_namespace": "urn:fluxot:brm:plb001"
        }',
        NOW() - INTERVAL '60 days'
    ),
    -- Pilbara Operations — Road Crossing #2
    (
        'b2000000-0000-0000-0000-000000000004',
        'a1000000-0000-0000-0000-000000000002',
        'ROAD_CROSSING',
        'RXS-PLB-001',
        '1.8.3',
        'MAINTENANCE',
        '{
            "barrier_type": "pneumatic",
            "max_barrier_travel_time_s": 8,
            "vehicle_detection_range_m": 40,
            "traffic_light_count": 2,
            "emergency_stop_inputs": ["ES-01"],
            "modbus_slave_id": 2,
            "opcua_namespace": "urn:fluxot:rxs:plb001",
            "auto_close_delay_s": 20
        }',
        NOW() - INTERVAL '90 days'
    )
ON CONFLICT (site_id, skid_name) DO NOTHING;

-- Update last_seen for online skids
UPDATE skids SET last_seen = NOW() - INTERVAL '30 seconds'
WHERE id IN (
    'b2000000-0000-0000-0000-000000000001',
    'b2000000-0000-0000-0000-000000000002'
);

UPDATE skids SET last_seen = NOW() - INTERVAL '2 minutes'
WHERE id = 'b2000000-0000-0000-0000-000000000003';

UPDATE skids SET last_seen = NOW() - INTERVAL '4 hours'
WHERE id = 'b2000000-0000-0000-0000-000000000004';

-- ---------------------------------------------------------------------------
-- Sample telemetry — BRM-NWM-001 (realistic belt rip monitor values)
-- ---------------------------------------------------------------------------
INSERT INTO telemetry_records (skid_id, timestamp, tag_name, value_float, quality, unit)
VALUES
    -- Current belt speed
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '10 seconds', 'belt_speed',            4.47,  'GOOD', 'm/s'),
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '20 seconds', 'belt_speed',            4.49,  'GOOD', 'm/s'),
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '30 seconds', 'belt_speed',            4.51,  'GOOD', 'm/s'),
    -- Motor current
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '10 seconds', 'motor_current_a',       142.3, 'GOOD', 'A'),
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '20 seconds', 'motor_current_a',       141.8, 'GOOD', 'A'),
    -- Belt tension
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '10 seconds', 'belt_tension_kn',       87.4,  'GOOD', 'kN'),
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '20 seconds', 'belt_tension_kn',       86.9,  'GOOD', 'kN'),
    -- Magnetic field sensors (peak reading per scan)
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '10 seconds', 'mag_sensor_0_mt',       12.6,  'GOOD', 'mT'),
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '10 seconds', 'mag_sensor_1_mt',       12.4,  'GOOD', 'mT'),
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '10 seconds', 'mag_sensor_2_mt',       12.5,  'GOOD', 'mT'),
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '10 seconds', 'mag_sensor_3_mt',       12.3,  'GOOD', 'mT'),
    -- Composite anomaly score (0.0 = no anomaly, 1.0 = certain rip)
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '10 seconds', 'composite_score',       0.12,  'GOOD', NULL),
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '20 seconds', 'composite_score',       0.10,  'GOOD', NULL),
    -- Temperature
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '10 seconds', 'ambient_temp_c',        38.2,  'GOOD', 'degC'),
    -- Power consumption
    ('b2000000-0000-0000-0000-000000000001', NOW() - INTERVAL '10 seconds', 'power_kw',              74.6,  'GOOD', 'kW')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Sample telemetry — RXS-NWM-001 (realistic road crossing values)
-- ---------------------------------------------------------------------------
INSERT INTO telemetry_records (skid_id, timestamp, tag_name, value_float, value_bool, quality, unit)
VALUES
    -- Barrier position (0.0 = fully closed, 1.0 = fully open)
    ('b2000000-0000-0000-0000-000000000002', NOW() - INTERVAL '10 seconds', 'barrier_position',  0.0,   NULL,  'GOOD', NULL),
    -- Vehicle detection
    ('b2000000-0000-0000-0000-000000000002', NOW() - INTERVAL '10 seconds', 'vehicle_detected',  NULL,  FALSE, 'GOOD', NULL),
    -- Loop detector occupancy
    ('b2000000-0000-0000-0000-000000000002', NOW() - INTERVAL '10 seconds', 'loop_detector_1',   NULL,  FALSE, 'GOOD', NULL),
    ('b2000000-0000-0000-0000-000000000002', NOW() - INTERVAL '10 seconds', 'loop_detector_2',   NULL,  FALSE, 'GOOD', NULL),
    -- Hydraulic pressure
    ('b2000000-0000-0000-0000-000000000002', NOW() - INTERVAL '10 seconds', 'hydraulic_pressure_bar', 148.7, NULL, 'GOOD', 'bar'),
    -- Traffic signal state (1=red, 2=amber, 3=green)
    ('b2000000-0000-0000-0000-000000000002', NOW() - INTERVAL '10 seconds', 'signal_state',      3.0,   NULL,  'GOOD', NULL),
    -- Emergency stop status
    ('b2000000-0000-0000-0000-000000000002', NOW() - INTERVAL '10 seconds', 'emergency_stop',    NULL,  FALSE, 'GOOD', NULL),
    -- Cycle count
    ('b2000000-0000-0000-0000-000000000002', NOW() - INTERVAL '10 seconds', 'cycle_count',       14823.0, NULL, 'GOOD', NULL),
    -- Ambient temperature
    ('b2000000-0000-0000-0000-000000000002', NOW() - INTERVAL '10 seconds', 'ambient_temp_c',    41.3,  NULL,  'GOOD', 'degC')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Sample telemetry — BRM-PLB-001 (in ALARM state — elevated anomaly score)
-- ---------------------------------------------------------------------------
INSERT INTO telemetry_records (skid_id, timestamp, tag_name, value_float, quality, unit)
VALUES
    ('b2000000-0000-0000-0000-000000000003', NOW() - INTERVAL '2 minutes', 'belt_speed',         5.18,  'GOOD', 'm/s'),
    ('b2000000-0000-0000-0000-000000000003', NOW() - INTERVAL '2 minutes', 'motor_current_a',    198.4, 'GOOD', 'A'),
    ('b2000000-0000-0000-0000-000000000003', NOW() - INTERVAL '2 minutes', 'belt_tension_kn',    104.2, 'GOOD', 'kN'),
    -- Elevated magnetic reading at sensor 3 — rip signature
    ('b2000000-0000-0000-0000-000000000003', NOW() - INTERVAL '2 minutes', 'mag_sensor_0_mt',    14.3,  'GOOD', 'mT'),
    ('b2000000-0000-0000-0000-000000000003', NOW() - INTERVAL '2 minutes', 'mag_sensor_1_mt',    14.1,  'GOOD', 'mT'),
    ('b2000000-0000-0000-0000-000000000003', NOW() - INTERVAL '2 minutes', 'mag_sensor_2_mt',    14.5,  'GOOD', 'mT'),
    ('b2000000-0000-0000-0000-000000000003', NOW() - INTERVAL '2 minutes', 'mag_sensor_3_mt',    28.7,  'GOOD', 'mT'),  -- anomaly
    ('b2000000-0000-0000-0000-000000000003', NOW() - INTERVAL '2 minutes', 'composite_score',    0.87,  'GOOD', NULL),  -- above threshold
    ('b2000000-0000-0000-0000-000000000003', NOW() - INTERVAL '2 minutes', 'magnetic_anomaly',   1.0,   'GOOD', NULL),  -- detected
    ('b2000000-0000-0000-0000-000000000003', NOW() - INTERVAL '2 minutes', 'ambient_temp_c',     42.7,  'GOOD', 'degC'),
    ('b2000000-0000-0000-0000-000000000003', NOW() - INTERVAL '2 minutes', 'power_kw',           92.1,  'GOOD', 'kW')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Sample alerts
-- ---------------------------------------------------------------------------
INSERT INTO alert_records (
    id, skid_id, timestamp, alert_id, severity, message,
    tag_name, value_json, acknowledged, acknowledged_by, acknowledged_at, resolved_at
)
VALUES
    -- WARNING: Belt speed slightly below nominal (BRM-NWM-001)
    (
        'c3000000-0000-0000-0000-000000000001',
        'b2000000-0000-0000-0000-000000000001',
        NOW() - INTERVAL '1 hour',
        'BRM_SPEED_LOW',
        'WARNING',
        'Belt speed 4.1 m/s is below nominal 4.5 m/s (threshold -10%)',
        'belt_speed',
        '{"value": 4.1, "threshold": 4.05, "nominal": 4.5, "unit": "m/s"}',
        TRUE,
        'operator1',
        NOW() - INTERVAL '50 minutes',
        NOW() - INTERVAL '40 minutes'  -- resolved
    ),
    -- ALARM: Magnetic anomaly detected (BRM-PLB-001) — active, unacknowledged
    (
        'c3000000-0000-0000-0000-000000000002',
        'b2000000-0000-0000-0000-000000000003',
        NOW() - INTERVAL '2 minutes',
        'BRM_MAGNETIC_ANOMALY',
        'ALARM',
        'Magnetic anomaly detected — possible belt rip. Composite score: 0.87 (threshold: 0.75). Sensor 3 reading: 28.7 mT (baseline: 14.2 mT)',
        'composite_score',
        '{"composite_score": 0.87, "threshold": 0.75, "sensor_readings": {"mag_sensor_3_mt": 28.7, "baseline_mt": 14.2}, "unit": null}',
        FALSE,
        NULL,
        NULL,
        NULL  -- active
    ),
    -- CRITICAL: Belt rip confirmed — auto-escalated from ALARM (BRM-PLB-001)
    (
        'c3000000-0000-0000-0000-000000000003',
        'b2000000-0000-0000-0000-000000000003',
        NOW() - INTERVAL '1 minute',
        'BRM_RIP_CONFIRMED',
        'CRITICAL',
        'Belt rip confirmed — EMERGENCY STOP engaged. Composite score: 0.95. Immediate inspection required.',
        'composite_score',
        '{"composite_score": 0.95, "escalated_from": "c3000000-0000-0000-0000-000000000002", "auto_escalated": true, "escalation_reason": "ALARM not acknowledged within 10 minutes"}',
        FALSE,
        NULL,
        NULL,
        NULL  -- active
    )
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Sample command record (EMERGENCY_STOP issued to BRM-PLB-001)
-- ---------------------------------------------------------------------------
INSERT INTO command_records (
    id, skid_id, timestamp, command_type, parameters_json,
    source, status, result_json, created_at
)
VALUES (
    'd4000000-0000-0000-0000-000000000001',
    'b2000000-0000-0000-0000-000000000003',
    NOW() - INTERVAL '55 seconds',
    'EMERGENCY_STOP',
    '{"reason": "Belt rip detection — automatic emergency stop", "initiated_by": "alert_processor"}',
    'system:alert_processor',
    'COMPLETED',
    '{"success": true, "skid_response": "ESTOP_ENGAGED", "belt_stopped_at": "2024-01-15T02:58:30Z", "response_time_ms": 180}',
    NOW() - INTERVAL '55 seconds'
)
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Sample writeback audit entry
-- ---------------------------------------------------------------------------
INSERT INTO writeback_audit (
    id, skid_id, timestamp, command_id, target_protocol,
    target_node, value_json, confirmed, operator
)
VALUES (
    'e5000000-0000-0000-0000-000000000001',
    'b2000000-0000-0000-0000-000000000003',
    NOW() - INTERVAL '55 seconds',
    'd4000000-0000-0000-0000-000000000001',
    'MODBUS',
    '1:40001',  -- Modbus slave 1, holding register 40001 (ESTOP coil)
    '{"register": 40001, "value": 1, "description": "Write 1 to ESTOP coil"}',
    TRUE,
    'system:alert_processor'
)
ON CONFLICT (id) DO NOTHING;
