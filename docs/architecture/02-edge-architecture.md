# Flux OT Platform — Edge Architecture

**Version:** 1.0  
**Audience:** Field Engineers, Integration Partners, OT Systems Integrators

---

## Table of Contents

1. [Edge Computing Philosophy](#1-edge-computing-philosophy)
2. [Edge Hardware Requirements](#2-edge-hardware-requirements)
3. [Edge Software Stack](#3-edge-software-stack)
4. [OPC-UA Integration](#4-opc-ua-integration)
5. [Modbus Integration](#5-modbus-integration)
6. [MQTT Edge Broker](#6-mqtt-edge-broker)
7. [Topic Schema](#7-topic-schema)
8. [Offline Resilience](#8-offline-resilience)
9. [Belt Rip Monitor Skid](#9-belt-rip-monitor-skid)
10. [Road Crossing Skid](#10-road-crossing-skid)
11. [Edge Deployment](#11-edge-deployment)
12. [Edge Security](#12-edge-security)

---

## 1. Edge Computing Philosophy

### Why Compute at the Edge

Traditional SCADA architectures poll data to a central server, creating single points of failure and introducing unacceptable latency for safety-critical decisions. Flux OT distributes intelligence to the edge for three core reasons:

**1. Latency**  
Safety actions such as emergency belt stop or barrier closure must execute within 50–200 ms of a trigger condition. Round-trip to a cloud platform over a mine site WAN link (typically 4G/LTE or private radio) introduces 100–500 ms of network latency plus processing time—too slow for safety-critical responses. The edge service makes the safety decision locally using rule-based logic.

**2. Bandwidth Efficiency**  
A 1,200 mm conveyor belt with 8 magnetic sensors, 4 acoustic sensors, and 6 other analog points polled at 500 ms generates ~3,600 data points per minute. Raw streaming at 10 ms resolution would require ~360,000 points per minute per skid. Edge pre-processing filters, aggregates, and publishes only meaningful telemetry, reducing WAN bandwidth by 90%+.

**3. Offline Resilience**  
Mine site connectivity is frequently disrupted by radio interference, adverse weather, or infrastructure maintenance. Edge services buffer up to 1,000 messages locally and flush automatically on reconnection. The belt rip detection logic continues operating during WAN outages—the only degradation is AI inference (which requires the cloud service), and the rule-based fallback continues protecting the asset.

---

## 2. Edge Hardware Requirements

### Minimum Specification (Industrial PC / IPC)

| Parameter | Minimum | Recommended |
|---|---|---|
| CPU | Intel Core i3 (6th gen+) or ARM Cortex-A72 | Intel Core i5/i7 or AMD Ryzen Embedded |
| RAM | 4 GB DDR4 ECC | 8–16 GB DDR4 ECC |
| Storage | 64 GB SSD (industrial grade, -40°C rated) | 256 GB NVMe SSD |
| Network | 2 × GbE (one OT, one IT/DMZ) | 2 × GbE + 4G/LTE fallback modem |
| Serial | 2 × RS-485 for Modbus RTU | 4 × RS-485 |
| USB | 2 × USB 3.0 | 4 × USB 3.0 |
| Operating Temperature | -20°C to 60°C | -40°C to 70°C |
| Ingress Protection | IP40 minimum (rack-mounted) | IP65 (field-mounted enclosure) |
| Power | 12–24 VDC | 9–36 VDC wide-range input |
| Vibration | IEC 60068-2-6 | IEC 60068-2-64 random vibration |
| MTBF | > 50,000 hours | > 100,000 hours |

### Certified Industrial Vendors

| Vendor | Model Series | Notes |
|---|---|---|
| Siemens | SIMATIC IPC427E, IPC847E | Tier 1 industrial, full IEC 61131 integration |
| Beckhoff | CX2033, CX2040 | TwinCAT runtime option, good DIN-rail form factor |
| Moxa | MC-7230, DA-820C | Rugged, -40°C rated, strong serial/IO expansion |
| Advantech | MIC-770, UNO-2484G | Wide range, good Australian distributor support |
| Phoenix Contact | BL2 BPC | DIN-rail, compact, fanless |
| Kontron | KBOX A-203 | Compact fanless, EN 50121-4 (EMC railway, applicable to mining) |

> **Note:** All edge hardware must be assessed against site-specific hazardous area classifications (Zone 2 gas/dust or equivalent) by a qualified hazardous area engineer before installation in potentially explosive atmospheres.

---

## 3. Edge Software Stack

### Layer Diagram

```
┌──────────────────────────────────────────────────────┐
│  Application Layer                                    │
│  ┌────────────────────┐  ┌────────────────────────┐  │
│  │  Belt Rip Monitor  │  │   Road Crossing Skid   │  │
│  │  (Python service)  │  │   (Python service)     │  │
│  └────────────────────┘  └────────────────────────┘  │
├──────────────────────────────────────────────────────┤
│  Edge Common Libraries                                │
│  opcua_client | modbus_adapter | mqtt_publisher      │
│  base_skid | data_models | config | logging_config   │
├──────────────────────────────────────────────────────┤
│  Container Runtime: containerd 1.7                   │
├──────────────────────────────────────────────────────┤
│  Kubernetes: K3s v1.29 (single-node)                 │
│  Flannel CNI | Local-path StorageClass               │
├──────────────────────────────────────────────────────┤
│  Operating System: RHEL 9.3 / Ubuntu 22.04 LTS       │
│  SELinux enforcing | CIS Hardened | Chrony NTP       │
├──────────────────────────────────────────────────────┤
│  Hardware: Industrial PC (IPC)                       │
│  Intel i5/i7 | 8 GB RAM | 256 GB SSD               │
│  Dual GbE NIC | RS-485 Serial                        │
└──────────────────────────────────────────────────────┘
```

### K3s Configuration

K3s is deployed in single-node mode (no HA controller) per edge skid to minimize resource consumption:

```bash
# Install K3s without traefik and servicelb (not needed for edge)
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server \
  --disable traefik \
  --disable servicelb \
  --node-name edge-$(hostname) \
  --data-dir /opt/k3s-data" sh -
```

### Python Environment

Edge services use Python 3.12 with the following key dependencies:

| Package | Version | Purpose |
|---|---|---|
| asyncua | 1.0.x | OPC-UA client |
| pymodbus | 3.6.x | Modbus TCP/RTU client |
| paho-mqtt | 2.0.x | MQTT client |
| pydantic | 2.7.x | Data validation and serialization |
| pydantic-settings | 2.2.x | Environment variable configuration |
| aiohttp | 3.9.x | HTTP client for AI service calls |

---

## 4. OPC-UA Integration

### OPC-UA Overview

OPC-UA (Unified Architecture, IEC 62541) is the primary field protocol for connecting edge services to PLCs and DCS systems. It provides:

- **Structured information model** — hierarchical namespace with typed nodes
- **Security** — authentication, authorization, encryption, message signing
- **Reliability** — session persistence, subscription monitoring, status codes
- **Multi-vendor interoperability** — certified implementations from all major PLC vendors

### Supported PLC/DCS OPC-UA Servers

| Vendor | Product | OPC-UA Server | Notes |
|---|---|---|---|
| Siemens | S7-1500, S7-1200 | Built-in (FW V2.0+) | Best-in-class OPC-UA server, supports all security policies |
| Allen-Bradley | ControlLogix, CompactLogix | via RSLinx or FactoryTalk | Requires separate OPC-UA gateway in older firmware |
| Emerson | DeltaV DCS | DeltaV OPC-UA Server | Full address space browsing support |
| ABB | 800xA, AC500 | OPC-UA built-in (AC500 V3.x) | |
| Schneider Electric | Modicon M580 | EcoStruxure OPC-UA | |
| Rockwell | PLC5, SLC (legacy) | via OPC-UA wrapper gateway | Legacy hardware requires separate gateway device |

### Security Policies

```python
# EdgeConfig settings — production must use at minimum Basic256Sha256
OPCUA_SECURITY_POLICY = "Basic256Sha256"  # Options: None, Basic256Sha256, Aes128Sha256RsaOaep
OPCUA_SECURITY_MODE   = "SignAndEncrypt"  # Options: None, Sign, SignAndEncrypt
OPCUA_CERT_PATH       = "/etc/fluxot/certs/opcua-client.pem"
OPCUA_KEY_PATH        = "/etc/fluxot/certs/opcua-client.key"
```

| Policy | Use Case | Key Strength | Signature | Encryption |
|---|---|---|---|---|
| `None` | Development/lab only — NEVER in production | N/A | None | None |
| `Basic256Sha256` | Production minimum | RSA-2048 | SHA-256 | AES-256 CBC |
| `Aes128Sha256RsaOaep` | High-security installations | RSA-2048 | SHA-256 | AES-128 GCM |

### Node ID Naming Conventions

Flux OT uses namespace 2 (`ns=2`) for all application-specific nodes, with a structured naming hierarchy:

```
ns=2;s=<Category>.<MeasurementName><Index>

Categories:
  Sensor.*        — All sensor input values
  BeltStatus.*    — Belt operational status flags
  BeltControl.*   — Belt control setpoints and commands
  SafetySystem.*  — Safety relay and interlock states
  Diagnostics.*   — Device health and diagnostic data

Examples:
  ns=2;s=Sensor.Mag1              — Magnetic sensor channel 1
  ns=2;s=Sensor.Acoustic3         — Acoustic sensor channel 3
  ns=2;s=Sensor.BeltSpeed         — Belt surface speed (m/s)
  ns=2;s=Sensor.BeltTension       — Belt tension (kN)
  ns=2;s=Sensor.MotorCurrent      — Drive motor current (A)
  ns=2;s=BeltStatus.Running       — Belt running status (boolean)
  ns=2;s=BeltControl.EmergencyStop — E-stop command coil (boolean)
  ns=2;s=BeltControl.SpeedSetpoint — Speed setpoint (m/s)
```

### Subscription vs Polling

Flux OT uses **polling** (cyclic reads) rather than OPC-UA subscriptions for the following reasons:

1. **Determinism** — poll intervals are exactly aligned with the configured `POLL_INTERVAL_MS` (default 500 ms), making data analysis simpler
2. **Bandwidth control** — subscription-driven models can flood the edge service if many nodes change simultaneously
3. **Reconnect simplicity** — no subscription state needs to be restored after reconnect; just restart the poll loop

For future high-frequency applications (e.g., vibration analysis at 10+ kHz), OPC-UA subscriptions with `MonitoredItem` intervals will be used.

---

## 5. Modbus Integration

### Modbus TCP (Networked Devices)

Used for modern Modbus devices connected via Ethernet:

```python
# EdgeConfig settings for Modbus TCP
MODBUS_HOST     = "192.168.1.100"   # Device IP address
MODBUS_PORT     = 502               # Standard Modbus TCP port
MODBUS_UNIT_ID  = 1                 # Slave/Unit ID
MODBUS_TIMEOUT  = 3.0               # Request timeout in seconds
```

### Modbus RTU (Legacy Serial Devices)

Used for older field instruments connected via RS-485:

- **Baud rate:** Typically 9,600 or 19,200 bps
- **Parity:** Even (E) — most common in industrial devices
- **Stop bits:** 1
- **Maximum cable length:** 1,200 m at 9,600 bps

For Modbus RTU over RS-485, a USB-to-RS485 or native RS-485 serial port is required on the IPC. The pymodbus library handles both transport layers transparently.

### Register Map Conventions

Flux OT uses a standardized register map layout for all Modbus devices:

| Register Block | Address Range | Data Type | Description |
|---|---|---|---|
| Status flags | 00001–00100 | Coil (1-bit) | Running, fault, alarm states |
| Measurements | 30001–30100 | Input Register (16-bit) | Sensor raw values (scaled integers) |
| Setpoints | 40001–40100 | Holding Register (16-bit) | Operator-settable parameters |
| Float values | 30101–30200 | Input Register (2×16-bit) | IEEE 754 floats (big-endian word order) |
| Control output | 40101–40200 | Holding Register (16-bit) | Command writeback values |

**Scaling convention:** Integer registers store physical values multiplied by a scale factor (typically 100 or 1000) to preserve decimal precision. Scale factors must be documented in the device commissioning sheet.

---

## 6. MQTT Edge Broker

### Eclipse Mosquitto Configuration

Each edge cluster runs a local Mosquitto instance as a sidecar to provide:
- Message queuing during brief disconnections before the WAN link drops
- Local MQTT for future on-skid dashboard/HMI integration
- Topic routing and ACL enforcement at the edge

```ini
# /etc/mosquitto/mosquitto.conf (production)
listener 8883
protocol mqtt

# TLS configuration
cafile   /etc/fluxot/certs/ca.pem
certfile /etc/fluxot/certs/mosquitto-server.pem
keyfile  /etc/fluxot/certs/mosquitto-server.key
require_certificate true
use_identity_as_username true

# Connection limits
max_connections 50
max_queued_messages 2000
max_packet_size 1048576

# Persistence
persistence true
persistence_location /var/lib/mosquitto/

# Logging
log_dest file /var/log/mosquitto/mosquitto.log
log_type error
log_type warning
log_type notice
log_type information
```

### Bridge to Platform MQTT Broker

Mosquitto bridges the edge topic namespace to the platform AMQ Broker in the DMZ:

```ini
# Bridge configuration (appended to mosquitto.conf)
connection fluxot-platform-bridge
address mqtt-dmz.fluxot.internal:8883

# Bridge TLS
bridge_cafile   /etc/fluxot/certs/ca.pem
bridge_certfile /etc/fluxot/certs/edge-bridge.pem
bridge_keyfile  /etc/fluxot/certs/edge-bridge.key

# Bridge topics — forward telemetry/alerts upward, subscribe to commands downward
topic fluxot/+/+/+/telemetry  out 1
topic fluxot/+/+/+/alerts     out 2
topic fluxot/+/+/+/heartbeat  out 1
topic fluxot/+/+/+/writeback  out 2
topic fluxot/+/+/+/commands   in  2

bridge_protocol_version mqttv50
keepalive_interval 30
restart_timeout 10
```

### QoS Level Strategy

| Message Type | QoS | Reason |
|---|---|---|
| Telemetry | 1 (at least once) | Acceptable to receive duplicate readings; broker crash must not lose data |
| Heartbeat | 1 (at least once) | Liveness signals; occasional duplicates harmless |
| Alerts | 2 (exactly once) | No duplicate alert processing; no lost alerts acceptable |
| Commands | 2 (exactly once) | Must not execute command twice; must not lose commands |
| Writeback audit | 2 (exactly once) | Tamper-evident audit trail requires exactly-once delivery |
| LWT (status) | 1 (at least once) | Retained offline notification; duplicates harmless |

### TLS Configuration

All MQTT connections use **TLS 1.3** with mutual certificate authentication (mTLS):

- **Server certificate:** Issued by Flux OT internal CA, hostname-validated
- **Client certificate:** Per-skid certificate with `CN={SITE_ID}-{SKID_ID}` — used as MQTT client identifier
- **CA certificate:** Shared Flux OT Root CA, distributed to all edge and platform nodes via cert-manager or manual provisioning

---

## 7. Topic Schema

Full MQTT topic hierarchy for all message types published by Flux OT edge services:

| Topic Pattern | Publisher | Subscriber | QoS | Description |
|---|---|---|---|---|
| `fluxot/{site_id}/{skid_type}/{skid_id}/telemetry` | Edge service | Platform (bridge) | 1 | Sensor readings batch (500 ms) |
| `fluxot/{site_id}/{skid_type}/{skid_id}/alerts` | Edge service | Platform (bridge) | 2 | Alert/alarm events |
| `fluxot/{site_id}/{skid_type}/{skid_id}/heartbeat` | Edge service | Platform (bridge) | 1 | Liveness signal (30 s interval) |
| `fluxot/{site_id}/{skid_type}/{skid_id}/writeback` | Edge service | Platform (bridge) | 2 | Writeback audit records |
| `fluxot/{site_id}/{skid_type}/{skid_id}/commands` | Platform | Edge service | 2 | Commands from operator/AI |
| `fluxot/{site_id}/{skid_type}/{skid_id}/cmd-ack` | Edge service | Platform (bridge) | 2 | Command acknowledgements |
| `fluxot/{site_id}/{skid_type}/{skid_id}/status` | Edge (LWT) | Platform | 1 | Retained online/offline status |

**Topic variable substitutions:**

| Variable | Format | Example |
|---|---|---|
| `{site_id}` | `SITE_[A-Z0-9]+` | `SITE_KALGOORLIE_01` |
| `{skid_type}` | `BELT_RIP` or `ROAD_CROSSING` | `BELT_RIP` |
| `{skid_id}` | `SKID_[A-Z0-9]+` | `SKID_CV001` |

---

## 8. Offline Resilience

### Local Message Buffer

The `MQTTPublisher` class maintains a **ring buffer** (Python `collections.deque`) with a configurable maximum size (`BUFFER_SIZE`, default 1,000 messages). When the MQTT broker is unreachable:

1. Messages are serialized and appended to the buffer
2. If the buffer is full, the oldest message is dropped (ring buffer semantics)
3. On reconnect, the buffer is flushed in FIFO order before new messages are published

At 500 ms poll interval with 18 tags per skid, 1,000 buffered messages represents **~28 seconds of uninterrupted telemetry** buffered locally.

### SQLite Fallback (Extended Outages)

For WAN outages exceeding the ring buffer capacity, a SQLite fallback database stores telemetry batches on local SSD. This provides:

- Up to **48 hours** of full-resolution telemetry at 500 ms intervals (dependent on storage capacity)
- Automatic detection of SQLite data on reconnect
- Bulk upload with backpressure management (does not flood the broker on reconnect)

### Reconnection Logic

The `MQTTPublisher` and `OPCUAClient` both implement **exponential backoff reconnection**:

```
Initial delay: 1 second (MQTT) / 2 seconds (OPC-UA)
Backoff multiplier: 2×
Maximum delay: 60 seconds
On reconnect: flush offline buffer, restore subscriptions
```

### Heartbeat Monitoring

Edge services publish a heartbeat every `HEARTBEAT_INTERVAL_S` seconds (default 30). The platform API marks a skid as `OFFLINE` if no heartbeat is received within **3× the interval** (90 seconds by default). The MQTT LWT message is published immediately by the broker when the TCP connection drops, providing sub-second outage detection.

---

## 9. Belt Rip Monitor Skid

### Purpose

Belt conveyor rips are one of the most costly failure modes in mining. A single rip event can cause hours of downtime, hundreds of thousands of dollars in belt replacement costs, and catastrophic injury risk if not detected and stopped quickly. The Belt Rip Monitor Skid detects longitudinal belt rips within 2–5 seconds of initiation and executes an emergency stop within 50 ms of detection.

### Physical Setup — Sensor Placement

```
Belt conveyor — top view:

     ╔═══════════════════════════════════════════════════╗
     ║  LOADING ZONE                                     ║
     ╠═══════════════════════════════════════════════════╣
     ║                                                   ║
     ║  [M1][M2][M3][M4]    ← Magnetic sensors array A  ║
     ║                                                   ║
     ║  =====================  ← Belt surface            ║
     ║                                                   ║
     ║  [M5][M6][M7][M8]    ← Magnetic sensors array B  ║
     ║  [A1][A2]            ← Acoustic sensors (head)   ║
     ║  [L1]     [L2]       ← Load cells (1 each side)  ║
     ║                                                   ║
     ╠═══════════════════════════════════════════════════╣
     ║  DISCHARGE ZONE                                   ║
     ╚═══════════════════════════════════════════════════╝

Side elevation:
     ─────────────────────────────────────────
     [A3]    [A4]                             ← Acoustic sensors (return side)
     ─────────────────────────────────────────
```

**Sensor spacing:** Magnetic arrays should be placed 5–10 m before the head pulley. Load cells are mounted on idler frames. Spacing between sensors in each array: 150–200 mm for 1,200 mm belt.

### Sensor Suite

| Sensor | Tag Name | Unit | Nominal Range | Detection Role |
|---|---|---|---|---|
| Magnetic sensor 1–8 | `Sensor.Mag1`–`Sensor.Mag8` | mT | 40–70 mT | Primary rip detection via magnetic disruption |
| Acoustic sensor 1–4 | `Sensor.Acoustic1`–`Sensor.Acoustic4` | dB | 55–75 dB | Secondary detection via sound signature |
| Load cell 1–2 | `Sensor.LoadCell1`–`Sensor.LoadCell2` | kN | 15–30 kN | Tertiary detection via load imbalance |
| Belt speed | `Sensor.BeltSpeed` | m/s | 2–5 m/s | Speed-normalized threshold adjustment |
| Belt tension | `Sensor.BeltTension` | kN | 100–250 kN | Mechanical integrity indicator |
| Motor current | `Sensor.MotorCurrent` | A | 60–120 A | Motor load anomaly detection |
| Belt temperature | `Sensor.BeltTemperature` | °C | 20–60 °C | Splice failure early warning |
| Running status | `BeltStatus.Running` | bool | true/false | Enable/disable detection logic |

### Detection Algorithm

The detection logic uses a multi-modal weighted scoring approach:

```python
def compute_rip_score(sensor_data: BeltSensorData) -> float:
    """
    Returns a composite rip detection confidence score (0.0–1.0).
    Score >= RIP_DETECTION_THRESHOLD (default 0.75) triggers alarm.
    """
    score = 0.0
    
    # 1. Magnetic anomaly (weight: 0.50)
    mag_mean = statistics.mean(sensor_data.magnetic_sensors)
    mag_std  = statistics.stdev(sensor_data.magnetic_sensors)
    mag_cv   = mag_std / mag_mean if mag_mean > 0 else 0  # coefficient of variation
    # A rip causes a sharp asymmetry across the sensor array
    mag_score = min(1.0, mag_cv / 0.15)  # 15% CV → score=1.0
    score += 0.50 * mag_score
    
    # 2. Acoustic energy spike (weight: 0.30)
    acou_max  = max(sensor_data.acoustic_sensors)
    acou_base = 60.0  # nominal dB baseline
    acou_score = min(1.0, max(0.0, (acou_max - acou_base) / 20.0))  # 20 dB above baseline → 1.0
    score += 0.30 * acou_score
    
    # 3. Load cell imbalance (weight: 0.20)
    load_diff = abs(sensor_data.load_cells[0] - sensor_data.load_cells[1])
    load_score = min(1.0, load_diff / 5.0)  # 5 kN differential → score=1.0
    score += 0.20 * load_score
    
    return score
```

### AI Enhancement

The cloud AI service (LSTM neural network) provides a parallel prediction that fuses:
- 5-minute rolling window of all sensor time-series
- Derived features: magnetic variance trend, acoustic spectral centroid, load cell correlation
- Belt speed normalization

The AI prediction score is combined with the rule-based score:
```
final_score = 0.6 × rule_score + 0.4 × ai_score
```

When the AI service is unavailable (offline), `ai_score = rule_score` (rule-based alone).

### Writeback Actions

| Condition | Action | OPC-UA Write | Alert Severity |
|---|---|---|---|
| Score 0.50–0.74 | `MONITOR` — no action | None | WARNING |
| Score 0.75–0.89 | `SLOW_DOWN` — reduce to 50% speed | `ns=2;s=BeltControl.SpeedSetpoint` = 50% of nominal | ALARM |
| Score ≥ 0.90 | `EMERGENCY_STOP` — stop belt immediately | `ns=2;s=BeltControl.EmergencyStop` = TRUE | CRITICAL |
| Manual operator command | `EMERGENCY_STOP` | `ns=2;s=BeltControl.EmergencyStop` = TRUE | — |
| Manual operator command | `MAINTENANCE_MODE` | `ns=2;s=BeltStatus.MaintenanceMode` = TRUE | INFO |

---

## 10. Road Crossing Skid

### Purpose

Autonomous haul roads at mining operations carry 400-tonne trucks at speeds up to 40 km/h. Road crossings with conveyor belts create intersection hazards where vehicle-belt collisions can be fatal. The Road Crossing Skid provides an automated decision engine that:

1. Detects approaching or waiting vehicles using multiple sensor technologies
2. Determines when it is safe for the conveyor belt to cross vehicle traffic paths
3. Controls physical barriers and traffic light signals
4. Maintains a complete audit trail of all crossing events

### Decision Engine State Machine

```
                    ┌─────────────────┐
                    │   BELT_RUNNING  │◄──────────────────────┐
                    │   (default)     │                       │
                    └────────┬────────┘                       │
                             │                                │
                    Vehicle detected                    Belt cleared
                    (loop/radar/LiDAR)                  (no vehicle)
                             │                                │
                    ┌────────▼────────┐                       │
                    │  VEHICLE_QUEUE  │                       │
                    │  (accumulating) │                       │
                    └────────┬────────┘                       │
                             │                                │
                    Belt stop window                          │
                    reached (safe gap)                        │
                             │                                │
                    ┌────────▼────────┐                       │
                    │   SAFE_WINDOW   │                       │
                    │   (barriers     │                       │
                    │    open)        │                       │
                    └────────┬────────┘                       │
                             │                                │
                    Vehicle enters / first axle crossed       │
                             │                                │
                    ┌────────▼────────┐                       │
                    │  CROSSING_IN_   │                       │
                    │  PROGRESS       │                       │
                    └────────┬────────┘                       │
                             │                                │
                    Last axle cleared +                       │
                    clearance distance confirmed              │
                             │                                │
                    ┌────────▼────────┐                       │
                    │   CLEARANCE     ├───────────────────────┘
                    │   CONFIRMED     │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │    EMERGENCY    │ ← Any sensor detects
                    │    (barriers    │   vehicle in belt path
                    │     close,      │   while belt running,
                    │     belt stop)  │   or manual E-Stop
                    └─────────────────┘
```

### Sensor Suite

| Sensor | Type | Location | Primary Role |
|---|---|---|---|
| Loop detector 1 | Inductive loop | 50 m approach (inbound) | Vehicle approach detection |
| Loop detector 2 | Inductive loop | 10 m approach (inbound) | Vehicle commit point |
| Loop detector 3 | Inductive loop | 10 m departure (outbound) | Vehicle clear confirmation |
| Radar 1 | 24 GHz FMCW radar | Crossing zone | Vehicle presence + speed |
| Radar 2 | 24 GHz FMCW radar | Crossing zone (far side) | Vehicle classification |
| LiDAR (optional) | 3D point cloud | Overhead gantry | Spatial occupancy mapping |
| Camera 1–2 (optional) | IP camera | Approach lanes | Computer vision + recording |
| Barrier position 1 | Limit switch | Barrier arm | Barrier fully open/closed |
| Barrier position 2 | Limit switch | Barrier arm | Barrier fully open/closed |

### Safety Interlocks (Fail-Safe Design)

All interlocks are **normally-energized** — a power loss, communication failure, or sensor fault causes the system to default to the **SAFE** (barriers closed, belt stopped) state:

1. **Power failure** → barriers spring to closed position via pneumatic/hydraulic fail-safe actuators
2. **Communication failure** (edge-to-PLC timeout > 500 ms) → PLC autonomous safety hold
3. **Sensor fault** (any primary sensor reporting bad quality) → immediate SAFE state
4. **Belt runaway** (speed > 110% nominal) → EMERGENCY state
5. **Manual E-Stop** (operator button at crossing) → EMERGENCY state, requires manual reset

### Writeback Actions

| Command | OPC-UA Write | Description |
|---|---|---|
| OPEN_BARRIERS | `ns=2;s=CrossingControl.Barrier1Open` + `Barrier2Open` = TRUE | Allow vehicle crossing |
| CLOSE_BARRIERS | `ns=2;s=CrossingControl.Barrier1Open` + `Barrier2Open` = FALSE | Prevent vehicle crossing |
| SET_TRAFFIC_LIGHT | `ns=2;s=CrossingControl.TrafficLight` = RED/AMBER/GREEN | Traffic light control |
| BELT_STOP_REQUEST | `ns=2;s=BeltControl.StopRequest` = TRUE | Request belt stop for crossing |
| EMERGENCY | `ns=2;s=SafetySystem.EmergencyStop` = TRUE | Emergency stop all motion |

---

## 11. Edge Deployment

### Prerequisites

```bash
# Verify K3s is running
kubectl get nodes

# Verify connectivity to DMZ MQTT broker
mosquitto_pub -h mqtt-dmz.fluxot.internal -p 8883 \
  --cafile /etc/fluxot/certs/ca.pem \
  --cert /etc/fluxot/certs/edge-$(hostname).pem \
  --key /etc/fluxot/certs/edge-$(hostname).key \
  -t test/connectivity -m '{"test": true}' -d
```

### Deploy Belt Rip Monitor Service

```bash
# Create namespace
kubectl create namespace fluxot-edge

# Deploy ConfigMap with environment variables
kubectl apply -f /opt/fluxot/k3s/manifests/belt-rip-configmap.yaml -n fluxot-edge

# Deploy secrets (pre-created with cert-manager or manual)
kubectl apply -f /opt/fluxot/k3s/manifests/belt-rip-secrets.yaml -n fluxot-edge

# Deploy the service
kubectl apply -f /opt/fluxot/k3s/manifests/belt-rip-deployment.yaml -n fluxot-edge

# Verify
kubectl get pods -n fluxot-edge
kubectl logs -f -n fluxot-edge -l app=belt-rip-monitor
```

### Day-2 Operations

**Restart service after config change:**
```bash
kubectl rollout restart deployment/belt-rip-monitor -n fluxot-edge
```

**Check MQTT buffer depth:**
```bash
kubectl exec -n fluxot-edge deploy/belt-rip-monitor -- \
  python -c "from edge.common.health import get_buffer_depth; print(get_buffer_depth())"
```

**Force sensor re-baseline:**
```bash
# Publish maintenance mode command via MQTT
mosquitto_pub -h localhost -p 8883 \
  -t "fluxot/SITE_001/BELT_RIP/SKID_CV001/commands" \
  -m '{"command_type": "SET_MAINTENANCE_MODE", "parameters": {"enabled": true}}'
```

---

## 12. Edge Security

### Certificate Management

Each edge skid has a unique X.509 certificate used for:
- MQTT client authentication (Common Name = `{SITE_ID}-{SKID_ID}`)
- OPC-UA security (application certificate)

**Certificate lifecycle:**
- **Validity period:** 1 year
- **Renewal trigger:** 30 days before expiry (cert-manager monitors and auto-renews for platform certificates; edge certificates require manual rotation via the deployment scripts)
- **Revocation:** Certificate revocation list (CRL) published by the Flux OT CA; edge certificates revoked immediately on decommission

### Credential Rotation

| Credential | Rotation Frequency | Method |
|---|---|---|
| MQTT client certificate | Annual | Re-deployment of Kubernetes Secret |
| OPC-UA application certificate | Annual | Re-deployment + OPC-UA server trust list update |
| MQTT username/password (if used) | 90 days | Kubernetes Secret update + pod restart |
| Kafka SCRAM credentials | 90 days | AMQ Streams KafkaUser CRD update |

### Physical Security

- IPC enclosure must be **secured with a keyed lock** — key held by site supervisor
- Enclosure tamper switches connected to PLC digital input — alarm on unauthorized opening
- All external ports (USB, console) covered with blanking plates in production
- Management access via dedicated management VLAN only (not OT network)

### Hardening Checklist

- [ ] RHEL 9 SELinux in `enforcing` mode with targeted policy
- [ ] CIS Level 1 benchmark applied (auto-remediation via Ansible)
- [ ] Firewall (firewalld): allow only OPC-UA (4840), MQTT (8883), K3s API (6443)
- [ ] SSH key-based auth only; password auth disabled
- [ ] No root login via SSH
- [ ] Auditd configured for file access to `/etc/fluxot/certs/`
- [ ] Automatic security updates enabled (dnf-automatic)
- [ ] NTP synchronised to site GPS or GNSS time server (Chrony)
- [ ] BIOS/UEFI password set; secure boot enabled
