# Flux OT Platform — Platform Architecture (OpenShift)

**Version:** 1.0  
**Audience:** Platform Engineers, Cloud Architects, DevOps Teams

---

## Table of Contents

1. [OpenShift Overview](#1-openshift-overview)
2. [Namespace Strategy](#2-namespace-strategy)
3. [Messaging Layer](#3-messaging-layer)
4. [Platform API](#4-platform-api)
5. [Data Architecture](#5-data-architecture)
6. [Integration Layer](#6-integration-layer)
7. [AI Integration](#7-ai-integration)
8. [Write-back Architecture](#8-write-back-architecture)
9. [High Availability](#9-high-availability)
10. [Resource Sizing](#10-resource-sizing)
11. [Operator Catalog](#11-operator-catalog)

---

## 1. OpenShift Overview

### Why Red Hat OpenShift

Flux OT runs on Red Hat OpenShift 4.14+ rather than vanilla Kubernetes for the following reasons:

| Feature | OpenShift Advantage |
|---|---|
| **Security** | Pod Security Admission enforced by default; built-in image scanning (RHACS); SCC (SecurityContextConstraints) |
| **Operator ecosystem** | OperatorHub provides certified operators for AMQ Streams, AMQ Broker, Camel K, and Grafana with enterprise support SLAs |
| **Image registry** | Internal registry with image signing, vulnerability scanning, and pull-through caching |
| **Route object** | Native TLS passthrough/reencrypt routes simplify external MQTT and API exposure without additional ingress controllers |
| **Enterprise support** | Red Hat subscription provides certified security patches within 24 hours for critical CVEs |
| **Compliance** | OpenShift ships with CIS benchmarks, FIPS 140-2 mode, and FedRAMP controls pre-integrated |

### OpenShift Version Requirements

| Component | Minimum Version | Notes |
|---|---|---|
| OpenShift | 4.12 | OVN-Kubernetes becomes default CNI in 4.12 |
| OCP worker nodes | RHCOS or RHEL 9.2+ | CoreOS preferred for control plane |
| oc CLI | 4.12+ | Must match cluster version |
| Helm | 3.12+ | For non-Operator components |

---

## 2. Namespace Strategy

### Namespace Layout

| Namespace | Purpose | Workloads |
|---|---|---|
| `fluxot-messaging` | Kafka and MQTT messaging layer | AMQ Streams Kafka cluster, Zookeeper, Entity Operator, AMQ Broker |
| `fluxot-platform` | Core application services | Platform API, MQTT-Kafka bridge, TimescaleDB, Redis |
| `fluxot-ai` | AI inference and MLflow | Belt rip AI service, Road crossing AI service, MLflow tracking server |
| `fluxot-monitoring` | Observability stack | Grafana, Prometheus, Alertmanager, Loki |

### RBAC Design

```yaml
# Service accounts per namespace — minimum required permissions
fluxot-messaging:    kafka-admin-sa      # Manage KafkaTopic/KafkaUser CRDs
fluxot-platform:     platform-api-sa     # Read ConfigMaps/Secrets; list Pods
fluxot-platform:     timescaledb-sa      # PVC access; Secret read for credentials
fluxot-ai:          ai-inference-sa     # Read models from PVC; call platform API
fluxot-monitoring:   grafana-sa         # Read ServiceMonitor; create ConfigMaps
```

```yaml
# ClusterRoleBindings (cross-namespace monitoring)
grafana-sa → ClusterRole: view          # Read pods/services across all namespaces
prometheus-sa → ClusterRole: monitoring # Read metrics endpoints across all namespaces
```

---

## 3. Messaging Layer

### 3.1 AMQ Streams (Kafka)

The Kafka cluster (`fluxot-kafka`) is deployed via the AMQ Streams operator in the `fluxot-messaging` namespace. See `deploy/openshift/messaging/kafka-cluster.yaml` for the full CRD specification.

**Cluster topology:**
- 3 Kafka brokers across 3 OpenShift availability zones
- 3 ZooKeeper nodes (migrating to KRaft in future release)
- Rack awareness via `topology.kubernetes.io/zone` label
- 100 Gi persistent storage per broker (gp3-csi StorageClass)

**Topic naming convention:**

| Topic Name | Partitions | Replication | Retention | Description |
|---|---|---|---|---|
| `fluxot.telemetry.belt_rip` | 12 | 3 | 7 days | Belt rip telemetry batches |
| `fluxot.telemetry.road_crossing` | 6 | 3 | 7 days | Road crossing telemetry batches |
| `fluxot.telemetry.generic` | 6 | 3 | 7 days | Generic skid telemetry |
| `fluxot.alerts` | 3 | 3 | 90 days | All alert events |
| `fluxot.commands` | 6 | 3 | 24 hours | Commands from platform to edge |
| `fluxot.cmd-ack` | 6 | 3 | 24 hours | Command acknowledgements from edge |
| `fluxot.heartbeats` | 3 | 3 | 2 hours | Edge skid heartbeats |
| `fluxot.writeback-audit` | 3 | 3 | 365 days | Writeback audit trail |
| `fluxot.predictions.belt_rip` | 6 | 3 | 7 days | AI predictions for belt rip |
| `fluxot.predictions.road_crossing` | 6 | 3 | 7 days | AI predictions for road crossing |

**Consumer group design:**

| Consumer Group | Consumes | Purpose |
|---|---|---|
| `fluxot-platform-api` | `fluxot.telemetry.*` | Write telemetry to TimescaleDB |
| `fluxot-alert-processor` | `fluxot.alerts` | Process and persist alerts |
| `fluxot-ai-belt-rip` | `fluxot.telemetry.belt_rip` | Feature extraction for ML inference |
| `fluxot-ai-road-crossing` | `fluxot.telemetry.road_crossing` | Feature extraction for ML inference |
| `fluxot-command-router` | `fluxot.commands` | Route commands to MQTT topics |
| `fluxot-prediction-consumer` | `fluxot.predictions.*` | Persist and act on AI predictions |

### 3.2 AMQ Broker (ActiveMQ Artemis)

The AMQ Broker provides MQTT protocol support within the OpenShift cluster, acting as the platform-side MQTT endpoint:

```yaml
apiVersion: broker.amq.io/v1beta1
kind: ActiveMQArtemis
metadata:
  name: fluxot-broker
  namespace: fluxot-messaging
spec:
  version: "7.12.0"
  deploymentPlan:
    size: 2
    requireLogin: true
    persistenceEnabled: true
    messageMigration: true
  acceptors:
    - name: mqtt
      port: 8883
      protocols: MQTT
      sslEnabled: true
      sslSecret: fluxot-broker-tls
      enabledCipherSuites: TLS_AES_256_GCM_SHA384
      enabledProtocols: TLSv1.3
      needClientAuth: true   # mTLS required
  console:
    expose: true
```

### 3.3 MQTT-Kafka Bridge Service

A custom Python service in `fluxot-platform` bridges MQTT messages from the AMQ Broker to Kafka topics and vice versa:

**MQTT → Kafka mapping:**

| MQTT Topic Pattern | Kafka Topic |
|---|---|
| `fluxot/+/BELT_RIP/+/telemetry` | `fluxot.telemetry.belt_rip` |
| `fluxot/+/ROAD_CROSSING/+/telemetry` | `fluxot.telemetry.road_crossing` |
| `fluxot/+/+/+/alerts` | `fluxot.alerts` |
| `fluxot/+/+/+/heartbeat` | `fluxot.heartbeats` |
| `fluxot/+/+/+/writeback` | `fluxot.writeback-audit` |
| `fluxot/+/+/+/cmd-ack` | `fluxot.cmd-ack` |

**Kafka → MQTT mapping (commands):**

| Kafka Topic | MQTT Topic |
|---|---|
| `fluxot.commands` | `fluxot/{site_id}/{skid_type}/{skid_id}/commands` |

---

## 4. Platform API

### Architecture

The Platform API is a **FastAPI** application running with **Uvicorn** in async mode. It provides the single unified REST interface for:

- Dashboard data access (telemetry, alerts, status)
- Command issuance and writeback initiation
- Site and skid registration/management
- Internal telemetry ingestion from the Kafka consumer

```
┌──────────────────────────────────────────────────────────┐
│                    Platform API                           │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │  FastAPI Application (Uvicorn, 4 workers)        │   │
│  │  ┌──────────────┐  ┌──────────────────────────┐  │   │
│  │  │ Auth Router  │  │  Telemetry Router        │  │   │
│  │  │ /api/v1/auth │  │  /api/v1/telemetry       │  │   │
│  │  └──────────────┘  └──────────────────────────┘  │   │
│  │  ┌──────────────┐  ┌──────────────────────────┐  │   │
│  │  │ Alert Router │  │  Command Router          │  │   │
│  │  │ /api/v1/alerts│ │  /api/v1/commands        │  │   │
│  │  └──────────────┘  └──────────────────────────┘  │   │
│  │  ┌──────────────┐  ┌──────────────────────────┐  │   │
│  │  │ Site Router  │  │  AI Router               │  │   │
│  │  │ /api/v1/sites│  │  /api/v1/predictions     │  │   │
│  │  └──────────────┘  └──────────────────────────┘  │   │
│  └──────────────────────────────────────────────────┘   │
│                                                          │
│  ┌──────────────────┐  ┌──────────────────────────────┐  │
│  │  SQLAlchemy 2.0  │  │  Redis AsyncIO Client        │  │
│  │  Async ORM       │  │  Cache + WebSocket fan-out   │  │
│  └──────────────────┘  └──────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
         ↕                            ↕
   TimescaleDB                      Redis
   (PostgreSQL 16)                  (7.x)
```

### REST Endpoint Catalog

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/api/v1/auth/token` | None (public) | Obtain JWT access token |
| `GET` | `/api/v1/auth/me` | Bearer | Current user info |
| `GET` | `/api/v1/sites` | Bearer (Viewer+) | List all sites |
| `POST` | `/api/v1/sites` | Bearer (Admin) | Create site |
| `GET` | `/api/v1/sites/{site_id}` | Bearer (Viewer+) | Get site details |
| `GET` | `/api/v1/sites/{site_id}/skids` | Bearer (Viewer+) | List skids in site |
| `POST` | `/api/v1/sites/{site_id}/skids` | Bearer (Admin) | Register new skid |
| `GET` | `/api/v1/sites/{site_id}/skids/{skid_id}` | Bearer (Viewer+) | Get skid details |
| `PATCH` | `/api/v1/sites/{site_id}/skids/{skid_id}/status` | Bearer (Operator+) | Update skid status |
| `GET` | `/api/v1/telemetry/sites/{site_id}/skids/{skid_id}/latest` | Bearer (Viewer+) | Latest telemetry per tag |
| `GET` | `/api/v1/telemetry/sites/{site_id}/skids/{skid_id}/history` | Bearer (Viewer+) | Paginated telemetry history |
| `GET` | `/api/v1/telemetry/sites/{site_id}/skids/{skid_id}/tags` | Bearer (Viewer+) | List tag names |
| `GET` | `/api/v1/telemetry/sites/{site_id}/dashboard` | Bearer (Viewer+) | Site health dashboard |
| `GET` | `/api/v1/alerts/sites/{site_id}/skids/{skid_id}` | Bearer (Viewer+) | List skid alerts |
| `POST` | `/api/v1/alerts/{alert_id}/acknowledge` | Bearer (Operator+) | Acknowledge alert |
| `POST` | `/api/v1/sites/{site_id}/skids/{skid_id}/commands` | Bearer (Operator+) | Issue command to skid |
| `GET` | `/api/v1/sites/{site_id}/skids/{skid_id}/commands` | Bearer (Viewer+) | List command history |
| `GET` | `/api/v1/sites/{site_id}/skids/{skid_id}/writeback` | Bearer (Viewer+) | List writeback audit |
| `POST` | `/api/v1/telemetry/ingest` | X-Internal-Key header | Internal: ingest telemetry batch |

### WebSocket Real-Time Streaming

```
ws://platform-api.fluxot.svc/api/v1/telemetry/ws/{site_id}/{skid_id}
```

The WebSocket endpoint subscribes to a Redis pub/sub channel (`telemetry:{site_id}:{skid_id}`) and streams telemetry events to connected clients in real-time. Grafana uses this for live dashboard updates.

**Message format:**
```json
{
  "timestamp": "2024-11-15T03:42:00.123Z",
  "site_id": "550e8400-e29b-41d4-a716-446655440000",
  "skid_id": "550e8400-e29b-41d4-a716-446655440001",
  "tag_name": "Sensor.BeltSpeed",
  "value_float": 3.47,
  "quality": "GOOD",
  "unit": "m/s"
}
```

### JWT Authentication and RBAC

JWT tokens are issued via `POST /api/v1/auth/token` using OAuth2 Password flow:

```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 28800,
  "role": "OPERATOR"
}
```

**JWT claims:**
```json
{
  "sub": "operator_jane",
  "role": "OPERATOR",
  "iat": 1700000000,
  "exp": 1700028800
}
```

**RBAC matrix:**

| Endpoint Category | VIEWER | OPERATOR | ADMIN |
|---|---|---|---|
| Read telemetry | ✓ | ✓ | ✓ |
| Read alerts | ✓ | ✓ | ✓ |
| Acknowledge alerts | ✗ | ✓ | ✓ |
| Issue commands | ✗ | ✓ | ✓ |
| Writeback (OPC-UA/Modbus write) | ✗ | ✓ | ✓ |
| Create/modify sites and skids | ✗ | ✗ | ✓ |
| User management | ✗ | ✗ | ✓ |

### Rate Limiting

Implemented via `slowapi` (FastAPI rate limit middleware):
- Unauthenticated requests: 10 requests/minute per IP
- Viewer role: 300 requests/minute per user
- Operator role: 600 requests/minute per user
- Admin role: 1,200 requests/minute per user
- Internal endpoints (`/ingest`): 10,000 requests/minute (internal key bypasses user rate limit)

---

## 5. Data Architecture

### TimescaleDB Configuration

TimescaleDB extends PostgreSQL 16 with time-series optimizations. The `telemetry_records` table is the primary hypertable:

```sql
-- Convert to TimescaleDB hypertable after table creation
SELECT create_hypertable(
    'telemetry_records',
    'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- Compression policy: compress chunks older than 7 days
SELECT add_compression_policy(
    'telemetry_records',
    compress_after => INTERVAL '7 days'
);

-- Retention policy: drop chunks older than 2 years
SELECT add_retention_policy(
    'telemetry_records',
    drop_after => INTERVAL '2 years'
);
```

**Continuous aggregates for dashboard performance:**

```sql
-- 1-minute averages for time-series panels
CREATE MATERIALIZED VIEW telemetry_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp)  AS bucket,
    skid_id,
    tag_name,
    AVG(value_float)                    AS avg_value,
    MIN(value_float)                    AS min_value,
    MAX(value_float)                    AS max_value,
    COUNT(*)                            AS sample_count
FROM telemetry_records
WHERE value_float IS NOT NULL
GROUP BY 1, 2, 3
WITH NO DATA;

-- Refresh policy for continuous aggregate
SELECT add_continuous_aggregate_policy(
    'telemetry_1min',
    start_offset => INTERVAL '2 minutes',
    end_offset   => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '30 seconds'
);

-- 1-hour aggregates for trend panels
CREATE MATERIALIZED VIEW telemetry_1hour
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp) AS bucket,
    skid_id,
    tag_name,
    AVG(value_float)                 AS avg_value,
    MIN(value_float)                 AS min_value,
    MAX(value_float)                 AS max_value,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY value_float) AS p95_value
FROM telemetry_records
WHERE value_float IS NOT NULL
GROUP BY 1, 2, 3
WITH NO DATA;
```

### Redis Caching Strategy

| Key Pattern | TTL | Contents |
|---|---|---|
| `skid:status:{skid_id}` | 120 s | Latest skid status object |
| `telemetry:latest:{skid_id}:{tag}` | 60 s | Latest telemetry reading per tag |
| `dashboard:{site_id}` | 30 s | Cached DashboardSummary response |
| `alert:active:{skid_id}` | 0 (no TTL) | Set of active alert IDs |

Redis pub/sub channels used for WebSocket fan-out:
- `telemetry:{site_id}:{skid_id}` — new telemetry reading published here → forwarded to WebSocket clients

---

## 6. Integration Layer

### Camel K Routes

Apache Camel K is deployed via the Camel K operator for integration and data transformation tasks:

**Route 1: MQTT Telemetry Enrichment**
```yaml
# Enriches incoming MQTT telemetry with site/skid metadata before Kafka publish
from("mqtt5:telemetry?host=amq-broker&clientId=camel-enricher")
  .unmarshal().json()
  .enrich("direct:lookup-skid-metadata")
  .marshal().json()
  .to("kafka:fluxot.telemetry.belt_rip?brokers=fluxot-kafka:9092")
```

**Route 2: Alert Deduplication**
```yaml
# Prevents duplicate alerts within a 60-second window
from("kafka:fluxot.alerts?groupId=camel-dedup")
  .idempotentConsumer(header("alert_id")).messageIdRepository(redisRepo)
  .to("kafka:fluxot.alerts.deduped")
```

**Route 3: External ERP Integration (Future)**
```yaml
# Forward production event summaries to SAP ERP via REST
from("kafka:fluxot.events.production?groupId=camel-erp")
  .transform().groovy("transform.groovy")
  .to("rest:POST:https://erp.site.internal/api/production-events")
```

---

## 7. AI Integration

### How Predictions Flow Back

The AI services consume Kafka telemetry topics, run inference, and publish predictions:

```
Kafka: fluxot.telemetry.belt_rip
         ↓ (AI service consumes)
  Feature extraction (5-min rolling window)
         ↓
  ML inference (Random Forest + LSTM ensemble)
         ↓
  Prediction message produced to:
Kafka: fluxot.predictions.belt_rip
         ↓ (Platform API consumes)
  Persist to: ai_predictions table (TimescaleDB)
         ↓ (threshold check)
  If score > 0.75 AND not already alarmed:
    Produce command to: fluxot.commands (EMERGENCY_STOP or SLOW_DOWN)
```

### Model Lifecycle (MLflow)

MLflow tracks all experiment runs and model versions:

```python
# Model stages in MLflow registry
STAGE_NONE       = "None"       # Newly registered, not yet evaluated
STAGE_STAGING    = "Staging"    # Under A/B shadow testing
STAGE_PRODUCTION = "Production" # Active inference model
STAGE_ARCHIVED   = "Archived"   # Superseded, kept for audit
```

The `ModelRegistry` class (at `ai/common/model_registry.py`) provides a unified interface to MLflow with local filesystem fallback for edge deployments.

---

## 8. Write-back Architecture

### Full Writeback Flow

```
1. OPERATOR ACTION
   └── POST /api/v1/sites/{site_id}/skids/{skid_id}/commands
       Body: {"command_type": "EMERGENCY_STOP", "parameters": {}, "source": "operator_jane"}

2. PLATFORM API
   ├── Validate JWT + check OPERATOR role
   ├── Validate command type against skid type
   ├── Create CommandRecord (status=PENDING) in TimescaleDB
   ├── Publish to Kafka topic: fluxot.commands
   │   Key: {skid_id}, Value: {command JSON}
   └── Return HTTP 202 Accepted + command_id

3. KAFKA COMMAND ROUTER
   ├── Consume from fluxot.commands
   ├── Lookup MQTT topic for skid: fluxot/{site_id}/BELT_RIP/{skid_id}/commands
   ├── Publish command via MQTT QoS 2
   └── Update CommandRecord status=SENT

4. MQTT DMZ BROKER
   └── Route to AMQ Broker → Bridge → Mosquitto Edge

5. EDGE SERVICE (MQTTPublisher subscriber)
   ├── Receive command on /commands topic
   ├── Validate command_id (idempotency check)
   ├── Check safety interlocks (MAINTENANCE_MODE, etc.)
   ├── Execute: OPCUAClient.write_node("ns=2;s=BeltControl.EmergencyStop", True)
   ├── Publish cmd-ack: status=COMPLETED or FAILED
   └── Publish writeback-audit record (QoS 2)

6. WRITEBACK AUDIT (back in platform)
   ├── Consume writeback-audit from Kafka
   ├── Persist WritebackAudit record to TimescaleDB
   ├── Update CommandRecord status=COMPLETED
   └── Redis publish on audit channel (for dashboard update)
```

### Writeback Safety Constraints

1. **No writeback during MAINTENANCE_MODE** — edge service rejects all commands
2. **Command expiry** — commands not executed within 30 seconds are rejected
3. **Idempotency** — command_id tracked; duplicate commands are acknowledged but not re-executed
4. **Operator attribution** — every writeback record includes the operator username
5. **Immutable audit log** — WritebackAudit records have no UPDATE/DELETE permissions

---

## 9. High Availability

### Component HA Configuration

| Component | Mode | Failover Time | Notes |
|---|---|---|---|
| Kafka | 3-broker cluster, RF=3, min.ISR=2 | < 30 s (leader election) | Survive 1 broker failure without data loss |
| ZooKeeper | 3-node ensemble | < 30 s | Quorum-based, survive 1 node failure |
| AMQ Broker | 2-node active/passive | < 15 s | Shared journal via ReadWriteMany PVC |
| Platform API | 2 replicas, HPA | < 10 s (Kubernetes readiness) | Stateless; Redis session store |
| TimescaleDB | 1 primary + 1 hot standby | < 60 s (Patroni failover) | Patroni manages automatic failover |
| Redis | Redis Sentinel (3 nodes) | < 30 s | For caching; data loss on failover is acceptable |
| AI Services | 2 replicas per model | < 30 s | Stateless inference |
| Grafana | 2 replicas + shared SQLite/PostgreSQL | < 10 s | Dashboard definitions in ConfigMaps |

### RTO / RPO Targets

| Scenario | RTO (Recovery Time) | RPO (Recovery Point) |
|---|---|---|
| Single pod failure | < 30 seconds | Zero (stateless pods) |
| Single Kafka broker failure | < 60 seconds | Zero (ISR replication) |
| Database primary failure | < 2 minutes | < 1 second (streaming replication) |
| Availability zone failure | < 5 minutes | < 1 second |
| Full platform cluster failure | < 30 minutes | < 5 minutes (from backup) |
| Edge skid failure | Immediate (redundant skid) | < 5 seconds (buffer flush) |

---

## 10. Resource Sizing

### OpenShift Node Recommendations by Scale

**Starter (1–10 skids):**
```
Control Plane: 3 × 4 vCPU / 16 GB RAM / 100 GB SSD
Worker Nodes:  3 × 8 vCPU / 32 GB RAM / 200 GB SSD
Storage:       300 GB gp3 (Kafka) + 500 GB gp3 (TimescaleDB) + 100 GB gp3 (Redis)
```

**Standard (10–50 skids):**
```
Control Plane: 3 × 8 vCPU / 32 GB RAM / 200 GB SSD
Worker Nodes:  5 × 16 vCPU / 64 GB RAM / 500 GB SSD
Storage:       3 × 100 GB gp3 (Kafka) + 2 TB gp3 (TimescaleDB) + 200 GB gp3 (Redis)
```

**Enterprise (50–200 skids):**
```
Control Plane: 3 × 8 vCPU / 32 GB RAM / 200 GB SSD
Worker Nodes:  7 × 16 vCPU / 64 GB RAM / 500 GB SSD
DB Nodes:      2 × 8 vCPU / 128 GB RAM / 10 TB NVMe (TimescaleDB dedicated)
Storage:       3 × 100 GB gp3 (Kafka) + 10 TB NVMe (DB) + 500 GB (others)
```

### Per-Namespace Resource Quotas

| Namespace | CPU Request | CPU Limit | Memory Request | Memory Limit |
|---|---|---|---|---|
| `fluxot-messaging` | 8 cores | 24 cores | 16 GB | 48 GB |
| `fluxot-platform` | 4 cores | 12 cores | 8 GB | 24 GB |
| `fluxot-ai` | 4 cores | 16 cores | 8 GB | 32 GB |
| `fluxot-monitoring` | 2 cores | 8 cores | 4 GB | 16 GB |

---

## 11. Operator Catalog

All operators are installed from the Red Hat Certified Operator catalog (available in disconnected environments via a mirror):

| Operator | Version | Namespace | Purpose | CRDs Used |
|---|---|---|---|---|
| Red Hat AMQ Streams | 2.7.x | `fluxot-messaging` | Kafka cluster lifecycle | `Kafka`, `KafkaTopic`, `KafkaUser`, `KafkaMirrorMaker2` |
| Red Hat AMQ Broker | 7.12.x | `fluxot-messaging` | ActiveMQ Artemis (MQTT/AMQP) | `ActiveMQArtemis`, `ActiveMQArtemisAddress` |
| Apache Camel K | 2.3.x | `fluxot-platform` | Integration routes | `Integration`, `IntegrationPlatform`, `KameletBinding` |
| Grafana Operator | 5.x | `fluxot-monitoring` | Grafana instance + dashboards | `Grafana`, `GrafanaDashboard`, `GrafanaDataSource` |
| cert-manager | 1.14.x | `cert-manager` (cluster-scoped) | Certificate lifecycle | `Certificate`, `ClusterIssuer`, `CertificateRequest` |
| Sealed Secrets | 0.26.x | `kube-system` | GitOps-safe encrypted secrets | `SealedSecret` |
| OpenShift Logging | 5.9.x | `openshift-logging` | Centralized log aggregation | `ClusterLogging`, `ClusterLogForwarder` |
| RHACS (optional) | 4.4.x | `rhacs-operator` | Advanced container security | `Central`, `SecuredCluster` |
