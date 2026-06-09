# Flux OT Platform — Security Architecture

**Version:** 1.0  
**Classification:** Confidential — Implementation Partners Only  
**Standard:** IEC 62443 Security Level 2 (SL2)

---

## Table of Contents

1. [Threat Model](#1-threat-model)
2. [Defense in Depth](#2-defense-in-depth)
3. [IT/OT Segmentation](#3-itot-segmentation)
4. [Network Security](#4-network-security)
5. [Identity and Access](#5-identity-and-access)
6. [Certificate Management](#6-certificate-management)
7. [Secret Management](#7-secret-management)
8. [Audit Logging](#8-audit-logging)
9. [Incident Response](#9-incident-response)
10. [Compliance Mapping](#10-compliance-mapping)

---

## 1. Threat Model

### Protected Assets

| Asset | Criticality | Value |
|---|---|---|
| Belt conveyor control (emergency stop) | CRITICAL | Worker safety; belt replacement cost AUD $200K–$2M |
| Road crossing barrier control | CRITICAL | Worker safety; vehicle/pedestrian lives |
| Telemetry data | HIGH | Production monitoring; regulatory compliance |
| Platform API credentials | HIGH | Gateway to all control functions |
| OPC-UA server on PLCs | CRITICAL | Direct control of physical equipment |
| Site network infrastructure | HIGH | Connectivity of all OT systems |
| AI models | MEDIUM | Proprietary IP; manipulation could degrade safety |
| TimescaleDB (production data) | HIGH | 7-year audit trail; business intelligence |

### Threat Actors

| Actor | Motivation | Capability | Likelihood |
|---|---|---|---|
| **Disgruntled insider** | Sabotage, financial gain | High (system access) | Medium |
| **External criminal** | Ransomware, extortion | Medium (targeted) | Medium-High |
| **Competitor/Industrial espionage** | Process data theft | Medium | Low-Medium |
| **Nation-state (critical infrastructure)** | Disruption, espionage | Very High | Low (but high impact) |
| **Unsophisticated opportunist** | Ransomware spray | Low | High |
| **Supply chain attacker** | Trojanised software/hardware | High (trusted position) | Low |

### Attack Vectors

| Vector | Mitigation |
|---|---|
| Compromised operator workstation | MFA, endpoint detection, network segmentation |
| Phishing/credential theft | MFA, JWT short expiry, credential rotation |
| VPN/remote access abuse | Certificate-based VPN, jump host, session recording |
| USB/physical media | Physical port blocking, USB whitelisting |
| Rogue MQTT device | mTLS certificate requirement on all MQTT connections |
| Kafka consumer injection | SCRAM-SHA-512 auth, topic ACLs, consumer group restriction |
| OPC-UA session hijacking | Mutual certificate auth, SignAndEncrypt policy |
| Container escape | OpenShift SCC, seccomp profiles, read-only filesystems |
| Kubernetes API abuse | RBAC, audit logging, no cluster-admin for workload SAs |
| Supply chain (base images) | Red Hat UBI base images, image signing, vulnerability scanning |

---

## 2. Defense in Depth

### Layer 1: Physical Security

- Edge IPC enclosures: **IP65** minimum, keyed locks, tamper-evident seals
- Tamper switch connected to PLC digital input — generates CRITICAL alert on opening
- All USB/serial ports blanked in production; boot device locked to internal SSD
- Server room / plant room physical access control (card reader, CCTV)
- Hardware Security Module (HSM) for long-term key storage (optional, recommended for flagship sites)

### Layer 2: Network Security

- Dedicated OT network (separate VLAN/physical switches) — no internet access
- Industrial DMZ: one firewall segment between OT and IT networks
- Firewall default-deny both directions; explicit allow rules only
- All traffic between zones encrypted (TLS 1.3 minimum)
- Micro-segmentation within OpenShift via NetworkPolicies (Layer 3/4)

### Layer 3: Host Security

- **Edge:** RHEL 9 with SELinux enforcing, CIS Level 1 benchmark, auditd
- **Platform:** Red Hat CoreOS (RHCOS) — immutable OS, no SSH in production, MachineConfig for hardening
- **Container:** Read-only root filesystem where possible; no privilege escalation; seccomp/AppArmor profiles
- Vulnerability scanning: Red Hat RHACS (Advanced Cluster Security) scans all container images

### Layer 4: Application Security

- JWT authentication for all API access; tokens expire after 8 hours
- MQTT mTLS — no anonymous connections accepted
- OPC-UA SignAndEncrypt policy in production
- Kafka SCRAM-SHA-512 per-service credentials
- Input validation on all API endpoints (Pydantic v2 strict mode)
- Rate limiting to prevent credential brute-force

### Layer 5: Data Security

- **At rest:** AES-256 encryption for TimescaleDB tablespace (LUKS at OS layer or cloud provider managed)
- **In transit:** TLS 1.3 for all network communication; TLS 1.2 minimum
- **In processing:** No sensitive data in logs (redacted by structured logging)
- **Backup:** Encrypted backups with key managed separately from data

---

## 3. IT/OT Segmentation

### Zone/Conduit Model (IEC 62443-3-2)

```
┌─────────────────────────────────────────────────────────────────────┐
│ ZONE: Enterprise IT                                                   │
│  Workstations, ERP, Business Intelligence                             │
│  Access: Grafana dashboards, API (read-only for Viewer role)         │
├─────────────────────────────── FW-IT ───────────────────────────────┤
│ ZONE: Platform / Level 3-4                                           │
│  OpenShift: API, Kafka, TimescaleDB, AI, Grafana                    │
│  Access: Platform API REST/WebSocket                                 │
│  Conduit to DMZ: MQTT over TLS 1.3 (AMQ Broker → Mosquitto)        │
├─────────────────────────────── FW-DMZ ──────────────────────────────┤
│ ZONE: Industrial DMZ                                                  │
│  Mosquitto MQTT Broker, Certificate Authority, Jump Host            │
│  Conduit to OT: MQTT TLS+mTLS (Mosquitto → Edge skid)              │
├─────────────────────────────── FW-OT ───────────────────────────────┤
│ ZONE: Level 2 Edge                                                   │
│  K3s edge clusters, Python services                                  │
│  Conduit to Level 1: OPC-UA (TCP 4840), Modbus TCP (TCP 502)       │
├─────────────────────────────── OT SWITCH ───────────────────────────┤
│ ZONE: Level 0-1 OT                                                   │
│  PLCs, DCS, Field instruments                                        │
│  No internet access; no IT network access                           │
└─────────────────────────────────────────────────────────────────────┘
```

### Firewall Rules (FW-OT — between Level 2 Edge and Level 1 OT)

| Source | Destination | Port | Protocol | Direction | Purpose |
|---|---|---|---|---|---|
| Edge IPC | PLC OPC-UA server | TCP 4840 | OPC-UA | Outbound | Telemetry read + writeback |
| Edge IPC | Modbus device | TCP 502 | Modbus TCP | Outbound | Modbus read/write |
| DENY | ALL | ANY | ANY | ANY | Default deny |

### Firewall Rules (FW-DMZ — between DMZ and Level 2 Edge)

| Source | Destination | Port | Protocol | Direction | Purpose |
|---|---|---|---|---|---|
| DMZ Mosquitto | Edge Mosquitto | TCP 8883 | MQTT TLS | Outbound | Command delivery to edge |
| Edge Mosquitto | DMZ Mosquitto | TCP 8883 | MQTT TLS | Inbound | Telemetry from edge |
| Jump Host | Edge IPC | TCP 22 | SSH (key-based) | Outbound | Maintenance access |
| DENY | ALL | ANY | ANY | ANY | Default deny |

---

## 4. Network Security

### OpenShift NetworkPolicies

Flux OT deploys NetworkPolicies following a default-deny posture with explicit allow rules:

```yaml
# Default deny-all in each namespace
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: fluxot-platform
spec:
  podSelector: {}  # Matches all pods
  policyTypes: [Ingress, Egress]
  # No ingress/egress rules = deny all

---
# Allow: Platform API → TimescaleDB
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-api-to-timescale
  namespace: fluxot-platform
spec:
  podSelector:
    matchLabels:
      app: timescaledb
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: platform-api
      ports:
        - port: 5432

---
# Allow: Platform API → Kafka (in fluxot-messaging namespace)
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-api-to-kafka
  namespace: fluxot-messaging
spec:
  podSelector:
    matchLabels:
      strimzi.io/name: fluxot-kafka-kafka
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: fluxot-platform
      ports:
        - port: 9093  # TLS listener only
```

### Service Mesh (Optional - Recommended for Enterprise)

For deployments requiring mTLS between all internal services (Zero Trust within the cluster), OpenShift Service Mesh (Istio) can be deployed:

```yaml
# Enables automatic mTLS for all traffic in fluxot-platform namespace
apiVersion: security.istio.io/v1beta1
kind: PeerAuthentication
metadata:
  name: default
  namespace: fluxot-platform
spec:
  mtls:
    mode: STRICT
```

---

## 5. Identity and Access

### JWT Token Architecture

API access tokens use HS256 with a 256-bit random secret key rotated quarterly:

```python
# Token claims
{
  "sub":  "operator_jane",          # Username (subject)
  "role": "OPERATOR",               # User role
  "iat":  1700000000,               # Issued at (Unix timestamp)
  "exp":  1700028800,               # Expires at (iat + 8 hours)
  "jti":  "f47ac10b-58cc-4372..."   # Unique token ID (for revocation)
}
```

**Token revocation:** JWT tokens are stateless and cannot be revoked before expiry without a denylist. Flux OT uses a Redis denylist for immediate revocation (e.g., on user disable or suspected compromise).

### MQTT Client Certificate Authentication

Each edge skid has a unique client certificate:

```
Certificate Structure:
  Subject:     CN={SITE_ID}-{SKID_ID}, O=FluxOT, OU=Edge
  Issuer:      CN=FluxOT Edge CA, O=FluxOT
  Key Usage:   Digital Signature, Key Encipherment
  EKU:         TLS Web Client Authentication
  SANs:        {SKID_ID}.edge.fluxot.internal
  Validity:    1 year
```

MQTT broker ACL rules enforce that each client can only publish to their own site/skid topic namespace:
```
# Mosquitto ACL file
# Client CN: SITE_KALGOORLIE_01-SKID_CV001
topic write fluxot/SITE_KALGOORLIE_01/+/SKID_CV001/#
topic read  fluxot/SITE_KALGOORLIE_01/+/SKID_CV001/commands
topic read  fluxot/SITE_KALGOORLIE_01/+/SKID_CV001/#
```

### Kafka SCRAM-SHA-512

Each service has a dedicated Kafka user with minimal ACLs:

```yaml
# Platform API consumer user
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: platform-api-consumer
  namespace: fluxot-messaging
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource: {type: topic, name: "fluxot.telemetry.", patternType: prefix}
        operation: Read
      - resource: {type: group, name: fluxot-platform-api}
        operation: Read
```

### RBAC: Role Definitions

| Role | Permissions |
|---|---|
| **Viewer** | Read all telemetry, alerts, status, and history. No write access. |
| **Operator** | Viewer + acknowledge alerts, issue commands, initiate writebacks, set maintenance mode. |
| **Admin** | Operator + create/edit sites and skids, manage users, view audit logs, access API documentation. |
| **SystemAdmin** | Admin + manage API keys, certificate rotation, Kafka admin, TimescaleDB access. Not a regular role. |

---

## 6. Certificate Management

### CA Hierarchy

```
FluxOT Root CA (offline, air-gapped)
├── FluxOT Platform CA (online, cert-manager managed)
│   ├── platform-api TLS certificate
│   ├── timescaledb server certificate
│   ├── kafka broker certificates
│   └── grafana server certificate
└── FluxOT Edge CA (online, limited exposure)
    ├── SITE_001-SKID_CV001 client cert (edge MQTT)
    ├── SITE_001-SKID_CV001 OPC-UA app cert
    └── (one cert pair per edge skid)
```

### cert-manager Configuration

```yaml
# Platform CA ClusterIssuer
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: fluxot-platform-ca
spec:
  ca:
    secretName: fluxot-platform-ca-keypair  # CA cert+key stored as Secret

---
# Certificate for Platform API
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: platform-api-tls
  namespace: fluxot-platform
spec:
  secretName: platform-api-tls-secret
  issuerRef:
    name: fluxot-platform-ca
    kind: ClusterIssuer
  commonName: platform-api.fluxot.svc
  dnsNames:
    - platform-api.fluxot.svc.cluster.local
    - platform-api.fluxot-platform.svc
  duration: 8760h   # 1 year
  renewBefore: 720h # Renew 30 days before expiry
```

### Certificate Rotation

| Certificate Type | Validity | Renewal | Method |
|---|---|---|---|
| Root CA | 10 years | Manual (planned, offline operation) | Manual re-sign |
| Platform CA | 5 years | Manual + 30 days lead time | Manual re-sign |
| Service TLS certs | 1 year | Automatic (cert-manager) | cert-manager auto-renew |
| Edge MQTT client certs | 1 year | Semi-automatic (deployment script) | Re-deploy K8s Secret |
| OPC-UA application certs | 1 year | Manual | OPC-UA trust list update required |
| JWT signing secret | Quarterly | Manual (key rotation) | Secret update + pod restart |

---

## 7. Secret Management

### SealedSecrets (Default)

SealedSecrets from Bitnami allows storing encrypted secrets in Git:

```bash
# Seal a secret for GitOps
kubectl create secret generic kafka-credentials \
  --from-literal=username=platform-api-consumer \
  --from-literal=password='<generated>' \
  --dry-run=client -o yaml | kubeseal -o yaml > deploy/secrets/kafka-credentials-sealed.yaml

# Commit the SealedSecret (safe to store in Git)
git add deploy/secrets/kafka-credentials-sealed.yaml
```

The SealedSecrets controller decrypts them in-cluster using its private key (stored in the cluster, never in Git).

### HashiCorp Vault (Enterprise Option)

For enterprise deployments requiring centralized secret management, HSM integration, and dynamic credentials:

```yaml
# Vault Agent Injector annotation
vault.hashicorp.com/agent-inject: "true"
vault.hashicorp.com/agent-inject-secret-db-creds: "database/creds/platform-api"
vault.hashicorp.com/role: "platform-api"
```

Vault dynamic database credentials provide:
- Time-limited credentials (TTL 1 hour)
- Automatic rotation without pod restart
- Full audit trail of credential issuance

---

## 8. Audit Logging

### What is Audited

All of the following events are captured in the audit log:

| Event Category | Log Destination | Retention |
|---|---|---|
| API authentication (login, token refresh, logout) | OpenShift Audit + TimescaleDB `auth_audit` | 5 years |
| API authorization failures | OpenShift Audit + Prometheus metric | 5 years |
| Command issuance (operator sends command) | TimescaleDB `command_records` | 5 years |
| Writeback execution (OPC-UA/Modbus write) | TimescaleDB `writeback_audit` | 7 years |
| Alert acknowledgement | TimescaleDB `alert_records` | 5 years |
| User management changes | OpenShift Audit | 5 years |
| Certificate operations | cert-manager events + OpenShift Audit | 5 years |
| Kubernetes API access | OpenShift Audit (all API server requests) | 90 days (hot) + 5 years (cold) |

### Tamper-Evident Audit Trail

The `writeback_audit` table is append-only — no UPDATE or DELETE grants are given to any application role:

```sql
-- Restrict permissions on writeback_audit
REVOKE UPDATE, DELETE ON writeback_audit FROM platform_api_role;
GRANT INSERT, SELECT ON writeback_audit TO platform_api_role;

-- Optionally: implement row-level hash chain for tamper detection
ALTER TABLE writeback_audit ADD COLUMN row_hash TEXT;
-- Trigger computes SHA-256(prev_hash || timestamp || command_id || value_json)
```

### Structured Logging

All Flux OT services use structured JSON logging with consistent fields:

```json
{
  "timestamp": "2024-11-15T03:43:20.055Z",
  "level": "INFO",
  "logger": "edge.belt_rip_monitor",
  "message": "OPC-UA write_node completed",
  "site_id": "SITE_KALGOORLIE_01",
  "skid_id": "SKID_CV001",
  "node_id": "ns=2;s=BeltControl.EmergencyStop",
  "value": true,
  "command_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "latency_ms": 23
}
```

Log shipping to OpenShift Logging (Loki) via the OpenShift Logging operator. Grafana Loki datasource provides log querying from dashboards.

---

## 9. Incident Response

### Cyber Incident Playbook for OT Environments

**CRITICAL PRIORITY: Worker safety overrides all other response actions.**

#### Step 1: Initial Detection and Triage (0–15 minutes)

1. Confirm the incident is genuine (not a false positive from monitoring)
2. Notify: Site Superintendent → Control Room Supervisor → IT Security Team
3. **Isolate affected systems without disrupting running plant** (consult with control room)
4. Preserve evidence: capture logs, do not reboot affected systems

#### Step 2: Immediate Containment (15–60 minutes)

1. If an edge skid is compromised: physically disconnect network cable (belt detection still works locally via hardwired safety PLC logic)
2. If platform is compromised: activate maintenance mode on all skids (prevents remote commands); revert to local PLC control
3. Revoke compromised credentials: JWT signing key rotation, MQTT certificate revocation
4. Block suspicious IP addresses at firewall

#### Step 3: Eradication (1–24 hours)

1. Forensic analysis: OpenShift audit logs, network captures, system logs
2. Identify root cause and attack vector
3. Patch or remediate vulnerability
4. Re-image compromised systems (do not restore from potentially-compromised backup)

#### Step 4: Recovery (24–72 hours)

1. Deploy clean systems from known-good images
2. Re-issue certificates for all affected edge skids
3. Validate system integrity: checksums on deployment manifests, container image SHAs
4. Incremental reconnection with enhanced monitoring

#### Step 5: Post-Incident (1–4 weeks)

1. Root cause analysis report
2. Update threat model if new attack vector discovered
3. IEC 62443 risk reassessment
4. Regulatory notification if required (OAIC Data Breach Notification where personal data involved; ASIO/ASD for critical infrastructure attacks)

---

## 10. Compliance Mapping

### IEC 62443-3-3 Security Requirements — SL2 Mapping

| Requirement | Description | Flux OT Implementation |
|---|---|---|
| SR 1.1 | Human user identification and authentication | JWT tokens, bcrypt password hashing |
| SR 1.2 | Software process and device identification | MQTT client certificates, Kafka SCRAM, OPC-UA certs |
| SR 1.3 | Account management | User CRUD via Admin API; disabled flag |
| SR 1.4 | Identifier management | Unique username per user; unique CN per device cert |
| SR 1.5 | Authenticator management | Password policy; cert rotation procedures |
| SR 1.6 | Wireless access management | N/A (no wireless in OT zone) |
| SR 1.7 | Strength of password-based authentication | bcrypt cost factor 12; minimum complexity |
| SR 2.1 | Authorization enforcement | RBAC: Viewer/Operator/Admin; Kafka ACLs; MQTT ACLs |
| SR 2.4 | Mobile code | Not used |
| SR 2.6 | Remote session termination | JWT expiry; WebSocket timeout |
| SR 2.8 | Auditable events | All authentication, commands, writeback events logged |
| SR 2.9 | Audit storage capacity | 5–7 year retention in TimescaleDB + cold storage |
| SR 2.10 | Response to audit processing failures | Alertmanager rule on audit log write failure |
| SR 2.11 | Timestamps | All events UTC-stamped; NTP synchronised |
| SR 3.1 | Communications integrity | TLS 1.3 for all communications; OPC-UA SignAndEncrypt |
| SR 3.2 | Protection from malicious code | Red Hat UBI images; RHACS image scanning; read-only FS |
| SR 3.3 | Security functionality verification | Automated tests; GitOps immutable deployments |
| SR 3.5 | Input validation | Pydantic strict validation on all API inputs |
| SR 4.1 | Information confidentiality | TLS in transit; AES-256 at rest |
| SR 4.2 | Information persistence | Append-only audit tables; backup with encryption |
| SR 5.1 | Network segmentation | Purdue model zones; OpenShift NetworkPolicies |
| SR 5.2 | Zone boundary protection | Firewalls FW-OT, FW-DMZ, FW-IT |
| SR 5.4 | Application partitioning | Separate namespaces; NetworkPolicy isolation |
| SR 6.1 | Availability | HA for all components; RTO < 5 min |
| SR 6.2 | Continuous monitoring | Prometheus + Alertmanager; Grafana dashboards |
| SR 7.1 | Denial of service protection | Rate limiting; HPA; resource quotas |
| SR 7.2 | Resource management | OpenShift LimitRange + ResourceQuota |
| SR 7.3 | Control system backup | TimescaleDB WAL + daily pg_dump; GitOps for configs |
| SR 7.4 | Control system recovery | RTO/RPO targets documented; tested quarterly |
| SR 7.5 | Emergency power | UPS at edge sites; generator for data centre |
| SR 7.6 | Network and security configuration settings | GitOps (ArgoCD or Flux); all config in Git |
| SR 7.7 | Least functionality | Minimal base images; only required ports open |
| SR 7.8 | Control system component inventory | RHACS + OpenShift console provide full asset inventory |
