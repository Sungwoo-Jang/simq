# M6 security operations

M6 security is opt-in. `SIMQ_SECURITY_MODE=disabled` retains the M1-M5 local
compatibility behavior. `SIMQ_SECURITY_MODE=oidc` enables HTTPS, verified OIDC
identity, deny-by-default roles, tenant namespaces, encrypted payload storage,
quotas, rate limiting, metrics, and audit records. An invalid secure
configuration stops startup; it never falls back to disabled mode.

## Required secure-mode configuration

- `SIMQ_OIDC_ISSUER`: exact HTTPS issuer used for discovery and `iss` checks.
- `SIMQ_OIDC_AUDIENCE`: required token audience.
- `SIMQ_OIDC_TENANT_CLAIM`: tenant claim; default `simq_tenant`.
- `SIMQ_OIDC_ROLES_CLAIM`: string or string-array roles claim; default
  `simq_roles`.
- `SIMQ_TLS_CERT_FILE` and `SIMQ_TLS_KEY_FILE`: API server identity. Secure mode
  serves TLS 1.3 directly.
- `SIMQ_ENCRYPTION_KEYS`: comma-separated `key-id=base64-32-byte-key` entries.
- `SIMQ_ENCRYPTION_ACTIVE_KEY`: configured key ID used for new messages.
- `SIMQ_LEGACY_TENANT`: verified tenant ID that owns the unprefixed pre-M6
  namespace.
- `SIMQ_METRICS_TOKEN`: independent bearer credential for `GET /metrics`.
- `SIMQ_AUDIT_PATH`: dedicated regular JSONL file; every append is synchronized.

Secure mode requires bbolt. Keep TLS private keys, encryption keys, OIDC client
trust, metrics credentials, and cluster administration credentials in the
deployment secret manager; do not put them in command lines, logs, or images.

## Roles

- `simq.admin`: every queue action, including configuration and redrive.
- `simq.reader`: get/list actions.
- `simq.producer`: single and batch sends.
- `simq.consumer`: receive, delete, and visibility actions.

Unknown actions and missing roles are denied. M2 permission statements remain
compatibility metadata and grant no access.

## Tenant limits

The defaults are 10,000 queues, 1,000,000 active messages, 10 GiB of stored
payload, 100 requests/second, and a burst of 200. Override them with:

- `SIMQ_TENANT_MAX_QUEUES`
- `SIMQ_TENANT_MAX_MESSAGES`
- `SIMQ_TENANT_MAX_PAYLOAD_BYTES`
- `SIMQ_TENANT_REQUESTS_PER_SECOND`
- `SIMQ_TENANT_REQUEST_BURST`

Stored quotas are authoritative transaction state. In a cluster, the leader
places its quota values in each replicated command so every FSM applies the
same admission decision. Rate buckets are intentionally transient and reset on
leader change. Quota rejection is HTTP 429; batch sends account independently.

## Legacy enablement and key rotation

Schema v8 creates and validates tenant usage counters after preserving the
schema-v7 backup. The configured legacy tenant owns existing unprefixed queue
metadata. Reserved `tn_<sha256>_` names are hidden and rejected for that tenant,
so it cannot address another tenant's internal namespace.

Pre-M6 payloads are plaintext and therefore deliberately unreadable in secure
mode. Drain or purge them before enabling OIDC mode; a remaining plaintext
message fails closed as repository corruption instead of being returned. Keep
the pre-enable database backup until the secure deployment is verified.

For key rotation, add the new key while retaining every old read key, restart
all nodes with the identical key ring, then select the new active key. New
writes use it immediately and older envelopes remain readable. Do not remove an
old key until all messages written with it have expired, been consumed, or
been purged. Database and Raft backups do not contain key material, so preserve
the matching external key ring for recovery.

## Metrics, audit, and trace correlation

`GET /metrics` requires `Authorization: Bearer <SIMQ_METRICS_TOKEN>` and emits
Prometheus text exposition. Tenant labels are bounded hashes; actions and
results are fixed enums. Audit JSONL contains time, request ID, accepted W3C
`traceparent` trace ID, issuer-subject hash, tenant hash, action endpoint, and
result. It never includes tokens, queue request bodies, message data,
attributes, receipts, ciphertext, tenant strings, or key material.

Restrict audit-file access and ship it with an agent that preserves JSONL
record boundaries. Monitor `/readyz`, HTTP error rates, rate/quota rejections,
audit-sink filesystem capacity, Raft quorum, and certificate/key expiry.

## Optional Raft mutual TLS

Set the following on every node to authenticate and encrypt Raft TCP traffic:

- `SIMQ_CLUSTER_TLS_CERT_FILE`
- `SIMQ_CLUSTER_TLS_KEY_FILE`
- `SIMQ_CLUSTER_TLS_CA_FILE`
- optional `SIMQ_CLUSTER_TLS_SERVER_NAME` when all node certificates share a
  dedicated DNS identity; otherwise the advertised host is verified.

Certificate, key, and CA must be configured together. Certificates need both
server-auth and client-auth usage and must chain to the configured CA. Raft TLS
uses TLS 1.3. Roll trust by distributing overlapping CA/certificate material
before removing the old trust anchor.
