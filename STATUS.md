# SimQ status dashboard

Last verified: 2026-08-29 with Go 1.26.7 and `./scripts/verify.ps1`

Status legend: **complete**, **in progress**, **planned**, **out of scope**.
Only behavior backed by automated evidence may be marked complete.

## Executive status

| Dimension | Status | Current boundary |
|---|---|---|
| Queue model | complete through M9 | Standard and FIFO semantics preserved on standalone, one Raft group, or an explicitly owned elastic tenant shard |
| Public actions | complete through M4 | All 23 planned SQS-named JSON actions plus FIFO fields are implemented |
| SQS semantic portability | partial PoC evidence | One shared six-scenario runner passes on local three-node SimQ; the guarded Amazon SQS path is compiled and unit-tested but awaits an authorized account run |
| Durability | complete through M9 | Schema-v11 topology catalog, ownership epochs, operation cursors, tombstones, and durable quorum commit per shard |
| High availability | complete per fixed shard | Three or more voters tolerate one-node failure; placement rejects a single-domain quorum |
| Multi-tenancy | complete for elastic topology | Verified tenant digest has one explicit epoch-owned, non-aliasing shard namespace |
| Authentication and authorization | complete | Strict OIDC RS256 verification and fixed deny-by-default SimQ roles |
| Encryption | complete for payloads and transport | AES-256-GCM message envelopes, TLS 1.3 API, and optional Raft mTLS |
| Operations | complete for M9 boundary | Resumable backfill, activation, drain/retirement, tenant relocation, abort, leader hints, and recovery runbooks |

## API coverage

| Area | Action or behavior | Status | Milestone |
|---|---|---|---|
| Queues | `CreateQueue` | complete | M0 / M1-A durable |
| Queues | `GetQueueUrl` | complete | M0 / M1-A durable |
| Messages | `SendMessage` | complete | M0 / M1-A durable |
| Messages | `ReceiveMessage` short polling, up to 10 | complete | M0 / M1-A durable |
| Messages | `DeleteMessage` | complete | M0 / M1-A durable |
| Messages | `ChangeMessageVisibility` | complete | M1-B |
| Lifecycle | Queue and per-message delivery delay | complete | M1-C |
| Lifecycle | Message retention and cleanup | complete | M1-C |
| Payload | Message attributes and receive projection | complete | M1-D |
| Queues | `GetQueueAttributes` and `SetQueueAttributes` | complete | M1-D |
| Polling | Request-level long polling with cancellation and lifecycle wake-ups | complete | M1-E |
| Messages | Send/delete/visibility batch actions | complete | M1-F |
| Administration | `ListQueues`, `DeleteQueue`, and `PurgeQueue` | complete | M2 |
| Administration | `TagQueue`, `UntagQueue`, and `ListQueueTags` | complete | M2 |
| Administration | `AddPermission` and `RemovePermission` compatibility metadata | complete | M2; no authorization effect |
| Failure handling | Dead-letter queues and redrive | complete | M3 |
| Queue model | FIFO groups, ordering, deduplication, and receive-attempt replay | complete | M4 |
| Compatibility | AWS SDK, CLI, Query protocol, and SigV4 | out of scope | SimQ uses its own JSON HTTP contract |

## Correctness and storage

| Capability | Status | Evidence or limitation |
|---|---|---|
| Atomic receive claim | complete | Selection, receipt generation, count, generation, and deadline change together |
| Receipt generation safety | complete | New handle per receive; stale handles cannot mutate a newer generation |
| Visibility boundary | complete | Injected-clock tests cover the exact deadline |
| Delivery-delay boundary | complete | Queue default, message override, bypass scan, and exact availability are tested on both repositories |
| Retention boundary | complete | Delayed, visible, and in-flight messages become terminal exactly at expiration |
| Durable mutation boundary | complete | HTTP success follows a synchronized bbolt transaction |
| Restart recovery | complete | Queue settings, message attributes/digests, lifecycle, claims, receipts, and delete recover |
| Queue-generation isolation | complete | Generation-bound URLs and repository guards reject stale URLs and receipts after recreation |
| Administrative transactions | complete | Delete, purge, tags, and permission statements commit atomically and remain queue-scoped |
| Stable pagination | complete | Lexical pages bind prefix, cursor, and namespace revision; namespace changes fail closed |
| Dead-letter transfer | complete | Receive threshold, cross-queue transfer, original-source metadata, and receipt safety commit atomically |
| Message move tasks | complete | Durable boundary, progress, cancellation, failure state, rate worker, and restart resume |
| FIFO group ordering | complete | Delayed or in-flight group heads block later messages while independent groups progress |
| FIFO send deduplication | complete | Five-minute generation-scoped records survive deletion, purge, and restart; concurrent duplicates commit once |
| FIFO receive attempts | complete | Durable five-minute receipt snapshots replay without reclaiming, including long-poll completion semantics |
| FIFO transfers | complete | DLQ and move transitions preserve group metadata and atomically resequence in the destination generation |
| Process-crash recovery | complete | Unix SIGKILL and Windows test-child forced exit reopen the same bbolt file; batch send, visibility, and delete are covered |
| Fail-closed corruption policy | complete | Invalid schema or records cause explicit failure |
| Schema migration | complete for v1/v2/v3/v4/v5/v6/v7/v8/v9/v10→v11 | Sequential startup migration preserves validated `0600` backups for every source version |
| Backup and restore | complete for per-shard clustered snapshots | Complete schema-v11 FSM snapshots include relocation and topology state and validate before atomic publication; standalone export tooling remains deferred |
| Online migration and compaction | partial | Schema migration is offline; Raft snapshots compact replicated logs automatically |
| Replication and consensus | complete for reviewed elastic multi-Raft groups | Independent durable logs, quorums, leader reads, failover, minority fencing, and resumable cross-group relocation |

## Security and isolation

| Capability | Status | Required future decision |
|---|---|---|
| Tenant isolation | complete | Verified tenant scopes queues, messages, receipts, transfers, task handles, quotas, and metrics |
| OIDC authentication | complete | Exact issuer/audience discovery, RS256 JWKS validation, claim checks, and rotation |
| Authorization | complete | Fixed roles with deny-by-default action mapping; M2 metadata grants nothing |
| TLS | complete | Secure API TLS 1.3 and optional mutually authenticated Raft TLS 1.3 |
| Encryption at rest | complete for payloads | AES-256-GCM versioned envelopes and active/old-key rotation |
| Secret management | deployment boundary | Secrets are supplied externally by environment/deployment secret injection |
| Audit events | complete | Hashed identity/tenant, action endpoint, result, request ID, and trace ID without payload data |
| Quotas and rate limiting | complete | Atomic stored usage plus leader-local bounded token buckets and stable 429 results |

Secure-mode deployment and key-rotation procedures are documented in
`docs/security-operations.md`. Pre-M6 plaintext messages must be drained or
purged before enabling OIDC mode and otherwise fail closed.

## Availability and operations

| Capability | Status | Current boundary |
|---|---|---|
| `/healthz` | complete | Process liveness only |
| `/readyz` | complete | Repository health and schema readiness |
| Request correlation | complete | Matching JSON and `X-SimQ-Request-Id` values |
| Graceful shutdown | complete | Long-poll waiters and HTTP plus retention sweeper stop before repository close |
| Metrics | complete for M6 request boundary | Bounded tenant-hash/action/result request count and cumulative latency exposition |
| Tracing | partial | Valid W3C trace IDs correlate audit events; span export remains deferred |
| Alerting | complete for local rule evaluation | Target, ready-leader, server-error, and latency rules pass deterministic promtool tests; notification routing remains staging evidence |
| Audit logging | complete | Synchronized redacted JSONL sink with hashed tenant and issuer-subject identity |
| Web/admin dashboard | planned | Build after authoritative metrics and access control exist |
| Horizontal scaling | complete for reviewed elastic tenant sharding | Candidate groups activate after explicit-directory backfill; selective nodes host planned subsets and drains move one tenant at a time |
| Automated failover | complete for M5 | Raft elects a new leader; followers return configured API leader hints |
| Multi-AZ durability | complete at placement-policy boundary | Initial and changed voter sets reject any one failure domain containing quorum; deployment supplies real domains |

## Delivery roadmap

| Milestone | Status | Scope |
|---|---|---|
| M0 | complete | In-memory Standard Queue core |
| M1-A | complete | Single-node durability and restart recovery |
| M1-B | complete | Durable `ChangeMessageVisibility` |
| M1-C | complete | Delivery delay, retention cleanup, and schema-v2 migration |
| M1-D | complete | Message and queue attributes plus schema-v3 migration |
| M1-E | complete | Request-level long polling with transient waiters and authoritative re-claim |
| M1-F | complete | Independent per-entry Send/Delete/ChangeVisibility results with commit-before-success |
| M2 | complete | Generation-safe queue administration, stable pagination, tags, and permission metadata |
| M3 | complete | Generation-bound DLQ policy, source listing, and durable message move tasks |
| M4 | complete | FIFO queue types, group ordering, deduplication, sequences, receive-attempt replay, and transfer safety |
| M5 | complete | Deterministic replication, durable quorum commit, failover, snapshots, membership, and strict leader routing |
| M6 | complete | Authentication, tenancy, encryption, quotas, rate limiting, metrics, audit, TLS, and product operations |
| M7 | complete | Fixed catalog, tenant rendezvous routing, independent Raft shards, safe placement, relocation, and per-shard recovery |
| M8 | complete | Epoch-fenced tenant relocation, bounded verified transfer, resumable cutover/abort, and leader-failure recovery |
| M9 | complete | Explicit tenant directory, manifest-v2 candidate universe, selective hosting, activation, sequential drain, retirement, and permanent tombstones |
| M10 | complete with host limitation | CI, secure multi-shard failure injection, offline recovery, and tested SLO alerts pass; kind artifacts are complete but this workstation's Docker cgroup v1 cannot run nested kubelet cgroups |
| M11 | complete | Repeated scheduled secure/DR qualification, compatible-runner kind/PVC evidence, and secret-gated 14-day diagnostics pass on reviewed commit `2ef7b45` |

The separate SQS semantics PoC does not change the milestone or public API
boundary. It compares worker-visible acknowledgement, redelivery, visibility,
FIFO, deduplication, and DLQ behavior through backend adapters. It does not
claim AWS SDK, CLI, Query protocol, or SigV4 compatibility, and no authorized
Amazon SQS execution has been recorded yet.

## Update rules

- Update this file only after the corresponding acceptance evidence passes.
- Keep `SPEC.md` authoritative for public behavior and `INVARIANTS.md`
  authoritative for safety constraints.
- Keep detailed implementation checklists in `TASKS.md` and milestone sequencing
  in `M1_PLAN.md`.
- A green `./scripts/verify.sh` (or equivalent `./scripts/verify.ps1` on a
  Windows-only host) is necessary but does not by itself prove high
  availability, security, or AWS protocol compatibility.
