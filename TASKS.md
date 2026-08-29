# SimQ Tasks

## M0: In-memory Standard Queue

- [x] `CreateQueue`
- [x] `GetQueueUrl`
- [x] `SendMessage`
- [x] `ReceiveMessage`
- [x] `DeleteMessage`

## M0 completion gate

- [x] Send, receive, and delete through the public HTTP API.
- [x] Atomic receive claim under concurrent consumers.
- [x] Visibility expiry using an injected clock.
- [x] New receipt handle for every receive generation.
- [x] Stale receipt handles cannot delete a newer claim.
- [x] `./scripts/verify.sh` passes for the completed API stages.
- [x] Actual HTTP smoke test passes for `CreateQueue` → `SendMessage` → `ReceiveMessage` → `DeleteMessage` → empty receive.

Only the current stage may be implemented. Update this file after its acceptance
tests and full verification pass.

## M1-A: Single-node durability and restart recovery

- [x] Dependency policy updated and bbolt decision recorded in ADR 0001.
- [x] Versioned on-disk schema and fail-closed validation implemented.
- [x] Repository errors, health, and close are explicit at the interface boundary.
- [x] Shared repository conformance suite passes for memory and bbolt.
- [x] Queue creation and sent messages survive close/reopen.
- [x] Receive metadata, visibility boundary, and receipt history survive close/reopen.
- [x] Current delete is durable; stale receipts remain harmless after reopen.
- [x] Callback/commit/open failures cannot become HTTP success.
- [x] Unknown schema and corrupt records cause startup/readiness failure.
- [x] File-lock timeout, invalid path, and permission behavior are tested.
- [x] SIGKILL HTTP restart smoke test passes with the same data file.
- [x] Go 1.26.7 test, race, vet, build, and `./scripts/verify.sh` all pass.

## M1-B: ChangeMessageVisibility

- [x] Public API, strict JSON, deadline, receipt, and error contracts are recorded in `SPEC.md`.
- [x] M1-B invariant evidence and acceptance criteria are mapped in `INVARIANTS.md` and `M1_PLAN.md`.
- [x] Service validates required input and obtains exactly one command timestamp.
- [x] Memory and bbolt repositories change only the current generation deadline atomically.
- [x] Shared conformance tests cover deadline changes and receipt safety on both repositories.
- [x] Receive/change and delete/change concurrency tests pass on both repositories.
- [x] Changed deadlines and metadata survive close/reopen without message resurrection.
- [x] Repository update failure rolls back and becomes HTTP 503 without sensitive-data exposure.
- [x] Strict HTTP contract tests pass and `ChangeMessageVisibilityBatch` remains HTTP 501.
- [x] SIGKILL HTTP restart smoke covers zero and extended visibility changes plus deletion.
- [x] Go 1.26.7 test, race, vet, build, and `./scripts/verify.sh` all pass.

## M1-C: Delivery delay and message retention

- [x] Public lifecycle, exact-deadline, cleanup, and migration contracts are recorded in `SPEC.md`.
- [x] M1-C invariant evidence and acceptance criteria are mapped in `INVARIANTS.md` and `M1_PLAN.md`.
- [x] Schema-v2 and retention-cleanup decisions are recorded in ADR 0002.
- [x] Queue defaults and per-message `DelaySeconds` are validated by the service and strict HTTP decoder.
- [x] Memory and bbolt repositories skip delayed messages and expose them at the exact deadline.
- [x] Expired delayed, visible, and in-flight messages are terminal in both repositories.
- [x] Visibility changes cannot extend retention or resurrect an expired message.
- [x] An explicit timestamped expiration command and configurable local sweeper reclaim idle expired messages.
- [x] New durable stores use version 2; valid version 1 stores back up and migrate atomically.
- [x] Lifecycle settings, deadlines, and cleanup behavior survive close/reopen and process restart.
- [x] Shared conformance, strict HTTP, migration, rollback, and failure tests pass.
- [x] Go 1.26.7 test, race, vet, build, and `./scripts/verify.sh` all pass.

## M1-D: Message and queue attributes

- [x] Public message-attribute, receive-projection, and queue-attribute contracts are recorded in `SPEC.md`.
- [x] M1-D invariant evidence, acceptance criteria, atomic snapshot, and migration decisions are documented.
- [x] String, Number, Binary, custom type, count, syntax, base64, number, and exact total-size validation pass.
- [x] Canonical message-attribute digest and deep-copy immutability tests pass.
- [x] Receive exact/empty/`All` projection works without changing stored attributes or lifecycle state.
- [x] `GetQueueAttributes` and atomic partial `SetQueueAttributes` pass strict HTTP and domain tests.
- [x] Memory and bbolt enqueue/claim resolve queue defaults in the same atomic transition.
- [x] Set/Send and Set/Receive races yield complete old or new configuration snapshots in both repositories.
- [x] New stores use schema/record v3 with deterministic message-attribute records.
- [x] Valid v2 and v1 stores migrate fail-closed to v3 with distinct synchronized backups.
- [x] Attributes and changed queue settings survive close/reopen and SIGKILL without rewriting established deadlines.
- [x] Validation, repository, migration, and commit failures roll back and do not leak sensitive data.
- [x] Existing M0 through M1-C behavior and unimplemented M1-E/M1-F actions remain unchanged.
- [x] Go 1.26.7 test, race, vet, build, and `./scripts/verify.sh` all pass.

M1-D is complete.

## M1-E: Long polling

- [x] Public `WaitTimeSeconds` and short/long-poll contracts are recorded in `SPEC.md`.
- [x] Long-poll safety evidence and acceptance criteria are mapped in `INVARIANTS.md` and `M1_PLAN.md`.
- [x] Transient waiter, lifecycle hint, timer, cancellation, and shutdown decisions are recorded in ADR 0004.
- [x] Memory and bbolt return identical authoritative claim and next-transition semantics.
- [x] Claim/register/recheck prevents lost wake-ups; spurious wake-ups are harmless.
- [x] Send, delayed availability, and visibility expiry wake waiting receives without early delivery.
- [x] Retention expiration while waiting is terminal and queue notifications remain isolated.
- [x] Cancellation, timeout, and server shutdown release waiters without storage locks or mutation.
- [x] Multiple waiters preserve exclusive claims for one and several messages on both repositories.
- [x] Strict HTTP, storage failure, redaction, close/reopen, and SIGKILL recovery tests pass.
- [x] Go 1.26.7 test, race, vet, build, and `./scripts/verify.sh` all pass.

M1-E was completed before M1-F began.

## M1-F: Batch message actions

- [x] Official AWS batch semantics were reviewed and translated into SimQ's strict JSON contract.
- [x] Request-level validation, per-entry results, deterministic ordering, durability, and retry limits are documented.
- [x] ADR 0005 records independent per-entry commit orchestration and unchanged schema v3.
- [x] `SendMessageBatch` implements 1–10 entries, IDs, total size, delay, attributes, and ordered results.
- [x] `DeleteMessageBatch` preserves current, stale, consumed, unissued, malformed, and cross-queue receipt safety.
- [x] `ChangeMessageVisibilityBatch` preserves exact timestamped deadlines and receipt-generation safety.
- [x] Request-level errors apply no entries; partial and all-entry failures return complete redacted results.
- [x] Memory and bbolt conformance cover single/batch and concurrent-batch interactions.
- [x] Commit failure, close/reopen, and forced process-crash recovery cannot produce false success.
- [x] Strict HTTP, request-size, sensitive-data, and request-ID tests pass.
- [x] Go 1.26.7 gofmt, vet, test, race, build, and final verify pass.

M1-F is complete and preserved.

## M2: Queue control and administration

- [x] Public contracts, administrative errors, limits, and acceptance criteria are specified.
- [x] ADR 0006 records generation-bound URLs, transaction guards, revision-bound pagination, and metadata-only permissions.
- [x] Schema v4 adds queue identities, tombstones, tags, permissions, and namespace revision with validated schema-v3 backup migration.
- [x] Every public queue-scoped operation validates name and generation inside the authoritative repository boundary.
- [x] `ListQueues` provides lexical prefix filtering, 1–1,000 result pages, opaque cursors, and explicit revision invalidation.
- [x] `DeleteQueue` atomically removes a generation and owned state; recreation rejects stale URLs, aliases, and receipts.
- [x] `PurgeQueue` atomically removes messages and claim history while preserving configuration, identity, tags, permissions, and other queues.
- [x] Tag add/replace/remove/list validation and the 50-tag limit are atomic on memory and bbolt.
- [x] Permission statement replace/remove, 20-statement limit, deterministic `Policy`, and no-authorization semantics are covered.
- [x] Strict HTTP decoding, stable errors, request IDs, storage-error redaction, and all eight M2 endpoints are tested.
- [x] Conformance covers pagination invalidation, long-poll deletion wake-up, concurrent boundaries, restart recovery, migration, and injected commit rollback.
- [x] Go 1.26.7 gofmt, vet, forced process-crash recovery, test, race, build, and `scripts/verify.ps1` pass.

M2 is complete and preserved.

## M3: Dead-letter queues and redrive

- [x] `M3_PLAN.md`, `SPEC.md`, and ADR 0007 define queue ARNs, thresholds,
  payload metadata, task boundaries, cancellation, failure, and recovery.
- [x] Generation-bound `RedrivePolicy` validates live targets, count bounds,
  duplicate/unknown fields, self references, cycles, and stale generations.
- [x] `GetQueueAttributes` exposes canonical `QueueArn` and `RedrivePolicy`.
- [x] Receive-time source removal, DLQ insertion, and original-source metadata
  commit atomically while issued receipt history remains safe and monotonic.
- [x] `ListDeadLetterSourceQueues` provides lexical, revision-bound pagination.
- [x] Start/list/cancel message move actions enforce one RUNNING task per source,
  a durable high-water boundary, rate bounds, and observable terminal states.
- [x] Each moved message and progress increment share one transaction; injected
  bbolt failures roll both back.
- [x] Default tasks return messages to their recorded live original source;
  custom destinations and missing-origin failure are explicit.
- [x] RUNNING tasks survive close/reopen and the startup worker resumes them.
- [x] Schema v5 adds validated policy, origin, and task buckets; valid schema v4
  databases receive a synchronized `.schema-v4.bak` before migration.
- [x] Memory/bbolt conformance covers threshold behavior, payload preservation,
  stale receipts, concurrent exactly-once transfer, pagination invalidation,
  cycles, stale generations, cancellation, rollback, and recovery.
- [x] All four M3 HTTP actions, strict request shapes, stable errors, and request
  IDs are covered.
- [x] Go 1.26.7 gofmt, vet, forced process-crash recovery, test, race, build, and
  `scripts/verify.ps1` pass.

M3 is complete and preserved.

## M4: FIFO queues

- [x] `M4_PLAN.md`, `SPEC.md`, `INVARIANTS.md`, and ADR 0008 define strict
  group order and the deterministic M5 state-machine boundary.
- [x] FIFO queue names, immutable type, `FifoQueue`, and
  `ContentBasedDeduplication` contracts are enforced.
- [x] Group and deduplication IDs, content hashes, logical sequences, and exact
  five-minute send deduplication are atomic on memory and bbolt.
- [x] Delayed and in-flight group heads block later messages while independent
  groups can fill one receive batch.
- [x] Deduplication survives deletion and purge; logical sequences are not
  reused across purge, restart, redrive, or move transitions.
- [x] Durable `ReceiveRequestAttemptId` snapshots replay without new receipts or
  count increments, and long polls commit empty snapshots only on completion.
- [x] Redrive policies and move tasks preserve queue type, group metadata, and
  allocate a destination-generation sequence in the transfer commit.
- [x] Schema v6 adds validated FIFO side buckets and migrates valid schema v5
  databases after preserving a synchronized `.schema-v5.bak`.
- [x] Single and batch HTTP actions strictly decode FIFO fields and return
  decimal sequence numbers plus receive metadata.
- [x] Memory/bbolt/API tests cover concurrency, exact boundaries, purge,
  deletion, DLQ moves, restart, corruption, migration, and commit rollback.
- [x] Go 1.26.7 gofmt, vet, forced process-crash recovery, test, race, build, and
  `scripts/verify.ps1` pass.

M4 is complete and preserved.

## M5: clustered durability and availability

- [x] M5 architecture, quorum consistency, proposal identity, snapshot,
  membership, and upgrade boundaries are recorded in `M5_PLAN.md`, `SPEC.md`,
  `INVARIANTS.md`, and ADR 0009.
- [x] Schema v7 atomically stores a queue mutation, replayable proposal result,
  and applied Raft index; v6 migration keeps a validated `.schema-v6.bak`.
- [x] Versioned deterministic commands route every mutation through HashiCorp
  Raft v1.7.3 and leader reads through `VerifyLeader` plus `Barrier`.
- [x] Raft log and stable state use a separate durable bbolt database; complete
  FSM snapshots validate before atomic replacement and support log compaction.
- [x] Clustered mutations require request-scoped operation IDs; retries replay
  the first result without executing the mutation twice.
- [x] Followers and minority partitions fail closed; configured API URLs are
  returned as leader hints.
- [x] Static first bootstrap plus token-gated nonvoter, voter, demotion, removal,
  protocol-compatibility, status, and explicit snapshot operations are wired.
- [x] Tests cover three-node quorum, failover, minority fencing, FIFO ordering
  across election, proposal replay, snapshot/restart, corruption rejection,
  and schema migration.
- [x] A real three-process TCP smoke test kills the acknowledged-write leader
  and observes the queue on the newly elected leader.
- [x] Go 1.26.7 gofmt, vet, forced process-crash recovery, test, race, build, and
  `scripts/verify.ps1` pass.

M5 is complete and preserved.

## M6: product security and observability

- [x] M6 architecture, identity, tenancy, encryption, quota, TLS, metric, audit,
  and compatibility contracts are recorded in `M6_PLAN.md`, `SPEC.md`,
  `INVARIANTS.md`, and ADR 0010.
- [x] Strict OIDC discovery/JWKS and RS256 JWT verification validates issuer,
  audience, time, subject, tenant, roles, key size, algorithm, duplicate claims,
  HTTPS redirects, and key rotation fail closed.
- [x] Deny-by-default fixed roles protect every queue action while health,
  metrics, and cluster administration retain separate operator boundaries.
- [x] SHA-256 tenant namespaces cover queues, messages, receipts, redrive
  references, move handles, pagination, FIFO state, and replicated commands;
  the legacy namespace cannot alias reserved tenant keys.
- [x] AES-256-GCM encrypts body and attributes before memory, bbolt, Raft, and
  snapshots; key IDs, random nonces, AAD, old read keys, tamper/unknown-key
  failure, FIFO duplicate acknowledgements, and ciphertext-only bbolt storage
  are covered.
- [x] Schema v8 adds validated tenant usage with a synchronized schema-v7
  backup; queue, message, and stored-byte quota mutations remain atomic.
- [x] Leader-authoritative quota values are carried in Raft commands, transient
  token buckets are tenant-scoped, and HTTP/batch 429 results are stable.
- [x] TLS 1.3 protects secure-mode HTTP and optional mutually authenticated
  Raft; generated-certificate replication tests cover the transport.
- [x] Bounded Prometheus labels, independent metrics authentication, hashed
  structured audit identities, request/trace correlation, and JSONL audit
  persistence are implemented and tested for redaction.
- [x] `docs/security-operations.md` documents secure startup, roles, limits,
  legacy enablement, key rotation, recovery key retention, metrics, audit, and
  Raft certificate operation.
- [x] Go 1.26.7 gofmt, vet, forced process-crash recovery, test, race, build, and
  `scripts/verify.ps1` pass.

M6 is complete and M1-M5 behavior remains preserved when security mode is
disabled.

## M7: fixed-catalog multi-Raft sharding

- [x] M7 architecture, catalog, routing, placement, relocation, recovery, and
  compatibility contracts are recorded in `M7_PLAN.md`, `SPEC.md`,
  `INVARIANTS.md`, and ADR 0011.
- [x] A strict manifest validates at least two fixed shard IDs, default shard,
  local replicas, globally unique Raft addresses, bootstrap voters, and
  failure-domain-safe initial placement.
- [x] Canonical catalog revisions ignore JSON and placement order while binding
  the version, default shard, and sorted shard IDs.
- [x] Schema v9 migrates from v8 with a validated backup, binds catalog identity
  once, and rejects mismatched startup and snapshot restore.
- [x] Every process opens a separate M5 bbolt/Raft/FSM/snapshot stack per shard
  and closes partial startup safely.
- [x] Rendezvous hashing routes the authenticated tenant namespace to one shard
  before leader handling; legacy keys remain on the default shard.
- [x] Queue, FIFO, redrive, move-task, encryption, usage, and quota operations
  remain inside the selected tenant shard.
- [x] Token-protected shard status, per-shard membership, leader hint, and
  snapshot endpoints are implemented.
- [x] Whole-shard replica relocation validates planned node/address, a minimum
  of three voters, and the post-change failure-domain quorum before Raft calls.
- [x] Tests cover catalog determinism and mismatch, routing isolation,
  placement rejection, HTTP shard leader selection, schema migration, and the
  tenant queue-ID/receipt boundary.
- [x] A real three-node/two-shard integration test preserves FIFO order and
  acknowledged state in both independent Raft groups after one node fails.
- [x] `docs/shard-operations.md` documents secure startup, placement-only
  rollout, replica relocation, per-shard backup/recovery, and the explicit lack
  of online tenant resharding.
- [x] Go 1.26.7 gofmt, vet, forced process-crash recovery, test, race, build, and
  `scripts/verify.ps1` pass.

M7 is complete. M1-M6 semantics are preserved; changing the fixed shard catalog
or moving an existing tenant between shard IDs remains deliberately unsupported.

## M8: epoch-fenced tenant relocation

- [x] Record the fixed-catalog relocation protocol, ownership epochs, phase
  recovery, bundle bound, and single-writer invariants in the specification and
  ADR 0012.
- [x] Migrate schema v9 to v10 with validated ownership, migration, local fence,
  and tenant bundle buckets plus a validated schema-v9 backup.
- [x] Replicate version-2 control and data commands through each shard's existing
  Raft FSM and retain proposal replay records in a moved tenant bundle.
- [x] Route explicit ownership overrides through the default control shard and
  reject frozen, prepared, moved, stale-epoch, or wrong-shard access.
- [x] Freeze and capture atomically, prepare and hash-verify atomically, cut over
  ownership with compare-and-swap, activate destination, and clean source.
- [x] Make every advance and pre-cutover abort step idempotent and report the
  next required shard leader to operators.
- [x] Add cluster-token-protected create, list, status, advance, and abort APIs
  with strict bounded request decoding.
- [x] Preserve migration state in complete snapshots and resume through leader
  failure without exposing two writers.
- [x] Cover FIFO order, receipts, tenant isolation, abort, return migration,
  snapshot restore, proposal replay, and three-node/two-shard failover.
- [x] Document tenant relocation operation and pass `scripts/verify.ps1` with
  Go 1.26.7 before delivery on a feature branch through a pull request.

M8 is complete. M1-M7 semantics remain preserved. Dynamic shard-ID changes,
selective shard hosting, and tenant bundles larger than 16 MiB remain outside
the M8 boundary.

## M9: elastic shard topology

- [x] Specify manifest v2, immutable cluster and shard incarnations, explicit
  directory modes, catalog generations, and candidate/ready/draining/retired
  transitions in ADR 0013, `SPEC.md`, and `INVARIANTS.md`.
- [x] Migrate schema v10 to v11 with topology catalog, operation, and permanent
  tombstone buckets plus a validated schema-v10 backup.
- [x] Replicate topology command protocol version 3 and preserve its state in
  complete FSM snapshots and proposal replay.
- [x] CAS first-use tenant assignment and batch-backfill stored tenant digests
  before any candidate can become ready.
- [x] Open only locally planned data groups, keep the default control shard
  local, and return reviewed remote entry hints without local fallback.
- [x] Activate candidates without moving existing owners; drain one owner at a
  time through M8 before retirement and reject shard-ID reuse.
- [x] Add strict token-protected topology catalog, operation, backfill,
  activation, drain, advance, and abort APIs.
- [x] Cover deterministic selection, schema upgrade, manifest safety, explicit
  assignment, selective hosting, activation, drain, FIFO data preservation,
  retirement, API strictness, and existing three-node failover.
- [x] Document the operating sequence and pass the Go 1.26.7 verification gate
  before delivery through a feature-branch pull request.

M9 is complete. New catalog generations affect only unseen tenants; existing
tenants move only through an epoch-fenced M8 relocation. Automatic balancing,
tenant splitting, unreviewed endpoints, and physical shard deletion remain out
of scope.

## M10: operational validation and recovery

- [x] Pin and add PR CI for Go verify, Docker integration, and alert rules.
- [x] Generate ephemeral OIDC, JWT, API TLS, Raft mTLS, and encryption fixtures.
- [x] Run a three-node/two-shard manifest-v2 secure Compose environment.
- [x] Prove minority fencing and majority progress with Raft-only partitioning.
- [x] Prove offline volume backup and restore into recreated empty volumes.
- [x] Add deterministic Prometheus SLO alert-rule tests.
- [x] Support stable-DNS Raft bootstrap and provide a pinned kind profile; the
  current Windows Docker cgroup v1 host fails its explicit capability check.
- [x] Capture bounded diagnostics, cleanup, and local/staging evidence limits.
- [x] Pass Go 1.26.7 verification and every available M10 profile; kind is
  explicitly unavailable on this workstation's Docker cgroup v1 backend.
- [x] Deliver through feature branch `test/m10-operational-validation` and
  pull request #4.

## M11: continuous operational qualification

- [x] Merge M10 and isolate M11 on a new feature branch.
- [x] Pin kubectl to the kind node version on Linux and Windows.
- [x] Repeat the full secure chaos and offline restore profile with per-run
  timeout, cleanup, results, and bounded text diagnostics.
- [x] Add path-scoped PR, manual, and weekly heavy operational Actions jobs.
- [x] Gate 14-day diagnostic artifact upload on a sensitive-value scan.
- [x] Pass both heavy jobs on one reviewed GitHub commit.
- [x] Pass Go 1.26.7 verification, local supported profiles, and repository
  secret scanning.
- [x] Deliver M11 through feature branch
  `test/m11-continuous-operational-qualification` and pull request #5.
