# SimQ M1 plan

## Delivery order

M1 is delivered as correctness-first vertical slices:

1. **M1-A — single-node durability and restart recovery.** Persist the complete
   M0 state machine without changing its public action shapes.
2. **M1-B — `ChangeMessageVisibility`.** Extend current-generation receipt
   mutation after durable receipt semantics are proven.
3. **M1-C — delivery delay and message retention.** Add durable lifecycle
   deadlines and cleanup.
4. **M1-D — message attributes and queue attributes.** Expand immutable payload
   records and queue metadata with migration/versioning support.
5. **M1-E — long polling.** Add transient waiter notification around the same
   authoritative durable claim operation.
6. **M1-F — batch message operations.** Add per-entry results without weakening
   the single-message invariants.

M1-A through M1-F are complete and preserved. M2 is complete and tracked in
`M2_PLAN.md`.

## M1-A detailed work and invariants

| Work item | Invariants |
|---|---|
| Adopt/version bbolt and document durability boundary | DUR-001, DUR-002, DUR-004, DUR-005, DUR-006 |
| Define versioned repository records and strict schema validation | DUR-002, DUR-003, DUR-005, DUR-006, MSG-003, MSG-007, RCP-002 |
| Make repository reads return errors; define health and close | DUR-004, DUR-006, CON-001 |
| Run shared conformance tests against memory and bbolt | MSG-001–MSG-008, RCP-001–RCP-004, TIME-003, CON-001–CON-003 |
| Commit create and enqueue in one durable transaction each | DUR-001, DUR-002, DUR-004, MSG-001, MSG-007 |
| Commit selection and all receive metadata in one transaction | DUR-002, DUR-003, DUR-004, MSG-002–MSG-005, RCP-001, RCP-002, TIME-001, TIME-003, CON-002 |
| Commit delete and active-order removal together | DUR-001, DUR-002, DUR-004, MSG-006, RCP-003, CON-003 |
| Preserve ID and receipt issue history across delete/restart | DUR-003, DUR-005, RCP-001–RCP-004 |
| Wire default durable startup, bounded lock, readiness, close | DUR-004, DUR-006, CON-001 |
| Prove close/reopen and SIGKILL recovery | DUR-001–DUR-006, TIME-002, TIME-003, CON-001 |

`TIME-001` is preserved by generating command timestamps in the service.
Message IDs and receipt handles are also generated before repository mutation,
preparing for deterministic command replication in M5.

## M1-B detailed work and invariants

| Work item | Invariants |
|---|---|
| Specify the strict JSON action contract and stable error mapping | API-001–API-004, API-007, SEC-002, SEC-003 |
| Add an explicit service-clock timestamp to the repository command | TIME-001, TIME-003 |
| Change only the current generation's deadline under one memory lock | RCP-002–RCP-004, DUR-002, CON-001, CON-005 |
| Change only the current generation's deadline in one bbolt `DB.Update` | RCP-002–RCP-004, DUR-002, DUR-004, CON-002, CON-005 |
| Preserve stale and consumed receipt history as successful no-ops | RCP-003, TIME-004 |
| Run one conformance suite against memory and bbolt | MSG-005, MSG-007, MSG-010, RCP-002–RCP-004, TIME-002–TIME-004, CON-001, CON-002, CON-005 |
| Prove changed deadlines across close/reopen and SIGKILL restart | DUR-003, DUR-004, TIME-003 |
| Prove receive/change and delete/change linearization | RCP-003, TIME-004, CON-001, CON-002, CON-005 |
| Preserve strict failure and sensitive-data boundaries | DUR-004, SEC-001, SEC-002 |

The on-disk message record already stores `VisibilityDeadlineUnixNanos`, and
the receipts bucket already preserves queue/message/generation bindings.
M1-B therefore retains schema and record version 1 and creates no migration.
The service obtains its clock once per accepted command; neither repository
reads wall-clock time.

## M1-C detailed work and invariants

| Work item | Invariants |
|---|---|
| Specify queue defaults, per-message override, exact deadlines, and strict JSON | API-001–API-004, API-007, LIFE-001–LIFE-003, TIME-001 |
| Persist queue delay/retention and message availability/expiration in schema v2 | DUR-001–DUR-006, LIFE-001–LIFE-004, TIME-003 |
| Validate, back up, and atomically migrate valid schema v1 databases | DUR-001, DUR-003, DUR-005, DUR-006, LIFE-004 |
| Skip delayed and remove expired records inside the authoritative claim | MSG-002, MSG-003, LIFE-001, LIFE-002, CON-001, CON-002 |
| Make visibility changes consume expired active messages without resurrection | RCP-003, LIFE-002, LIFE-003, CON-001, CON-002 |
| Add an explicit service-timestamped expiration repository command | TIME-001, TIME-003, LIFE-002, CON-001, CON-002 |
| Run a local best-effort sweeper without making timers authoritative | LIFE-002, LIFE-003, DUR-004, SEC-001, SEC-002 |
| Run one lifecycle conformance suite against memory and bbolt | LIFE-001–LIFE-004, MSG-001–MSG-007, RCP-001–RCP-004 |
| Prove lifecycle deadlines and cleanup across close/reopen and process restart | DUR-001–DUR-006, LIFE-001–LIFE-004, TIME-002, TIME-003 |

M1-C deliberately keeps the existing ordered scan. Eligibility and cleanup are
correct before an expiration index is introduced. Repository methods receive
explicit timestamps and never read the wall clock.

## M1-D detailed work and invariants

| Work item | Invariants |
|---|---|
| Specify strict JSON message values, selection, size, digest, and queue actions | API-001–API-004, API-007, ATTR-001–ATTR-003, SEC-002, SEC-003 |
| Add deep-copied String/Number/Binary domain values and canonical digest helpers | MSG-010, ATTR-001, ATTR-002 |
| Move delay/retention default resolution into atomic enqueue commands | TIME-001, DUR-002, QUEUE-005, QUEUE-006, CON-001, CON-002 |
| Move default visibility resolution into the atomic claim command | MSG-002, TIME-001, QUEUE-006, CON-001, CON-002 |
| Apply partial queue-setting updates in one repository transition | DUR-002, DUR-004, QUEUE-002, QUEUE-005, QUEUE-007 |
| Persist deterministic attribute records and digests in schema version 3 | DUR-001–DUR-006, ATTR-001, ATTR-002 |
| Validate, back up, and atomically migrate schema v2; retain v1 sequential support | DUR-003–DUR-006, MIG-001, MIG-002 |
| Run the same attribute, setting, and Set-race suite for memory and bbolt | MSG-001–MSG-010, LIFE-001–LIFE-004, ATTR-001–ATTR-003, QUEUE-005–QUEUE-007 |
| Prove attributes/settings across close/reopen and process restart | DUR-001–DUR-006, ATTR-001, ATTR-002, QUEUE-005 |
| Preserve redaction and rollback at validation, encoding, and commit failures | DUR-002, DUR-004, SEC-001, SEC-002 |

The service validates payload meaning, creates IDs and one timestamp, and sends
an optional override rather than resolved queue defaults. The memory lock or
bbolt write transaction reads the complete current queue record and performs
enqueue/claim in the same boundary. `SetQueueAttributes` uses that same
serialization boundary. No repository reads the wall clock.

## M1-E detailed work and invariants

| Work item | Invariants |
|---|---|
| Specify request-only `WaitTimeSeconds` 0–20 and preserve omitted/zero short poll | API-001–API-004, API-007, MSG-008, SEC-002, SEC-003 |
| Return an earliest lifecycle-transition hint from the normal claim transaction | TIME-001–TIME-003, LIFE-001–LIFE-003, POLL-005, POLL-006 |
| Coordinate transient queue-scoped broadcast waiters in the service | POLL-001–POLL-003, POLL-007, MSG-007 |
| Register, recheck, then block without holding repository state | MSG-002, CON-002, POLL-002, POLL-003, POLL-005 |
| Wake on committed enqueue and visibility changes; recheck delayed/visibility deadlines by timer | MSG-001, MSG-005, LIFE-001, POLL-002, POLL-006 |
| Release waiters on request cancellation, polling timeout, and server shutdown | CON-004, POLL-001, POLL-004, POLL-005 |
| Prove multi-waiter exclusivity and queue isolation on memory and bbolt | MSG-004, MSG-007, CON-001, CON-002, POLL-007 |
| Preserve schema v3 and durable state across close/reopen and SIGKILL | DUR-001–DUR-004, POLL-001 |
| Preserve storage-error mapping and sensitive-data redaction | DUR-004, SEC-001, SEC-002 |

M1-E adds no durable waiter record and no queue-level wait attribute. Omitted
`WaitTimeSeconds` is zero. The service owns a process-local notification
registry and controllable timer abstraction. An empty repository claim returns
only an earliest-transition hint computed from stored lifecycle timestamps;
the service waits after the transaction closes and then performs another
ordinary atomic claim.

## M1-F detailed work and invariants

| Work item | Invariants |
|---|---|
| Specify strict batch shapes, request errors, per-entry results, and stable ordering | API-001–API-004, API-007, BAT-001–BAT-003, BAT-005, BAT-007 |
| Validate count, ID syntax/uniqueness, QueueUrl, queue existence, and aggregate Send size before mutation | BAT-002, BAT-005, SEC-002, SEC-003 |
| Reuse single Send validation and one enqueue commit per valid entry | MSG-001, MSG-010, ATTR-001, ATTR-002, QUEUE-006, DUR-004, BAT-004, BAT-006 |
| Reuse current/stale/consumed/unissued receipt semantics for Delete entries | MSG-006, RCP-002–RCP-004, DUR-004, BAT-004, BAT-006 |
| Reuse timestamped visibility mutation independently for each entry | MSG-005, RCP-003, TIME-001, TIME-003, CON-005, BAT-004, BAT-006 |
| Map validation and repository failures to redacted ordered entry results | API-003, BAT-001, BAT-003, BAT-006, BAT-007, SEC-001 |
| Prove batch/single and batch/batch concurrency on memory and bbolt | CON-001–CON-005, BAT-004, BAT-007 |
| Prove per-entry commit failure, close/reopen, and forced process recovery | DUR-001–DUR-006, BAT-006, BAT-008 |

M1-F deliberately adds no repository-wide batch transaction. The queue service
validates request-wide constraints, then attempts entries in request order by
calling the established single-message service paths. Each bbolt entry is one
existing synchronized `DB.Update`; memory uses the corresponding mutex-backed
transition. This preserves partial success and makes the durability boundary
auditable. Schema and record version 3 remain unchanged.

## Acceptance tests by stage

### Repository contract

- The same suite covers create/get, enqueue, empty receive, single and batch
  claim, queue isolation, current/repeated/stale/malformed delete, 20 concurrent
  receives, and 20 concurrent deletes for both repositories.
- Repository read/write errors reach the service and a storage-unavailable HTTP
  response; no operation reports success after its update returns an error.

### Schema and startup

- A new database contains the exact schema version, installation ID, and
  required buckets.
- Unknown schema versions, missing buckets/fields, malformed records, invalid
  paths, unsafe permissions, zero-length existing files, and a locked database
  all fail explicitly.
- A transaction callback error rolls back every partial write.

### Restart recovery

- Queues, empty queues, multiple-queue isolation, sent messages, message order,
  receive count/generation, first-receive timestamp, nanosecond visibility
  deadline, current receipt, and old receipt history survive close/reopen.
- A message remains hidden just before the recovered deadline and is eligible at
  exactly the deadline using an injected clock.
- Reopen issues a fresh receipt; the pre-reopen stale receipt cannot delete the
  newer claim.
- A committed delete remains terminal after reopen.

### Process boundary

- An HTTP subprocess is SIGKILLed after send, after receive, and after delete.
  Each restart uses the same bbolt file and proves recovery, hidden in-flight
  state, and terminal deletion respectively.
- Exact visibility timing is proven by close/reopen repository tests with a fake
  clock; the subprocess test uses a long timeout only to avoid crossing the
  deadline while checking state recovery.

### M1-B repository and domain contract

- Current-handle extension, shortening, repeated reset, zero timeout, and exact
  nanosecond eligibility boundaries pass for memory and bbolt.
- Deadline mutation preserves the receipt handle, receive count/generation,
  first-receive time, sent time, message ID, digest, body, active order, and all
  issue-history records.
- Malformed and unissued handles fail; stale and consumed issued handles are
  no-ops; cross-queue handles fail without changing either queue.
- A stale change racing with re-receive produces only one of the two documented
  serial orders, and an old generation never changes a newer deadline.
- Delete racing with a current visibility change always leaves the message
  deleted.

### M1-B restart and failure

- An extended or zero deadline survives close/reopen. The message stays hidden
  one nanosecond before the stored deadline and becomes eligible exactly at it.
- Reopen preserves receive count/generation and first-receive time; a reopened
  stale handle cannot change the current deadline, and a later delete remains
  terminal.
- An injected bbolt update/commit error rolls back the deadline. Repository
  failure reaches the service and becomes HTTP 503 without leaking paths,
  bodies, or receipt handles.
- The HTTP SIGKILL smoke proves zero-timeout immediate recovery, a later long
  visibility change remaining hidden after restart, and terminal deletion.
- `ChangeMessageVisibilityBatch` remains HTTP 501.

### M1-C lifecycle contract

- Queue-level defaults and explicit default values are idempotently equivalent;
  any difference in visibility, delay, or retention rejects recreation.
- A queue delay and a per-message override both hide a message before
  `AvailableAt` and expose it exactly at the deadline when it has not expired.
- An earlier delayed message does not block a later eligible message in the
  same Standard Queue scan.
- At `ExpiresAt`, delayed, visible, and in-flight messages are terminal.
  Receiving or changing visibility cannot extend retention.
- Claim and change-visibility enforce expiration even when no background sweep
  has run. A sweep removes idle expired active records while retaining issued
  message-ID and receipt history.

### M1-C migration and recovery

- New databases use schema and record version 2 with nanosecond lifecycle
  deadlines and durable queue defaults.
- Opening a valid version 1 database validates it, creates and syncs a `0600`
  `.schema-v1.bak`, atomically migrates every record, and validates version 2
  before startup succeeds.
- Unknown, malformed, partially migrated, or invalid-backup state fails closed.
- Queue settings and message deadlines survive close/reopen. Exact availability
  and expiration boundaries are tested with an injected clock, not real sleeps.
- A process restart cannot make delayed or expired messages visible and cannot
  recompute or extend their expiration.

### M1-C runtime cleanup

- `SIMQ_RETENTION_SWEEP_INTERVAL` accepts only a positive Go duration and
  defaults to one minute.
- Every pass obtains exactly one service-clock timestamp and submits an explicit
  repository expiration command.
- A pass failure is retried later and logged without payloads, receipt handles,
  or storage paths; it cannot change eligibility correctness.

### M1-D message attributes

- String, Number, Binary, and custom-suffix values pass strict validation;
  malformed names/types/value combinations, invalid numbers/base64, null,
  duplicate or unknown nested fields, 11 attributes, and one-byte-over-size
  payloads fail without enqueue.
- Zero and ten attributes are exact accepted count boundaries. Body plus names,
  complete types, and actual value bytes is accepted at exactly 1 MiB.
- Canonical digest vectors are independent of input map order. Empty attributes
  omit the digest, while send and selected receive projections return their
  specified digest.
- Input mutation, repository return mutation, and receive projection cannot
  alter the stored map or binary slices. Receive, visibility, retention, delete,
  and queue setting changes do not mutate attributes.
- Attribute values and digest are identical after close/reopen and SIGKILL; an
  enqueue failure rolls back message, order, ID history, and attributes.

### M1-D queue attributes and configuration races

- Get supports empty, exact, and `All` selection for the three supported names.
  Set validates a non-empty partial decimal-string update and leaves omitted
  settings intact.
- Range boundaries, unknown/null/empty/malformed input, missing queues, strict
  JSON, request IDs, and repository failures have the documented HTTP results
  and no side effects.
- A failed multi-value Set preserves every old value. After a successful Set,
  CreateQueue idempotency compares the current complete configuration.
- Set racing with Send yields only a complete old or new delay/retention
  snapshot; Set racing with Receive yields only the old or new default
  visibility. Tests use barriers and repeat against memory and bbolt under the
  race detector.
- Established message availability, expiration, timestamps, payload, and
  receipt history never change because of Set. Settings survive close/reopen
  and SIGKILL.

### M1-D schema migration

- New databases and all newly encoded records use version 3. Attribute records
  have deterministic order and strict graph/digest validation.
- Empty and populated v2 stores, including active claims and receipt history,
  create a synchronized mode-`0600` `.schema-v2.bak` and migrate in one write
  transaction. Old messages receive empty attributes without lifecycle or claim
  recomputation.
- A v1 store follows v1→v2→v3 and retains both source backups. Valid existing
  backups are reused; invalid backups, corrupt sources, partial records, unknown
  versions, callback/commit failures, and failed destination validation prevent
  startup.
- Interrupted/failed migration is retryable and never exposes a partial schema.

### M1-E long polling

- Omitted and zero wait return immediately; 1 and 20 succeed; negative, 21,
  fractional, null, duplicate, unknown, trailing, malformed, and oversized
  requests fail without claim.
- Immediately eligible messages return without waiting. An empty request times
  out with an empty array, while a committed send wakes a blocked request.
- A send in every claim/register/recheck window is observed by either the
  recheck or notification. Spurious broadcasts cannot fabricate or duplicate a
  claim.
- Delayed messages and expired visibility are claimed at their exact stored
  boundary using injected time and a controllable timer. Retention expiration
  while waiting never returns or resurrects a message.
- One message wakes several waiters but yields at most one positive-visibility
  claim. Several messages may satisfy several waiters, and queues remain
  isolated, for both memory and bbolt under the race detector.
- Client cancellation, poll timeout, and server shutdown release waiter
  resources without holding repository locks or changing durable state.
- Close/reopen and SIGKILL preserve messages and lifecycle state; process-local
  waiters are intentionally not recovered.
- Repository failure remains HTTP 503 and errors/logs contain no message body,
  attributes, receipt, or storage path.

### M1-F batch actions

- One and ten entries succeed; empty/null, eleven, duplicate, and invalid IDs
  are request-level errors that apply nothing.
- Strict nested JSON and QueueUrl/queue validation complete before mutation.
  Send aggregate logical size accepts exactly 1 MiB and rejects one byte more.
- Partial success and all-failure responses contain each ID exactly once, with
  both result arrays preserving relative input order.
- Send entries preserve body, per-entry delay, message attributes, digests, and
  atomic queue-default snapshots. Delete and visibility entries preserve all
  established receipt-generation and exact-deadline meanings.
- Each durable success follows its own commit. Injected bbolt failure produces
  a failed entry without rolling back other committed entries or reporting a
  false success.
- Batch/single and batch/batch races pass for memory and bbolt. Close/reopen and
  forced process termination recover committed entries and no uncommitted false
  success.
- Errors and logs omit bodies, attributes, receipts, and storage paths; the race
  detector covers both repositories and HTTP orchestration.

### Completion gate

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `./scripts/verify.sh`, including the subprocess restart test and build
- On Windows-only hosts, `./scripts/verify.ps1`, including the equivalent
  forced-process-exit restart test and build

## Explicit exclusions

M1-F does not implement queue-level receive-wait defaults, batch-wide atomicity,
exactly-once batch retry, queue deletion/purge, approximate counts,
policies, tags, DLQ/redrive, FIFO features, online backup/restore, compaction,
authentication, tenant isolation, encryption, clustering, replication, leader
election, or distributed operation identity.
