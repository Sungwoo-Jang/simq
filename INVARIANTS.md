# SimQ Invariants

Status: Draft

Version: 0.1

Last updated: 2026-08-23

This document defines the properties that SimQ MUST preserve. Features may be
incomplete, but an implementation MUST NOT claim a feature is complete while
violating an invariant that applies to that feature.

`SPEC.md` defines the public behavior. This document defines the safety rules
that constrain every implementation of that behavior.

## 1. How to use this document

Every implementation task MUST:

1. Identify the invariant IDs affected by the change.
2. Add or update automated tests that demonstrate those invariants.
3. Run the relevant verification commands.
4. Report any invariant that cannot yet be demonstrated.
5. Never weaken an invariant or its tests without an explicit specification or
   architecture decision.

An invariant is not proven merely because a happy-path test passes. Tests MUST
cover the concurrency, timeout, retry, restart, or failure condition named by
the invariant where applicable.

## 2. Activation by milestone

| Milestone | Required invariant groups |
|---|---|
| M0: in-memory Standard Queue | API, queue state, receipt handles, time, concurrency |
| M1: durable Standard Queue | M0 plus durability, delay, retention, long polling, batch |
| M2: administration | M1 plus queue lifecycle and metadata |
| M3: DLQ and redrive | M2 plus dead-letter and transfer invariants |
| M4: FIFO | M3 plus FIFO invariants |
| M5: clustered operation | All previous groups plus replication invariants |

Future invariants apply as soon as the corresponding feature is introduced,
even if the milestone containing that feature is otherwise incomplete.

## 3. API invariants

### API-001: Action routing is explicit

Every queue action MUST be invoked through:

```text
POST /v1/sqs/{ActionName}
```

The action name is case-sensitive. The server MUST NOT depend on AWS headers,
AWS signatures, AWS SDK behavior, or XML/query-protocol parsing to identify an
action.

### API-002: Requests and responses are JSON

Action request and response bodies MUST use `application/json`. Public field
names and types MUST match `SPEC.md`. Unknown fields MUST be rejected with
`InvalidRequest` while strict decoding remains part of the specification.

### API-003: Errors are explicit

Malformed or unsupported input MUST NOT produce a false success response.
Errors MUST use the documented JSON envelope and a stable error code. A known
but unimplemented action MUST return `NotImplemented` with HTTP 501.

### API-004: Every response is traceable

Every action response, including errors, MUST contain the same non-empty request
ID in `X-SimQ-Request-Id` and the JSON body.

### API-005: Identifiers are opaque

Clients MUST NOT need to parse message IDs, receipt handles, request IDs, or
queue URLs. Internal format changes MUST NOT change the documented behavior of
those identifiers.

### API-006: Queue URLs are trusted-server output

Queue URLs MUST be constructed from the configured public base URL. An
untrusted HTTP `Host` header MUST NOT control a returned queue URL.

### API-007: No ambiguous body parsing

The server MUST reject malformed JSON, duplicate semantic fields, invalid field
types, trailing non-whitespace JSON data, and request bodies larger than the
configured limit. A decoding error MUST NOT partially apply an operation.

## 4. Queue and message state invariants

### MSG-001: Acknowledged messages enter the queue

After a successful `SendMessage` response, the message MUST be represented in
the active queue state unless it has subsequently been deleted, purged, moved,
or expired according to a committed operation.

M0 guarantees this only for the lifetime of the current process. Durability
invariants strengthen this guarantee when persistent storage is introduced.

### MSG-002: Receive is an atomic state transition

`ReceiveMessage` is a mutating claim operation. Selecting a visible message,
incrementing its receive generation and receive count, assigning its receipt
handle, recording its first-receive timestamp when applicable, and setting its
visibility deadline MUST occur as one atomic state transition.

A peek followed by an asynchronous or separately locked visibility update is
forbidden.

### MSG-003: Active state is exclusive

An active message MUST be in exactly one of these logical states:

```text
delayed | visible | in-flight
```

A message MUST NOT simultaneously be present in multiple delivery indexes. A
deleted, purged, moved, or expired message MUST NOT remain in an active delivery
index.

### MSG-004: Claims are exclusive within a visibility window

During a positive, unexpired visibility timeout, one authoritative message
generation MUST NOT be successfully claimed by more than one consumer.

For a queue containing one visible message, concurrent receive requests may
return at most one successful claim until that claim expires or is released.

### MSG-005: Visibility expiration restores eligibility

An undeleted in-flight message MUST become eligible for receive when its
visibility deadline is reached. It MUST NOT become eligible before that
deadline.

A visibility timeout of zero makes the message immediately eligible again.

### MSG-006: Deletion is terminal for that message

After a successful delete using the current receipt handle, that message MUST
never be returned again. Repeating the same delete MAY succeed as an idempotent
no-op but MUST NOT affect another message.

### MSG-007: Queue isolation

An operation on one queue MUST NOT read, claim, modify, delete, purge, expire, or
move a message belonging to another queue unless an explicit redrive operation
targets that queue.

### MSG-008: Empty receive is not an error

Receiving from an existing queue with no eligible messages MUST return success
with an empty message list. It MUST NOT manufacture a message or return an
unrelated queue's message.

### MSG-009: At-least-once is the delivery contract

Standard queues MUST NOT claim exactly-once consumer processing. Duplicate
delivery is permitted after visibility expiration, retry, ambiguous response,
or failure. SimQ MUST NOT intentionally duplicate messages in normal operation
solely to simulate at-least-once delivery.

### MSG-010: Message contents are immutable

After enqueue, a message body and its user-provided attributes MUST NOT change.
Receive-related system metadata MAY change according to the specification.

## 5. Receipt handle invariants

### RCP-001: Every receive creates a new handle

Every successful receive generation MUST produce a receipt handle different
from all previous handles for that message.

### RCP-002: A handle is bound to one claim

A receipt handle MUST be bound to the queue, message, and receive generation
that created it. A handle from one queue or message MUST NOT operate on another.

### RCP-003: Only the current handle may mutate the current claim

Only the current receipt handle may delete the message or change its current
visibility deadline. A stale but well-formed handle MAY return success as a
no-op, but MUST NOT delete or modify a newer receive generation.

### RCP-004: Malformed handles have no effect

A malformed, corrupted, or unverifiable receipt handle MUST return
`ReceiptHandleIsInvalid` and MUST NOT mutate queue state.

### RCP-005: Handles expose no authority beyond their operation

Possession of a receipt handle MUST NOT permit changing the message body,
moving another message, or operating on another queue.

## 6. Time invariants

### TIME-001: Production time has one controlled source

Queue state transitions MUST obtain time through the service clock abstraction
or through an explicit command timestamp. Queue-domain code MUST NOT scatter
direct wall-clock reads across the implementation.

### TIME-002: Tests do not depend on sleeping

Tests for visibility, delay, retention, and deduplication MUST use an injected
clock or explicit timestamps. Sleeps MUST NOT be used to hide races or to make
correctness depend on scheduler timing.

### TIME-003: Deadlines use defined boundaries

A message is eligible when the relevant deadline is less than or equal to the
operation time. Boundary behavior MUST be consistent for delay, visibility,
retention, and deduplication.

### TIME-004: Time cannot resurrect terminal messages

Clock movement, visibility expiration, or delayed-delivery processing MUST NOT
resurrect a message that was deleted, purged, moved, or expired.

### TIME-005: Replicated time is deterministic

Once replication exists, timestamps affecting replicated state MUST be included
in the replicated command. Replica state-machine application MUST NOT call the
local wall clock.

## 7. Concurrency invariants

### CON-001: Shared state is race-free

Concurrent producers, consumers, deletes, visibility changes, and queue
operations MUST NOT cause data races. `go test -race ./...` MUST pass for every
completed milestone.

### CON-002: Concurrent receive preserves exclusive claims

Concurrent receives MUST be serialized at the state-transition boundary, not
merely at HTTP parsing or response serialization.

### CON-003: Concurrent delete cannot target a newer claim

A delete racing with visibility expiration or re-receive MUST be evaluated
against the receipt generation. An old delete MUST NOT remove a newer claim.

### CON-004: Cancellation does not imply rollback

HTTP cancellation or timeout MUST NOT be treated as proof that an operation was
not applied. Internal retries of mutating operations MUST preserve operation
identity once idempotent retry support exists.

### CON-005: Visibility mutation is generation-serialized

A visibility change racing with re-receive MUST be serialized with the receive
claim at the repository state-transition boundary. If receive creates a newer
generation first, the older handle MUST NOT change that generation's deadline.
A visibility change racing with deletion MUST NOT recreate the deleted message.

## 8. Durability invariants

These invariants become mandatory when durable local storage is introduced.

### DUR-001: Success means recoverable

After `SendMessage` returns success, the acknowledged message MUST survive an
immediate process crash and restart unless a later acknowledged operation
deleted, purged, moved, or expired it.

### DUR-002: State and indexes recover together

Message state and all indexes required to locate it MUST be updated atomically.
A crash MUST NOT leave a message visible and in-flight simultaneously, orphan a
live message, or retain a delivery index for a terminal message.

### DUR-003: Recovery preserves receive state

Receive count, receive generation, current receipt validity, first-receive
timestamp, visibility deadline, delay deadline, and retention deadline MUST
survive restart.

### DUR-004: Storage failure cannot produce success

If the durability boundary cannot be completed, the mutating HTTP operation
MUST NOT return success. Disk-full, checksum, serialization, and sync failures
MUST be surfaced as errors.

### DUR-005: Recovery is repeatable

Replaying the same committed durable operation more than once MUST not duplicate
its effect. Recovery interrupted by another crash MUST be safe to retry.

### DUR-006: Corruption is not silently accepted

Detected corruption MUST fail loudly or isolate the affected data according to
a documented recovery policy. The service MUST NOT silently replace corrupt
acknowledged messages with empty or fabricated data.

## 9. Delay and retention invariants

These invariants are active for M1-C.

### LIFE-001: Delayed messages are not delivered early

A delayed message MUST NOT be returned before its availability deadline. At or
after the deadline, it MUST become eligible unless it has expired or been
removed.

### LIFE-002: Retention removes messages from every active state

At the retention deadline, a message MUST cease to be deliverable whether it is
delayed, visible, or in-flight. Retention cleanup MUST remove all indexes and
current claim metadata that make the message active. Issued message-ID and
receipt-handle history MUST remain so consumed and stale handles preserve their
documented no-op behavior.

### LIFE-003: Visibility does not extend retention

Receiving a message or changing its visibility MUST NOT extend its retention
deadline.

### LIFE-004: Delay and retention survive restart

Once storage is durable, delayed and retained message timing MUST be recovered
after restart without early delivery or unintended lifetime extension.

## 10. Long-polling invariants

These invariants activate when long polling is implemented.

### POLL-001: Waiters do not own messages

Long-poll waiters are transient notification state. Registering, waking, timing
out, or losing a waiter MUST NOT itself claim, delete, or mutate a message.

### POLL-002: Wake-up is followed by an authoritative claim

A waiter notification only signals that work may exist. Every awakened waiter
MUST retry the same atomic claim operation used by short polling.

### POLL-003: Spurious and lost wake-ups are safe

Spurious wake-ups MUST NOT duplicate claims. A lost wake-up MAY delay an answer
until the polling deadline but MUST NOT lose or corrupt a message.

### POLL-004: Cancellation releases waiter resources

Client cancellation and polling deadlines MUST eventually remove local waiter
state without modifying durable queue state.

### POLL-005: Waiting holds no authoritative storage boundary

A long-poll waiter MUST NOT hold a memory repository lock, bbolt transaction,
or other authoritative claim boundary while blocked. Lifecycle timers and
notifications schedule a later claim; they do not extend a storage transaction.

### POLL-006: Lifecycle wake-ups use authoritative deadlines

An empty claim MAY return a transient earliest-transition hint derived from the
same explicit command timestamp and durable lifecycle records. Reaching that
hint MUST trigger a new claim. Delayed or in-flight messages MUST NOT be
returned early, and retention MUST remain terminal.

### POLL-007: Queue notifications are isolated broadcasts

An enqueue or eligibility hint for one queue MUST NOT wake or claim work from
another queue. Broadcasting to several waiters on the same queue MUST preserve
the exclusive repository claim rules.

## 11. Batch invariants

These invariants activate when batch actions are implemented.

### BAT-001: Each entry has exactly one result

Every batch request entry MUST appear exactly once in either the success list or
the failure list. It MUST NOT appear in both or neither.

### BAT-002: Entry IDs are request-local correlation IDs

Batch entry IDs MUST be unique within a request and MUST NOT be confused with
message IDs, receipt handles, or durable operation IDs.

### BAT-003: Partial failure is explicit

Failure of one entry MUST NOT turn successful entries into unreported results.
The response MUST expose per-entry failures using stable error codes.

### BAT-004: Single-entry invariants still apply

Batching MUST NOT weaken atomic claim, receipt generation, queue isolation,
durability, or FIFO ordering requirements for any individual entry.

### BAT-005: Request validation precedes entry mutation

Batch shape, count, entry-ID uniqueness and syntax, QueueUrl, queue existence,
strict JSON, HTTP size, and Send aggregate logical size MUST be validated before
the first entry mutation. A request-level failure MUST leave every entry
unapplied.

### BAT-006: Success follows each entry's commit

An entry MUST appear in `Successful` only after its own authoritative mutation
commits. A repository failure MUST produce a failed entry and MUST NOT produce a
false success. Entries are independent; no batch-wide atomicity is claimed.

### BAT-007: Result ordering is deterministic

Within `Successful` and `Failed`, results MUST preserve the relative input order
of their corresponding entries. Concurrent requests may interleave only at the
same repository boundaries used by single-message actions.

### BAT-008: Batch retry is not exactly once

An ambiguous client timeout does not establish whether an entry committed.
Until durable operation identities are specified, retrying Send batch entries
MAY enqueue additional messages and SimQ MUST NOT claim exactly-once retry.

## 12. Queue lifecycle and metadata invariants

These invariants activate with their corresponding administration features.

### QUEUE-001: Queue names are unique in a namespace

At most one live queue may exist for a given case-sensitive name within a
tenant or the initial global namespace.

### QUEUE-002: CreateQueue is conditionally idempotent

Creating an existing queue with equivalent creation attributes MUST return the
existing queue. Creating it with conflicting creation attributes MUST return a
conflict and MUST NOT modify the existing queue.

### QUEUE-003: Queue deletion isolates old identities

After a queue is deleted, stale queue URLs and receipt handles MUST NOT operate
on a subsequently recreated queue with the same name. Internal queue identity
MUST distinguish queue generations.

### QUEUE-004: Purge does not cross queue boundaries

Purging a queue MUST remove only that queue's messages and claim state. It MUST
NOT change its configuration or affect any other queue.

### QUEUE-005: Metadata changes do not rewrite message history

Changing queue attributes or tags MUST NOT alter immutable message bodies,
message IDs, send timestamps, or already established retention deadlines unless
the public specification explicitly defines such behavior.

### QUEUE-006: Configuration-dependent transitions use one snapshot

A send that uses queue delay or retention and a receive that uses queue default
visibility MUST read the queue configuration in the same repository critical
section or durable transaction that enqueues or claims the message. A concurrent
multi-attribute update may linearize before or after that transition, but an
operation MUST NOT combine values from different configuration revisions.

### QUEUE-007: Queue attribute updates are atomic

`SetQueueAttributes` is a partial update, but every supplied value MUST be
validated before mutation and all supplied values MUST commit together. An
invalid value or storage failure MUST leave the complete previous configuration
intact.

### QUEUE-008: Queue-scoped operations bind one generation

Every queue-scoped repository transaction MUST validate both the queue name and
opaque queue identity before reading or mutating state. Identity validation and
the requested operation MUST share one critical section or durable transaction.

### QUEUE-009: Queue deletion and purge are atomic

Delete MUST atomically remove the live queue generation and all generation-owned
state. Purge MUST atomically remove all active messages and receipt history for
one generation while preserving the queue and its metadata. Concurrent
operations may linearize before or after either boundary but MUST NOT observe a
partially deleted or partially purged state.

### QUEUE-010: Pagination either remains stable or fails closed

List results MUST have deterministic ordering. A continuation token MUST bind
its filter, cursor, and namespace revision. If create or delete invalidates that
revision, continuation MUST fail explicitly rather than silently skip or repeat
queues.

### QUEUE-011: Administrative metadata is isolated and atomic

Tag and permission updates MUST validate the complete request and commit as one
mutation. They MUST NOT alter queue configuration, message state, or other
queues. Permission compatibility metadata MUST NOT authorize or deny requests.

## 13. Message attribute and migration invariants

These invariants are active for M1-D.

### ATTR-001: User message attributes are immutable values

After enqueue, attribute names, data types, string values, and binary bytes MUST
remain unchanged until the message leaves active state. Input maps and byte
slices, response projections, receive metadata changes, visibility changes,
queue setting changes, retention processing, and repository reopen MUST NOT
alias or mutate the stored attribute set.

### ATTR-002: Attribute digests are canonical

`MD5OfMessageAttributes` MUST be derived from the documented sorted canonical
encoding and MUST NOT depend on map iteration or storage serialization order.
The send digest covers the complete stored set; a receive digest covers exactly
the attributes returned by that response projection.

### ATTR-003: Projection does not mutate authoritative payload

`MessageAttributeNames` selection is a response projection only. Exact-name,
empty, and `All` projections MUST produce the specified view without changing
the authoritative message or affecting claim, visibility, retention, or delete
semantics.

### MIG-001: Schema migrations preserve committed state or fail closed

Before a schema rewrite, the complete source schema and its synchronized backup
MUST validate. The rewrite and final schema-version change MUST be one durable
transaction, with the version changed last, followed by complete destination
validation. Failure MUST leave a valid previous schema or a valid fully migrated
schema; it MUST NOT open the listener on a partial or silently repaired store.

### MIG-002: Sequential migrations preserve recovery artifacts

Opening any supported older schema MUST follow a documented sequential or
equivalent safe path. Each source-version backup MUST be retained separately and
MUST NOT be silently overwritten. Migration MUST preserve all authoritative
message lifecycle, claim, receipt-history, ID, payload, and timestamp values
except for explicitly initialized fields introduced by the destination schema.

## 14. Dead-letter queue and redrive invariants

These invariants activate when DLQ or redrive support is implemented.

### DLQ-001: Receive count controls automatic movement

A message MUST move to its configured dead-letter queue only according to the
configured receive-count policy. Merely inspecting or listing a message MUST
NOT increment that count.

### DLQ-002: Movement never loses acknowledged message data

A move between a source queue and a dead-letter or destination queue MUST NOT
reach a completed state with the message absent from both queues.

### DLQ-003: Transfer retry is idempotent

Retrying the same logical transfer MUST NOT create unbounded destination
duplicates. Every transfer MUST have an identity sufficient to recover after an
ambiguous failure.

### DLQ-004: Completed movement removes source eligibility

After a move is completed, the source copy MUST no longer be receivable. A
partially completed move MUST remain recoverable and observable as incomplete.

### DLQ-005: Redrive preserves the message payload

Redrive MUST preserve the message body and user attributes. Any intentionally
reset or regenerated system metadata MUST be defined in `SPEC.md` before
implementation.

M3 preserves message ID, body, body digest, user attributes, and attribute
digest. Automatic transfer also preserves sent time and expiration. Both
transfer types reset the current receipt, receive count, first-receive time,
visibility, and delay, while internal receipt generation and issued handle
history remain monotonic. Manual move establishes a new sent time and retention
deadline.

### DLQ-006: Queue generations bind every redrive edge

Policies, original-source metadata, and task source/destination fields MUST bind
both queue name and generation ID. Deleting and recreating a queue name MUST NOT
retarget existing redrive state.

### DLQ-007: Move-task progress shares the message commit

For each manual move, source removal, destination insertion, origin cleanup, and
task progress MUST commit or roll back together. A task boundary MUST exclude
messages that arrive after the task starts.

## 15. FIFO invariants

These invariants activate when FIFO queues are implemented.

### FIFO-001: Ordering is per message group

Within a queue and `MessageGroupId`, later messages MUST NOT be delivered ahead
of an earlier eligible message. Independent groups MAY progress concurrently.

### FIFO-002: An in-flight group head blocks later group messages

While the current message for a group is in-flight, later messages in that same
group MUST NOT be newly delivered. Deletion or visibility expiration determines
the next eligible transition.

### FIFO-003: Send deduplication is atomic

Checking and recording a deduplication ID and enqueueing its message MUST be one
atomic operation. Concurrent sends with the same active deduplication ID MUST
NOT enqueue multiple messages.

### FIFO-004: Deduplication survives message deletion

Deleting or receiving a message MUST NOT prematurely remove its deduplication
record. The record remains effective for the configured five-minute window.

### FIFO-005: Content-based deduplication is deterministic

Content-based deduplication MUST derive the same deduplication value from the
same message body. User attributes MUST affect the value only if explicitly
defined in `SPEC.md`.

### FIFO-006: Sequence numbers are monotonic in their scope

FIFO sequence numbers MUST be unique and monotonically ordered within the scope
defined by the implementation. The scope MUST be documented before FIFO is
declared complete.

M4 sequence numbers are unique and strictly increasing within one FIFO queue
generation. They are allocated atomically with enqueue or arrival by queue
transfer and are never reused.

### FIFO-007: State-machine inputs are deterministic

FIFO application MUST NOT read local wall time or randomness. IDs, timestamps,
and retry identities MUST be explicit inputs so future replicas replay commands
identically.

### FIFO-008: Receive-attempt replay does not reclaim

Within its five-minute window, replaying one `ReceiveRequestAttemptId` MUST NOT
increment counts, allocate receipts, or select another group head.

### FIFO-009: Queue transfers preserve FIFO type and ordering metadata

Redrive and message-move transitions MUST NOT cross Standard and FIFO queue
types. A FIFO transfer preserves group and deduplication IDs and allocates a new
destination-generation sequence in the same atomic transition that removes the
message from the source order.

## 16. Replication and cluster invariants

These invariants activate as soon as replicated state is introduced.

### CLU-001: State-machine application is deterministic

Applying the same committed command to the same state MUST produce the same
state and result on every replica. State-machine application MUST NOT call:

- the local wall clock;
- random or UUID generators;
- network services;
- external mutable services;
- asynchronous goroutines that mutate replicated state.

All timestamps, IDs, nonces, and other nondeterministic inputs required by an
operation MUST be carried in the replicated command.

### CLU-002: Success follows durable commit and apply

A mutating HTTP response MUST NOT report success until its command is durably
committed by the required quorum and applied to the serving state machine.

### CLU-003: Minority partitions do not accept writes

A node or partition without quorum MUST NOT acknowledge a mutating operation as
successful. Data safety takes precedence over write availability without
quorum.

### CLU-004: One-node failure does not lose acknowledged data

In a healthy three-replica group, failure of any one node MUST NOT lose an
acknowledged message or an acknowledged delete, visibility change, purge, or
queue mutation.

### CLU-005: Applied state and replicated progress are atomic

The durable queue-state changes produced by a command and the durable record of
the applied replicated-log position MUST advance atomically.

### CLU-006: Leader changes preserve claim generations

Leader election MUST NOT reset receive counts, receipt generations, visibility
deadlines, or current receipt validity. A leader change MUST NOT independently
reapply a committed claim as a new claim.

### CLU-007: Proposal identity survives ambiguity

A request timeout or leader failure does not prove that a proposal failed.
Retries MUST use an operation identity that prevents a committed mutation from
being applied twice as two logical operations.

### CLU-008: Snapshots preserve all authoritative state

A snapshot and its restore process MUST preserve queue metadata, active
messages, indexes, claims, deadlines, receipt generations, deduplication state,
transfer state, and the applied log position required by enabled features.

### CLU-009: Membership changes preserve quorum safety

Adding, removing, or replacing replicas MUST use the consensus library's safe
membership-change mechanism. SimQ MUST NOT independently invent or bypass
consensus membership rules.

### CLU-010: Linearizable reads are leader-fenced

A clustered queue read MUST verify current leadership and pass a committed-log
barrier before reading local materialized state. Followers MUST NOT serve stale
queue state as if it were authoritative.

### CLU-011: Proposal replay is result-stable

The proposal ID, first committed result, and applied position MUST be durable.
Reapplying a committed proposal ID at a later log index advances replicated
progress without executing the queue mutation again and returns the same result.

### CLU-012: Restore publication is fail-closed

A snapshot candidate MUST pass physical and logical validation, including its
applied position, before replacing live queue state. Failed restore MUST leave
the prior valid state recoverable and the node unready.

## 17. Security and isolation invariants

These invariants apply even before authentication is implemented.

### SEC-001: Message data is not exposed by diagnostics

Health endpoints, readiness endpoints, metrics, traces, and ordinary logs MUST
NOT include message bodies, receipt handles, or secrets.

### SEC-002: Invalid input has no side effects

Rejected requests MUST NOT create queues, enqueue messages, claim messages,
delete messages, or partially update metadata.

### SEC-003: Resource limits are enforced before unsafe allocation

HTTP body, message body, batch size, queue name, and attribute limits MUST be
validated early enough to prevent unbounded memory allocation or work.

### SEC-004: Future tenant boundaries are authoritative

Once tenancy is introduced, every queue, message, receipt handle, transfer, and
administrative operation MUST be bound to a tenant. A tenant identifier supplied
only by an untrusted client MUST NOT be accepted without authentication.

### SEC-005: Authentication selects tenant authority

In OIDC security mode, only a successfully verified issuer, signature,
algorithm, audience, temporal validity, subject, and tenant claim may select a
tenant namespace. Authentication errors MUST NOT fall back to the legacy tenant.

### SEC-006: Tenant storage is non-aliasing

Two distinct accepted tenant identifiers MUST map to distinct authoritative
queue namespaces. Every repository and redrive operation MUST transform all
queue names and references before accessing state and strip them before return.

### SEC-007: Payload encryption precedes durability

In secure mode, message bodies and attributes MUST be authenticated-encrypted
before entering memory storage, bbolt, Raft commands, or snapshots. Decryption
failure, an unknown key ID, or context mismatch MUST return no payload.

### SEC-008: Quota decisions are atomic where authoritative

Persistent queue, active-message, and payload-byte quota checks and usage
changes MUST commit in the same transaction as their mutation. A transient rate
limiter MUST NOT be treated as authoritative stored usage.

### SEC-009: Authorization is deny-by-default

Every queue action in OIDC mode MUST map to a fixed required role. Unknown
actions and absent roles are denied. M2 permission metadata MUST NOT silently
become an authorization grant.

### SEC-010: Observability is bounded and redacted

Metric labels MUST use fixed enums or bounded hashes. Audit, metric, trace, and
ordinary log paths MUST NOT record bearer tokens, message payloads, attributes,
receipt handles, plaintext tenant IDs, ciphertext, or key material.

## 18. Sharding and placement invariants

### SHD-001: Catalog identity is canonical and durable

All nodes and snapshots that may serve one shard MUST use the same canonical
catalog revision. A malformed, missing-after-binding, or different revision
MUST fail before serving data or publishing a restore.

### SHD-002: Tenant routing is deterministic and singular

A valid tenant namespace digest and fixed catalog MUST always select exactly
one shard independent of process, manifest ordering, placement ordering, or
leadership. Non-namespaced legacy keys MUST select only the declared default.

### SHD-003: Tenant atomicity never crosses shards

All queues, messages, receipts, FIFO state, redrive state, move tasks, replay
records, and authoritative usage for one tenant MUST reside in one shard. No
operation may partially commit tenant state to another shard.

### SHD-004: Each shard retains its own quorum boundary

Success MUST follow durable commit and apply by the selected shard's quorum.
Leadership or availability of another shard MUST NOT acknowledge, read, or
repair the selected tenant's history.

### SHD-005: Placement changes preserve failure tolerance

A membership change MUST match the declared node and Raft address, retain at
least three voters, and leave no failure domain containing a quorum. Replica
relocation MUST catch up a nonvoter before removing the old durable voter.

### SHD-006: Unsupported resharding fails closed

Placement edits may not add or remove shard IDs, change the default shard, or
move existing tenants under the same catalog revision. Storage-key copying MUST
NOT substitute for a versioned, fenced ownership-transfer protocol.

## 19. Tenant relocation invariants

### REL-001: Ownership epochs are monotonic and authoritative

An explicit tenant ownership record MUST override hashing. Cutover MUST compare
the exact source shard and epoch and advance the epoch by exactly one.

### REL-002: At most one shard can write one tenant epoch

The source MUST commit a durable fence before destination ownership can commit.
The destination MUST NOT serve while merely prepared. Routing lag may reject
requests but MUST NOT permit concurrent source and destination writers.

### REL-003: Every data access checks the local fence

Foreground reads and mutations plus expiry and move workers MUST reject or skip
a tenant whose local state is frozen, prepared, moved, or at the wrong epoch.

### REL-004: Freeze and capture are atomic

The final source bundle MUST describe the same transaction that installs the
source fence. No acknowledged mutation may fall between that bundle and fence.

### REL-005: Tenant bundles are complete, canonical, and bounded

A bundle MUST include every authoritative tenant record and no record belonging
to another tenant. Canonical ordering and SHA-256 MUST detect omissions or
changes. Oversized, duplicate, malformed, or conflicting imports fail entirely.

### REL-006: Cutover is explicit, never inferred

Copied or prepared data MUST NOT change routing. Only the control Raft group's
compare-and-swap ownership commit changes the authoritative shard.

### REL-007: Every phase is crash-recoverable and idempotent

After any process, leader, or minority failure, retrying the recorded next phase
MUST either make progress once or return its already-completed result. Abort is
permitted only before cutover.

### REL-008: The tenant remains the atomic relocation unit

Queues, FIFO groups, redrive edges, move tasks, receipts, quotas, and encrypted
payloads for one tenant MUST move together. M8 MUST NOT split queues across
shards or decrypt payloads during transfer.

## 20. Elastic topology invariants

### TOP-001: Cluster identity is immutable

Dynamic catalog generations MUST retain the manifest-v2 cluster identity and
default shard. A different identity, default shard, or unplanned replica target
MUST fail before topology state or a snapshot is published.

### TOP-002: Catalog changes never implicitly move stored tenants

Every tenant data operation MUST materialize or observe one explicit ownership
record. A shard may become READY only after stored legacy tenant digests are
backfilled. Hashing a changed active set MUST NOT move an existing tenant.

### TOP-003: Assignment is a control-log compare-and-set

The first ownership record fixes shard and epoch 1. Concurrent first requests,
retries, and newer catalog generations MUST return that first assignment or a
conflict; they MUST NOT create two assignments.

### TOP-004: Only READY shards receive new tenants

CANDIDATE, DRAINING, and RETIRED shards MUST NOT be selected for a new tenant.
Existing owners on DRAINING remain routed there until an M8 ownership cutover.

### TOP-005: Selective hosting fails closed

A node without a selected shard MUST NOT fall back to another local repository,
create local data, or claim leadership. It may return only retryable
unavailability and a bounded configured entry hint.

### TOP-006: Activation is deployment-assisted and monotonic

Only a manifest-planned CANDIDATE may become READY. Catalog generation MUST
increase atomically with activation, and a READY shard MUST NOT return to
CANDIDATE.

### TOP-007: Drain reuses fenced ownership transfer

Drain MUST stop new assignment before moving data, run at most one tenant M8
migration per topology operation, and retire only after zero ownership records
name the shard. It MUST NOT copy live keys or dual write.

### TOP-008: Retired identities never alias new storage

Retirement MUST durably tombstone shard ID and incarnation. No later catalog,
manifest, restore, or operation may reuse either identity.

### TOP-009: Topology progress is durable and idempotent

Operation phase, cursor, and active migration reference MUST be replicated.
After process or leader failure, retry MUST resume or return the already
completed result without skipping a required M8 phase.

### TOP-010: Control-plane loss limits availability, not ownership safety

Existing explicit owners may continue only through their correct active epoch.
New tenant assignment and topology mutation MUST fail closed without the control
quorum. Stale catalog state MUST NOT authorize an unknown generation or shard.

## 21. Required M0 evidence

M0 MUST include automated evidence for at least the following cases:

| Test scenario | Invariants demonstrated |
|---|---|
| Create and retrieve a queue | API-001, API-002, QUEUE-001, QUEUE-002 |
| Send, receive, and delete through HTTP | MSG-001, MSG-002, MSG-006 |
| Twenty concurrent receives of one message | MSG-004, CON-001, CON-002 |
| Visibility expires using a fake clock | MSG-005, TIME-001, TIME-002, TIME-003 |
| Re-receive creates a new handle | RCP-001, RCP-002 |
| Stale handle races with a new claim | RCP-003, CON-003 |
| Malformed receipt handle | RCP-004, SEC-002 |
| Empty receive | MSG-008 |
| Invalid and oversized JSON requests | API-003, API-007, SEC-002, SEC-003 |
| Race detector over all packages | CON-001 |

M0 verification commands:

```bash
go test ./...
go test -race ./...
```

## 22. Required M1-B evidence

M1-B MUST add automated evidence for at least the following cases:

| Test scenario | Invariants demonstrated |
|---|---|
| Current handle extends, shortens, and resets visibility from one command timestamp | RCP-002, RCP-003, TIME-001 |
| Zero timeout and exact deadline-minus-one/exact-deadline receives use a fake clock | MSG-005, TIME-002, TIME-003 |
| Visibility change preserves handle, count, generation, timestamps, ID, and body | MSG-010, RCP-002, RCP-003, DUR-002 |
| Malformed, unissued, and cross-queue handles fail without mutation | MSG-007, RCP-002, RCP-004, SEC-002 |
| Stale and consumed issued handles succeed as no-ops | RCP-003, TIME-004 |
| Old visibility change races with re-receive | CON-001, CON-002, CON-005, RCP-003 |
| Current visibility change races with delete and cannot resurrect the message | CON-001, CON-005, TIME-004 |
| Deadline change rolls back on repository failure and HTTP reports 503 | DUR-002, DUR-004, SEC-001 |
| Changed deadline and unchanged receive metadata survive close/reopen | DUR-003, TIME-003 |
| SIGKILL restart preserves zero and extended visibility changes | DUR-003, DUR-004 |
| Strict JSON, required-field, integer-bound, and request-size tests | API-002, API-007, SEC-002, SEC-003 |
| Race detector covers memory and bbolt visibility operations | CON-001 |

The repository conformance suite MUST run the current, stale, consumed,
unissued, cross-queue, receive/change race, and delete/change race cases against
both the memory and bbolt repositories. Timing proofs use explicit timestamps or
injected clocks and never correctness sleeps.

## 23. Required M1-D evidence

M1-D MUST add automated evidence for at least the following cases:

| Test scenario | Invariants demonstrated |
|---|---|
| String, Number, Binary, custom type, 0/10/11 count, name/type/value and exact size validation | API-002, API-007, ATTR-001, SEC-002, SEC-003 |
| Different map orders produce the documented digest and storage order | ATTR-002, DUR-006 |
| Input mutation and receive projections cannot change stored bytes | MSG-010, ATTR-001, ATTR-003 |
| Receive projection supports empty, exact names, and `All` without lifecycle side effects | ATTR-003, MSG-002, LIFE-001–LIFE-003 |
| Attributes and digest survive close/reopen and SIGKILL | ATTR-001, ATTR-002, DUR-001, DUR-003 |
| Attribute-bearing enqueue rolls back completely on storage failure | DUR-002, DUR-004, SEC-001 |
| Get selects current queue settings and Set applies a validated partial update | QUEUE-002, QUEUE-005, QUEUE-007, SEC-002 |
| Set racing with Send observes one complete delay/retention snapshot | QUEUE-005, QUEUE-006, CON-001, CON-002 |
| Set racing with Receive observes one complete visibility snapshot | MSG-002, QUEUE-006, CON-001, CON-002 |
| Queue settings survive close/reopen/SIGKILL and do not rewrite old message deadlines | DUR-003, QUEUE-005 |
| New stores use v3; valid v2 and v1 stores migrate with distinct validated backups | DUR-005, DUR-006, MIG-001, MIG-002 |
| Corrupt source/backup and injected migration failures remain fail-closed | DUR-004, DUR-006, MIG-001, MIG-002 |
| Strict HTTP shapes, request IDs, 404/503 mapping, redaction, and remaining 501 actions | API-001–API-004, API-007, SEC-001–SEC-003 |
| Race detector covers both memory and bbolt configuration transitions | CON-001, QUEUE-006, QUEUE-007 |

The shared repository conformance suite MUST exercise attributes, queue-setting
updates, and both Set races against memory and bbolt. Time boundaries use an
injected clock. Migration fixtures MUST prove that pre-existing lifecycle and
receipt data are semantically unchanged apart from record-version and newly
initialized attribute fields.

## 24. Required M1-E evidence

M1-E MUST add automated evidence for at least the following cases:

| Test scenario | Invariants demonstrated |
|---|---|
| Omitted/zero wait remains short poll; 1 and 20 are accepted; invalid integer shapes and bounds do not claim | API-002, API-007, SEC-002, SEC-003 |
| An immediately eligible message returns without registering a persistent waiter | MSG-002, POLL-001, POLL-002 |
| An empty long poll expires with an empty array using a controllable timer and injected clock | MSG-008, TIME-001, TIME-002, POLL-004 |
| A send between claim/registration/recheck or while blocked wakes a waiter without lost delivery | MSG-001, POLL-002, POLL-003 |
| Delayed availability and visibility expiry wake exactly at their stored boundaries | MSG-005, LIFE-001, TIME-003, POLL-006 |
| Retention encountered while waiting remains terminal | LIFE-002, TIME-004, POLL-006 |
| Cancellation and server shutdown release waiter state without holding repository locks or changing messages | CON-004, POLL-001, POLL-004, POLL-005 |
| Several waiters with one or several messages preserve exclusive claim and queue isolation | MSG-004, MSG-007, CON-001, CON-002, POLL-007 |
| Spurious notifications only cause another authoritative claim | POLL-002, POLL-003 |
| Memory and bbolt share next-transition and claim semantics; restart preserves durable state but not waiters | DUR-003, POLL-001, POLL-006 |
| Repository errors become 503 without payload, receipt, attribute, or path disclosure | DUR-004, SEC-001 |
| Race detector covers waiter registration, notification, cancellation, enqueue, and claim | CON-001 |

Long-poll timing tests use an injected service clock and controllable timer.
They do not use sleeps as correctness evidence. The shared repository suite
checks lifecycle transition hints against both memory and bbolt.

## 25. Required M1-F evidence

M1-F MUST add automated evidence for at least the following cases:

| Test scenario | Invariants demonstrated |
|---|---|
| One and ten entries succeed for every action | API-001–API-004, BAT-001, BAT-002 |
| Empty, null, eleven, duplicate, and invalid IDs reject before mutation | API-007, BAT-002, BAT-005, SEC-002, SEC-003 |
| Partial success, all failure, and deterministic result order | BAT-001, BAT-003, BAT-007 |
| Send preserves body, delay, attributes, digests, queue defaults, and exact aggregate size boundary | MSG-001, MSG-010, ATTR-001, ATTR-002, QUEUE-006, BAT-004–BAT-006 |
| Delete preserves current, stale, consumed, unissued, malformed, and cross-queue receipt safety | MSG-006, RCP-002–RCP-004, BAT-004, BAT-006 |
| Visibility entries use their exact per-attempt command timestamps | MSG-005, RCP-003, TIME-001, TIME-003, BAT-004 |
| Concurrent batches and single operations preserve repository serialization | CON-001–CON-005, BAT-004, BAT-007 |
| Injected bbolt update failure cannot become a successful entry | DUR-002, DUR-004, BAT-006, SEC-001 |
| Close/reopen and forced process termination recover only committed entries | DUR-001–DUR-006, BAT-006 |
| Strict nested JSON and errors expose no payload, attribute, receipt, or path | API-007, SEC-001–SEC-003 |
| Shared service tests pass against memory and bbolt under the race detector | CON-001, BAT-001–BAT-008 |

Batch conformance MUST execute the same semantic suite against memory and bbolt.
Failure tests MUST distinguish request-level no-mutation errors from HTTP 200
entry-level results. No test may infer batch-wide atomicity or exactly-once retry.

## 26. Required M3 evidence

M3 MUST add automated evidence for at least the following cases:

| Test scenario | Invariants demonstrated |
|---|---|
| Exact receive threshold moves one message once under concurrent consumers | DLQ-001–DLQ-004, CON-001, CON-002 |
| Automatic transfer preserves payload/lifetime and resets documented delivery metadata | DLQ-002, DLQ-005, LIFE-002, LIFE-003 |
| Pre-move receipts remain harmless and cannot affect post-move claims | RCP-002, RCP-003, DLQ-005 |
| Policy self/cycle/stale-generation targets fail without mutation | QUEUE-008, DLQ-006, SEC-002 |
| Dead-letter source pages are lexical and relationship-revision bound | API-002, API-007, DLQ-006 |
| Task boundary excludes later arrivals and one RUNNING task exists per source | DLQ-003, DLQ-004, DLQ-007, CON-001 |
| Each manual move and progress increment commit or roll back together | DUR-002, DUR-004, DLQ-002–DLQ-004, DLQ-007 |
| Cancellation preserves committed moves; missing origin produces observable failure | DLQ-002–DLQ-004, DLQ-007 |
| Schema-v4 backup migration and RUNNING-task restart recovery succeed | DUR-003–DUR-006, MIG-001, MIG-002, DLQ-003 |
| Strict HTTP shapes and errors expose no message, receipt, task internals, or path | API-001–API-004, API-007, SEC-001–SEC-003 |
| Race detector covers policy, receive transfer, cancellation, and worker progress | CON-001, DLQ-001–DLQ-004 |

## 27. Required M7 evidence

M7 MUST add automated evidence for at least the following cases:

| Test scenario | Invariants demonstrated |
|---|---|
| Equivalent manifests with different ordering produce one catalog revision | SHD-001, SHD-002 |
| A database and snapshot reject a different persisted catalog revision | SHD-001, CLU-008, CLU-012 |
| Distinct tenant digests route deterministically and legacy keys use the default | SHD-002, SEC-006 |
| HTTP authentication selects the tenant shard before follower leader hints | SEC-005, SHD-002, CLU-010 |
| Unsafe address, voter-count, or failure-domain membership edits are rejected | CLU-009, SHD-005 |
| Three nodes host two Raft groups and both preserve FIFO after one node fails | FIFO-001–FIFO-004, CLU-004, SHD-003, SHD-004 |
| Tenant delete uses the public queue generation while receipts remain scoped | QUEUE-007, RCP-001–RCP-003, SHD-003 |

## 28. Required M8 evidence

M8 MUST add automated evidence for at least the following cases:

| Test scenario | Invariants demonstrated |
|---|---|
| Schema-v9 state migrates with a validated backup and snapshot round-trip | DUR-003–DUR-006, REL-001, REL-007 |
| Source freeze and bundle capture reject concurrent foreground mutation | CON-001, REL-002–REL-004 |
| A complete Standard/FIFO/redrive tenant bundle contains no other tenant | SEC-006, REL-005, REL-008 |
| Prepared destination remains unreadable before ownership CAS | REL-002, REL-006 |
| Stale source routing after cutover cannot read or write | CLU-010, REL-001–REL-003 |
| Retry after every phase crash reaches one completed ownership epoch | CLU-007, REL-006, REL-007 |
| Pre-cutover abort restores only the original source epoch | REL-001, REL-002, REL-007 |
| Three-node/two-shard FIFO relocation preserves order across leader failure | FIFO-001–FIFO-004, CLU-004, REL-002, REL-008 |

## 29. Required M9 evidence

M9 MUST add automated evidence for at least the following cases:

| Test scenario | Invariants demonstrated |
|---|---|
| Schema-v10 migrates to v11 with validated backup and snapshot state | DUR-003–DUR-006, TOP-001, TOP-009 |
| Concurrent first-use assignment stores one epoch-1 owner | REL-001, TOP-002, TOP-003 |
| Backfill plus candidate activation leaves every existing tenant in place | REL-001, TOP-002, TOP-006 |
| New tenants use READY shards and never CANDIDATE/DRAINING/RETIRED | TOP-003, TOP-004 |
| A node omitting the tenant shard returns an entry hint without local writes | SHD-003, TOP-005 |
| Interrupted activation and drain resume idempotently after leader failure | CLU-007, REL-007, TOP-007, TOP-009 |
| Drain preserves FIFO order and retires only after the final owner moves | FIFO-001–FIFO-004, REL-008, TOP-007 |
| Retired shard ID/incarnation reuse and stale restore are rejected | CLU-012, TOP-001, TOP-008 |

## 30. Required M10 evidence

M10 MUST add automated evidence for at least the following cases:

| Test scenario | Invariants demonstrated |
|---|---|
| A pull request runs exact-toolchain verify and a real three-process failure test | CON-001, DUR-001, CLU-004 |
| Ephemeral OIDC/TLS fixtures start two independent Raft shards and isolate two tenants | SEC-005–SEC-010, SHD-001–SHD-004 |
| A Raft-only minority partition remains API-reachable but cannot acknowledge writes | CLU-003, CLU-010, SEC-001 |
| The majority commits FIFO work and the reconnected replica converges | FIFO-001, FIFO-002, FIFO-006, CLU-004, CLU-007 |
| Stopped-node volume archives restore into recreated volumes with authoritative state intact | DUR-001–DUR-006, CLU-008, CLU-011 |
| Prometheus parses every rule and deterministic series fire and clear expected alerts | SEC-010 |
| Stable DNS voters bootstrap in Kubernetes and a leader Pod replacement preserves acknowledged state | DUR-001, CLU-004, CLU-009, CLU-010 |

Local kind evidence MUST NOT be presented as proof of independent physical
failure domains. Online file copying, unbounded fault injection, committed test
credentials, and cleanup that targets resources outside the named M10 project
or cluster are forbidden.

## 31. Forbidden implementation shortcuts

The following approaches violate this document unless the specification is
explicitly changed:

- Returning a message from `ReceiveMessage` before committing its in-flight
  state.
- Implementing receive as `Peek` followed by a later visibility update.
- Using one reusable receipt handle for the lifetime of a message.
- Letting a stale receipt handle delete the current receive generation.
- Using real sleeps as the primary proof of timing behavior.
- Returning success for an operation that failed to reach its active durability
  boundary.
- Silently dropping malformed fields or oversized input.
- Returning success from a stubbed or unimplemented action.
- Mutating replicated state from a local timer callback without a committed
  command.
- Reading time or randomness inside replicated state-machine application.
- Acknowledging replicated writes from a minority partition.
- Removing, skipping, or weakening a failing invariant test to make verification
  pass.
- Reading queue defaults with `Repository.Get` and later enqueueing or claiming
  in a different state-transition boundary once mutable queue settings exist.
- Retaining caller-owned message-attribute maps or binary slices.
- Depending on Go map iteration order for an attribute digest or durable record.
- Advancing a schema version before all record rewrites in the same transaction.
- Holding a bbolt transaction or memory repository lock while a long-poll
  request waits.
- Treating an enqueue notification or timer firing as ownership of a message.
- Wrapping all batch entries in one transaction and suppressing valid partial
  success, or reporting an entry successful before its own commit completes.
- Adding or removing a shard ID and relying on hashing to move live tenants.
- Copying tenant buckets between shards without durable ownership epochs,
  source fencing, verified cutover, and rollback.
- Applying a per-shard membership change without checking the planned address,
  voter floor, and failure-domain quorum result.
- Serving a prepared tenant before the control ownership compare-and-swap.
- Unfreezing a source after ownership has committed to the destination.
- Capturing tenant data before the durable source fence in a different
  transaction.
- Adding a READY shard to rendezvous inputs before explicit legacy assignment
  coverage is complete.
- Treating a non-hosted shard as an empty local repository or silently routing
  it to the default shard.
- Retiring a shard while an ownership record or unfinished migration names it.
- Reusing a retired shard ID, incarnation, or on-disk directory.

## 32. Changing an invariant

An invariant may change only when all of the following are present:

1. A documented product or correctness reason.
2. A corresponding update to `SPEC.md` when public behavior changes.
3. An Architecture Decision Record for storage, concurrency, durability, or
   cluster-safety changes.
4. Updated tests that demonstrate the new rule.
5. An explicit review of migration and compatibility impact.

Until those conditions are met, the existing invariant remains authoritative.
