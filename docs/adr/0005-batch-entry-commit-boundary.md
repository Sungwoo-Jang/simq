# ADR 0005: Independent batch-entry commit boundaries

- Status: Accepted
- Date: 2026-08-23
- Scope: M1-F

## Context

M1-F adds `SendMessageBatch`, `DeleteMessageBatch`, and
`ChangeMessageVisibilityBatch`. Official Amazon SQS documentation limits each
request to ten entries, uses request-local IDs, reports entry results
individually, and permits HTTP 200 responses containing failures. SimQ must add
that domain behavior without weakening existing single-message validation,
receipt generations, queue snapshots, commit-before-success, or concurrency.

A repository-level batch transaction would make all entries share one commit.
That conflicts with required partial success and would require defining whether
an invalid or failed entry aborts unrelated valid entries. It would also create
new durable command and retry semantics before operation identities exist.

## Decision

The HTTP layer strictly decodes the complete request and maps QueueUrl. The
queue service validates all request-wide constraints before mutation: count,
ID syntax and uniqueness, queue existence, and Send aggregate logical size.
It then attempts entries in input order through the established single-message
service methods.

Each valid Send calls one `Enqueue`; each Delete calls one `Delete`; and each
visibility change calls one timestamped `ChangeVisibility`. Memory serializes
each transition under its existing mutex. bbolt uses the existing synchronized
`DB.Update` per entry with `NoSync=false`. A success result is appended only
after that call returns successfully. Validation, receipt, storage, and internal
errors become redacted entry failures, and later entries are still attempted.

Both response arrays preserve relative request order. The service does not run
entries concurrently inside one request; separate batch and single requests may
interleave at repository transition boundaries. No batch-wide atomicity or
exactly-once retry is claimed. Existing message IDs, receipt handles, and
timestamps remain the only operation values; ambiguous Send retries may enqueue
new messages.

The on-disk schema and record version remain 3. No persistent batch record,
batch ID, or new index is introduced. No external dependency is added.

## Alternatives considered

- One repository transaction for the entire batch was rejected because a
  single commit failure would suppress valid partial success and invent a
  batch-wide atomicity promise.
- Repository-native multi-entry methods with savepoints were rejected because
  bbolt has no nested savepoints and memory/bbolt failure semantics would become
  unnecessarily different.
- Concurrent entry execution was rejected for M1-F because it makes response
  ordering, clock sampling, failure injection, and local writer contention more
  complex without changing the ten-entry limit or public correctness.
- Persisted idempotency keys were deferred because their identity, retention,
  migration, and ambiguous-retry contract is an open product decision.

## Consequences

- Existing single-action tests and invariants directly govern every entry.
- A bbolt batch of ten successful entries performs ten synchronized commits;
  throughput is intentionally secondary to explicit partial durability.
- An earlier entry may be committed when a later entry fails. Clients must
  inspect both result arrays and choose retries per entry.
- Concurrent requests observe a legal interleaving of single authoritative
  transitions. Response order is deterministic even though repository order
  across requests is not.
- M5 must attach durable proposal identities before replicated or exactly-once
  retry semantics can be claimed.

## References

- [AWS SQS SendMessageBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html)
- [AWS SQS DeleteMessageBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessageBatch.html)
- [AWS SQS ChangeMessageVisibilityBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibilityBatch.html)
- [AWS SQS BatchResultErrorEntry](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_BatchResultErrorEntry.html)
