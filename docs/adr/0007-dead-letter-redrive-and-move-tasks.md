# ADR 0007: Atomic dead-letter transfer and durable move tasks

## Status

Accepted for M3.

## Context

Dead-letter handling crosses two queue orderings and message-move tasks span
many commits. Splitting removal, insertion, and progress updates would permit a
crash to lose a message, duplicate it, or report progress that never committed.
Queue names can also be deleted and recreated, so names alone are unsafe durable
references.

## Decision

- Store every policy, origin, and task endpoint as a queue name plus immutable
  queue generation ID.
- Perform automatic source-to-DLQ transfer inside the same repository
  transaction used by `Claim`.
- Perform each manual message transfer and task-counter increment in one
  repository transaction.
- Preserve message IDs across both transfer types. Message-ID history remains
  global and prevents a transfer retry from allocating a second logical message.
- Snapshot the source order high-water mark when a move task starts.
- Persist task status and progress. Resume RUNNING tasks after process restart.
- Use a dedicated redrive revision for source-list pagination.
- Reject policy cycles and deletion of a currently referenced DLQ or a queue
  participating in a RUNNING move task.

## Consequences

The bbolt implementation gets schema version 5 with redrive-policy,
redrive-origin, and move-task buckets. The in-memory implementation mirrors the
same transitions under one mutex. Transfer throughput is intentionally bounded
to one atomic message step at a time, favoring correctness and predictable
recovery over bulk transaction size.

Default-destination tasks can fail when an original source generation no longer
exists. This state is durable and observable; already moved messages are not
rolled back.
