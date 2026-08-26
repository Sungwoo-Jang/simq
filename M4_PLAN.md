# M4 implementation plan

M4 adds FIFO queues while keeping state transitions deterministic enough to be
driven by a replicated log in M5.

## Architecture

The service creates a complete command envelope before repository mutation. IDs,
receipt handles, retry IDs, and timestamps are command inputs; storage never
reads wall time or randomness. The current commit boundary is one memory lock or
bbolt update. M5 can place the same command boundary behind a quorum committer.

Strict order is scoped to `(QueueGeneration, MessageGroupId)`. Only the earliest
active message in a group may be claimed. A delayed or in-flight head blocks all
later messages in that group. Independent groups may progress concurrently.
Queue-global order is obtained by using one group ID.

Schema v6 preserves v5 queue/message records and adds FIFO side buckets for
queue configuration, logical sequence counters, message group metadata,
five-minute send-deduplication results, and five-minute receive-attempt results.
Logical public sequences never depend on a storage-engine physical sequence.
DLQ and move-task transfers are type-preserving, retain group and deduplication
IDs, and allocate a new sequence in the destination generation in the same
commit as the move.

## Detailed tasks

- M4-A: Specify queue type, group ordering, deduplication, sequence scope,
  receive-attempt behavior, errors, and metadata.
- M4-B: Record the local-committer/replicated-state-machine boundary in ADR 0008.
- M4-C: Add schema v6 FIFO buckets, v5 backup migration, validation, and recovery.
- M4-D: Implement immutable FIFO queue creation and configuration.
- M4-E: Implement atomic logical sequence allocation and five-minute send dedup.
- M4-F: Implement group-head blocking across delay, visibility, retention,
  deletion, type-preserving DLQ transfer, and message moves with destination
  resequencing.
- M4-G: Implement durable `ReceiveRequestAttemptId` replay semantics.
- M4-H: Extend single and batch HTTP requests/responses with FIFO fields.
- M4-I: Add memory/bbolt ordering, concurrency, rollback, migration, restart,
  DLQ, strict JSON, and race tests.
- M4-J: Run the Go 1.26.7 verification gate and update status.

## M5 compatibility constraints

- A committed command is the only source of mutation.
- State-machine application is deterministic outside its transaction.
- Success follows local durable commit now and quorum commit later.
- A minority partition fails closed instead of violating strict order.
- Queue generations and future routing epochs fence stale owners.
- Leader timers propose explicit expiry commands and never mutate state directly.
