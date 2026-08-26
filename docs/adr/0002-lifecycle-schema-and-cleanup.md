# ADR 0002: Lifecycle schema and retention cleanup

Status: Accepted

Date: 2026-08-23

## Context

M1-C adds queue-level delivery delay, a per-message delay override, and message
retention. These values affect authoritative eligibility and must survive the
same crash boundary as the message. The M1-A schema has no availability or
retention deadline fields and version 1 readers reject unknown record fields.

Retention correctness cannot depend on a timer firing at exactly the deadline.
The service must reject expired messages at every authoritative operation while
also reclaiming their active records so an idle queue does not grow forever.

## Decision

Adopt schema and record version 2. Queue records store `DelaySeconds` and
`MessageRetentionPeriod` alongside `VisibilityTimeout`. Message records store
explicit Unix-nanosecond `AvailableAt` and `ExpiresAt` values. The domain model
continues to expose public sent and first-receive timestamps in milliseconds.

The send service samples its clock once. It resolves the optional per-message
delay against the durable queue default and supplies the resulting deadlines to
the repository. Repository code never reads wall-clock time.

The active message order remains one per-queue ordered bucket. M1-C does not add
separate delayed, visible, in-flight, or expiration indexes. Claim scans the
ordered records, removes expired entries, skips entries whose availability time
is in the future, and atomically claims eligible records. This is intentionally
O(n) for the correctness milestone. A deadline index may be introduced only
with atomic index/state updates and new invariant evidence.

An explicit `Expire` repository command removes expired message records and
their active-order entries across queues in one or more authoritative
transactions. Message-ID and receipt history are retained. Thus a previously
issued handle remains distinguishable from an unissued handle and becomes a
consumed no-op after retention cleanup.

## Version 1 migration

Opening a valid schema version 1 database performs a one-time version 2
migration before the HTTP listener starts.

1. Open the database under the existing exclusive process lock.
2. Fully validate it using the version 1 schema and record rules.
3. Create a consistent `0600` backup beside the database using a bbolt read
   transaction if the migration backup does not already exist.
4. Sync the backup and containing directory.
5. In one bbolt write transaction, convert every queue, active message,
   message-ID, and receipt record and finally update the schema version.
6. Commit with `NoSync=false` and validate the complete version 2 schema.
7. Keep the version 1 backup as the explicit rollback artifact.

The fixed backup suffix is `.schema-v1.bak`. A pre-existing backup must itself
be a valid version 1 snapshot; it is never overwritten silently. A callback,
encoding, allocation, write, commit, sync, or validation failure prevents the
server from listening. bbolt's copy-on-write transaction means a crash before
the migration meta-page commit leaves the main database at version 1; a crash
after it leaves a complete version 2 commit. The backup preserves an
operator-controlled downgrade artifact.

Version 1 defaults are deterministic and require no migration-time clock:

- queue delay: 0 seconds;
- queue retention: 345,600 seconds;
- message availability: stored `SentAtUnixMillis` converted to a time;
- message expiration: that converted time plus 345,600 seconds.

An old message may therefore already be expired when the new process starts.
It remains non-deliverable and is removed by the first authoritative claim or
retention sweep.

This ADR refines ADR 0001's general preference for copy-on-write migration into
a concrete bbolt procedure. The original version 1 snapshot remains on disk;
the main file is changed only by one atomic bbolt transaction.

## Cleanup scheduling

Production starts one local retention sweeper with a configurable interval,
defaulting to one minute. The ticker only schedules work. Every state-changing
pass receives one explicit timestamp from the queue service, preserving the
same time boundary used by requests and preparing for a future replicated
command.

Cleanup is best effort for physical reclamation but not for eligibility.
Receive and visibility-change paths enforce expiration transactionally even if
the sweeper is late or failing. Sweep errors are retried and logged without
queue payloads, receipt handles, or storage paths.

M5 must replace the local mutating sweep with a leader-proposed replicated
expiration command. A local timer callback must never independently mutate a
replicated state machine.

## Consequences

- Delay and retention survive restart without real-time test sleeps.
- Visibility changes cannot extend retention or resurrect expired messages.
- Active storage is eventually reclaimed even when a queue is never received.
- Claim and sweep cost is linear in active messages until a later indexed
  optimization.
- Receipt and message-ID histories continue to grow and require a separately
  specified safe garbage-collection policy.
- Downgrading to an M1-A binary is unsupported on the version 2 main file; the
  retained version 1 backup is an explicit operator rollback artifact.
