# M3 implementation plan

M3 adds dead-letter queues and durable message-move tasks without weakening the
queue-generation, receipt, ordering, or crash-consistency rules completed in M1
and M2.

## Architecture

### Queue identity and redrive policy

Every live queue has the generation-bound ARN
`arn:simq:sqs:::<queue-name>/<queue-id>`. A `RedrivePolicy` queue attribute is a
JSON string containing exactly `deadLetterTargetArn` and `maxReceiveCount`.
`maxReceiveCount` is a decimal string from 1 through 1000. The empty string
removes the policy.

The repository stores policies separately from ordinary queue attributes. Both
the source and destination are generation-bound references. Policy updates are
atomic, reject self references and directed cycles, and maintain a dedicated
redrive revision used by stable dead-letter-source pagination. A queue that is
the target of a live policy cannot be deleted.

### Automatic dead-letter transfer

`Claim` remains the concurrency boundary. For an eligible message, it checks
the policy before issuing a new receipt. If `ReceiveCount >= MaxReceiveCount`,
one transaction removes the source order entry, resets delivery state, inserts
the same message ID at the tail of the DLQ, and records the original source
generation. The scan then continues so poisoned messages do not consume the
receive batch.

Automatic transfer preserves body, body digest, user message attributes,
attribute digest, original sent timestamp, and expiration deadline. It resets
current receipt handle, receive count, first-receive timestamp, visibility, and
delay. The internal receipt generation remains monotonic and issued receipt
history is retained, so a pre-move handle can never affect a later claim.
Retention therefore cannot be extended by automatic redrive.

### Durable move tasks

`StartMessageMoveTask` records a source-order high-water mark, initial message
count, requested rate, and optional destination in the same transaction that
enforces one RUNNING task per source generation. Only messages at or below that
boundary are eligible, so later arrivals are not silently absorbed into an
existing task.

The worker moves one message and advances the durable task counter in the same
transaction. A custom destination receives every selected message. Without a
custom destination each message returns to the original source recorded during
automatic transfer. Manual movement preserves message ID, body, and user
attributes, but creates a new enqueue lifetime: sent time becomes the move
time, delivery metadata is reset, the destination default delay is bypassed,
and expiration becomes move time plus the destination retention period.

Tasks have `RUNNING`, `COMPLETED`, `CANCELLED`, and `FAILED` states. Cancellation
does not undo committed moves. On startup the service resumes durable RUNNING
tasks. A missing original destination causes a task to become `FAILED`; already
committed moves remain visible and counted.

## Implementation tasks

- M3-A: Specify ARN, policy validation, thresholds, metadata reset, API shapes,
  pagination, cancellation, and error behavior.
- M3-B: Add schema v5 buckets, v4 backup/migration, records, validation, and
  repository contracts.
- M3-C: Implement memory and bbolt policy mutation, source listing, and atomic
  receive-time DLQ transfer.
- M3-D: Implement durable start/list/cancel/step operations and recovery worker.
- M3-E: Wire `RedrivePolicy`, `QueueArn`, and the four M3 HTTP actions.
- M3-F: Add conformance, migration, crash/restart, threshold, pagination,
  cancellation, stale-generation, and concurrent receive tests; run the full
  Go 1.26.7 verification suite.

## Dependency rule

M3 uses the Go standard library and the existing pinned bbolt dependency. No
new dependency is introduced.
