# ADR 0003: Message attributes, mutable queue settings, and schema version 3

- Status: Accepted
- Date: 2026-08-23
- Scope: M1-D

## Context

M1-D adds immutable user message attributes and mutable access to the three
existing queue settings. The current M1-C service reads queue configuration with
`Repository.Get` and later calls `Enqueue` or `Claim`. Once
`SetQueueAttributes` exists, those two operations can observe a configuration
that changes between the read and write. In particular a send could combine an
old delay with a new retention period if responsibility were split further, and
a receive could claim with a default visibility value that was no longer the
configuration at its state-transition boundary.

The durable message record is version 2 and has no field for user attributes.
The storage format must remain independent of Go map iteration and domain
structure layout, and startup must remain fail-closed for every supported
schema version.

The semantic reference is the official Amazon SQS documentation for
[message metadata](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html),
[MessageAttributeValue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_MessageAttributeValue.html),
[SendMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html),
[ReceiveMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html),
[GetQueueAttributes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_GetQueueAttributes.html),
and [SetQueueAttributes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SetQueueAttributes.html).
SimQ adopts the documented meaning and MD5 canonicalization, not AWS Query
protocol, SigV4, SDK wire compatibility, or every AWS attribute.

## Decision

### Domain and public representation

The queue domain owns a `MessageAttribute` value with a complete `DataType`, a
base type, and either a UTF-8 string or decoded binary bytes. A message owns a
map keyed by the case-sensitive attribute name plus the canonical full-set MD5.
Binary input and output are base64 strings only at the HTTP boundary. Every
boundary that retains or returns these values deep-copies the map and binary
slices.

The service owns common name, type, number, count, total-size, selection, and
digest validation. HTTP owns strict JSON shape and base64 decoding/encoding;
repositories do not duplicate public request validation. Repositories still
strictly validate stored bindings and records because stored bytes are an
untrusted recovery input.

The digest is the AWS-documented canonical byte sequence: attributes sorted by
name, four-byte big-endian lengths for name/type/value, one-byte transport type,
and raw decoded binary values. The complete custom type suffix participates in
the digest. Send returns the full-set digest. Receive computes the digest of its
returned response projection; an empty projection omits both fields.

### Atomic queue-configuration snapshot

We adopt the repository-transaction approach rather than revision/CAS:

- `SendMessage` validates the payload, generates the message ID and one command
  timestamp, and passes them with an optional delay override to an enqueue
  command.
- Under the memory repository mutex or one bbolt write transaction, enqueue
  reads the current queue record, chooses the queue delay or override, uses the
  current retention value, writes the final deadlines and all indexes, and
  returns the stored message.
- `ReceiveMessage` validates any visibility override, generates candidate
  receipt handles and one timestamp, and passes an optional override. Claim
  reads the current visibility default and claims messages in the same memory
  critical section or bbolt write transaction.
- `SetQueueAttributes` validates a partial update in the service and applies all
  supplied values under one memory lock or bbolt write transaction.
- Repositories never read the wall clock. A collision retry reuses the original
  command timestamp and validated payload.

The linearization order therefore yields one complete old or new configuration
snapshot. Existing messages retain their established availability and expiry;
Set does not scan or rewrite messages.

A configuration revision with CAS was rejected for M1-D because it would add a
retry protocol and starvation/failure surface without improving the single
writer and mutex-backed implementations. A revision may become useful for a
future replicated or sharded control plane, but the command timestamp and
transaction-owned configuration lookup already prepare the state transition
for deterministic replication.

### Storage records and schema version 3

New databases use schema version 3 and every queue, message, message-ID, and
receipt record uses record version 3. The message record adds:

- a non-null array of message attribute records sorted strictly by name; and
- `MD5OfMessageAttributes`, which is empty exactly when that array is empty.

Each attribute record has its own explicit record version, name, full data type,
base type, and exactly one string value or base64-encoded binary value. The
codec rejects an unsorted array, duplicates, unknown fields, inconsistent base
types/value fields, invalid public constraints, or a digest that does not match
canonical data. JSON is retained as the record envelope because strict
versioned structs already provide auditable fail-closed decoding. Array order,
not Go map order, provides deterministic storage.

Queue record fields do not otherwise change, but their record version advances
so a schema 3 validator cannot accept a partially rewritten database. Message
ID and receipt records likewise advance without changing their semantic data.

### Version 2 to version 3 migration

Startup holds bbolt's exclusive file lock for migration and follows this order:

1. Validate the entire source using schema-2 record types and graph checks.
2. Establish `<database>.schema-v2.bak` with mode `0600` using a bbolt
   transaction copy, synchronize the temporary backup, validate it as schema 2,
   then publish it without overwriting an existing path and synchronize its
   directory.
3. If the backup already exists, validate and reuse it. Any invalid existing
   artifact fails startup.
4. In one writable transaction rewrite every queue, message, message-ID, and
   receipt record to version 3. Existing messages receive an empty attribute
   array and empty attribute digest. Update the schema version only after all
   record rewrites succeed.
5. Check the commit result, then validate the complete schema-3 graph before
   opening the HTTP listener.

An error or panic aborts the write transaction, leaving schema 2 intact and the
validated backup available. Reopening safely repeats validation or observes a
fully committed schema 3 store. A migration never recomputes or changes
availability, expiry, visibility, receive count/generation, current or
historical receipt bindings, IDs, body/digest, or timestamps.

Schema 1 remains supported by running the existing v1-to-v2 migration first and
then v2-to-v3. Both `.schema-v1.bak` and `.schema-v2.bak` remain separate,
validated recovery artifacts. Neither is an online backup API; restore remains
an explicit offline operator procedure until that contract is designed.

## Durability and failure boundaries

`SetQueueAttributes` and attribute-bearing enqueue each use one `DB.Update` and
return success only after bbolt commits with `NoSync=false`. `DB.Batch` is not
used. Encoding, callback, commit, sync, or validation failures propagate through
the repository boundary and become the established HTTP 503 response without
payload, attribute, receipt, or path details. Memory applies the same logical
atomic transition under one mutex but remains explicitly non-durable.

## Consequences

- Message attributes are immutable, deterministic, restart-safe payload data.
- Queue setting updates and default-dependent sends/receives are linearizable on
  the single node and cannot observe a torn configuration.
- Enqueue and claim now require a write transaction even to read their default
  settings; this matches their existing mutating nature and bbolt's single
  writer model.
- Repository commands become richer, but HTTP and service remain unaware of
  bbolt records and migration mechanics.
- Startup migration scans and rewrites every record and creates a full backup,
  so its time and free-space requirement are proportional to database size.
- The ordered receive scan and bbolt mmap/file-growth/compaction constraints
  from ADR 0001 remain. M1-D introduces no online compaction or backup command.
- M5 may apply these deterministic commands in a replicated state machine and
  replace bbolt as the authoritative commit log. The domain command and codec
  boundaries localize that replacement to repository/storage wiring and
  migration tooling.

No new dependency is added. The implementation uses the standard library for
base64, strict JSON, arbitrary-precision decimal validation, byte ordering, and
MD5, and retains the bbolt v1.5.0 dependency justified by ADR 0001.
