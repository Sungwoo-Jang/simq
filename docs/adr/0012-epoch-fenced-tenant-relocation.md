# ADR 0012: Epoch-fenced tenant relocation

## Status

Accepted for M8.

## Decision

SimQ relocates a complete tenant between existing fixed-catalog shards with a
durable ownership epoch and a single-writer state machine. The default M7 shard
stores authoritative ownership and migration records in its Raft FSM. Each data
shard stores local tenant fence state and a bounded migration bundle.

The source atomically freezes the tenant and captures its complete authoritative
records. The destination atomically imports them as PREPARED. A compare-and-swap
on the control shard changes ownership from `(source,E)` to `(destination,E+1)`.
The destination then becomes ACTIVE and the source is cleaned. Every transition
is idempotent and carries migration ID, tenant digest, source, destination, and
both epochs.

M8 keeps the M7 shard-ID catalog fixed and continues to host every configured
shard on every node. It does not add a general metadata service, dynamically add
shard IDs, or permit queues of one tenant to split across shards.

## Rationale

Copying data before changing hash routing can create two writers and divergent
FIFO histories. Ownership epochs make authority explicit. Persisting the source
bundle after a replicated freeze gives destination leaders a recoverable copy
without an external object store or cross-Raft transaction. Alternating durable
phases permits retries after any node or leader failure.

## Dependency decision

M8 adds no dependency. SHA-256, strict JSON, and bounded byte handling use the
Go 1.26.7 standard library. Control and data transitions reuse HashiCorp Raft
v1.7.3 and bbolt already covered by ADRs 0001 and 0009.

## Alternatives

- Dual writes were rejected because independently committed Raft groups cannot
  provide one FIFO order or atomic rollback.
- Changing rendezvous inputs was rejected because it silently reassigns every
  affected tenant.
- Copying live bbolt files was rejected because a shard contains other tenants
  and local Raft metadata.
- An external metadata database was deferred because the default shard already
  provides a durable quorum and deterministic FSM.
- Serving destination data before ownership cutover was rejected because stale
  routers could expose an uncommitted authority change.

## Invariant coverage

- REL-001 through REL-003: durable ownership epochs and local data fences allow
  at most one active writer.
- REL-004 and REL-005: frozen snapshot bundles are complete, bounded, hashed,
  and atomically prepared.
- REL-006 and REL-007: CAS cutover and idempotent recovery survive every phase.
- REL-008: tenant-wide movement preserves queue, FIFO, redrive, receipt, quota,
  and encryption boundaries.

## Removal or replacement impact

Changing this protocol affects schema-v10 records, cluster command dispatch,
tenant routing, background workers, snapshot validation, administration APIs,
and recovery tooling. A future dynamic catalog must migrate these ownership
records explicitly rather than returning to implicit hashing.
