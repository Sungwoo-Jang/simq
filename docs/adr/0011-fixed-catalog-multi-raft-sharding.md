# ADR 0011: Fixed-catalog multi-Raft sharding

## Status

Accepted for M7.

## Decision

SimQ partitions secure tenants across a fixed catalog of shard IDs. Rendezvous
hashing maps the SHA-256 tenant namespace digest to exactly one shard. Every
queue, receipt, FIFO record, redrive relationship, move task, usage counter,
and encrypted payload for that tenant therefore shares one Raft state machine.
Legacy non-namespaced data maps to an explicit default shard.

Each API process opens an independent HashiCorp Raft group, bbolt repository,
log, stable store, and snapshot store for every configured shard. The M7
manifest records planned node IDs, unique Raft addresses, API URLs, failure
domains, bootstrap node, and initial voters. Its canonical revision includes
only the manifest version, default shard, and sorted shard IDs, allowing replica
placement to change without reassigning data.

Schema v9 binds every shard database and restored snapshot to that canonical
revision. A mismatch fails closed. Membership operations validate the placement
plan and simulate the resulting voter set before applying it. Each shard retains
at least three voters and no one failure domain may contain a quorum.

## Rationale

Tenant-level sharding preserves M1-M6 atomicity and strict FIFO ordering without
introducing distributed transactions between queues. Independent consensus
groups isolate write ordering and failure while reusing the already-audited M5
repository/FSM boundary. A fixed catalog prevents an innocent configuration
change from silently remapping existing tenants.

## Dependency decision

M7 adds no dependency. It reuses the pinned HashiCorp Raft v1.7.3 dependency
selected by ADR 0009 and Go 1.26.7 standard-library hashing, JSON, and filesystem
facilities. Removing or replacing Raft still affects the layers recorded by ADR
0009; M7 additionally requires replacement support for multiple independent
groups and per-shard membership.

## Alternatives

- Per-queue sharding was rejected because redrive and tenant quota operations
  would require cross-shard transactions and FIFO-related state could split.
- A single larger Raft log was rejected because it retains one write leader and
  one failure/throughput domain.
- Adding shard IDs dynamically was deferred because rendezvous hashing would
  remap existing tenants without a migration protocol.
- Transparent tenant movement was deferred because safe cutover requires
  durable ownership epochs, source fencing, copy verification, and recovery.
- Accepting a stale catalog or snapshot was rejected because it can make the
  same tenant resolve to different authoritative histories.

## Invariant coverage

- SHD-001 and SHD-002: canonical catalog binding and deterministic single-shard
  tenant routing.
- SHD-003: the tenant boundary keeps FIFO, redrive, quota, and queue state in
  one ordered state machine.
- SHD-004 and SHD-005: independent quorum commit and failure-domain-safe planned
  membership changes.
- SHD-006: catalog mismatch and unsupported resharding fail closed.

## Removal or replacement impact

Changing routing affects `internal/shard`, tenant namespace binding, persisted
catalog interpretation, placement tooling, backup inventory, and every
multi-shard test. Changing the manifest affects runtime configuration and the
operator rollout contract. Tenant resharding must introduce a new versioned
ownership protocol rather than weakening schema-v9 catalog binding.
