# M7 implementation plan

Status: complete. Verified on 2026-08-26 with Go 1.26.7 and
`./scripts/verify.ps1`.

M7 scales the M6 secure multi-tenant service across a fixed catalog of
independent Raft shards. It preserves the ordering and atomicity boundary by
placing every queue and all related state for one tenant in exactly one shard.

## Architecture

A strict deployment manifest defines a version, default shard, fixed shard IDs,
and the planned replicas and failure domain for every shard. Its canonical
catalog revision is derived only from the version, default shard, and sorted
shard IDs. Every shard persists that revision in schema v9 and rejects a
different catalog or snapshot.

The authenticated tenant digest is routed with rendezvous hashing. Legacy
non-namespaced data remains on the configured default shard. Each process hosts
one independent Raft repository per configured shard, and an operation-scoped
repository chooses the shard before leader routing, authorization, quota, FIFO,
redrive, or storage mutation is evaluated.

Replica placement may change without changing the catalog revision. Planned
membership changes validate node ID, Raft address, minimum voter count, and
failure-domain quorum safety. Online relocation is a whole-shard replica
operation: add a nonvoter, wait for catch-up, promote it, then demote or remove
the old replica.

## Detailed tasks

- [x] M7-A: Specify the fixed shard catalog, tenant routing, compatibility,
  placement, recovery, and explicitly deferred resharding contracts.
- [x] M7-B: Record the multi-Raft and fixed-catalog decision in ADR 0011.
- [x] M7-C: Add strict manifest validation and a deterministic canonical catalog
  revision independent of JSON and placement ordering.
- [x] M7-D: Add schema v9 catalog binding, validated v8 backup migration, and
  snapshot restore rejection for a mismatched catalog.
- [x] M7-E: Host an independent durable Raft group and FSM per shard while
  preserving repository and queue-service separation.
- [x] M7-F: Route an authenticated tenant to one shard before leader redirects,
  and keep legacy keys on the default shard.
- [x] M7-G: Add token-protected shard status, membership, and snapshot APIs.
- [x] M7-H: Enforce planned Raft addresses, at least three voters, and
  failure-domain quorum safety during membership changes.
- [x] M7-I: Add deterministic routing, catalog, placement, API isolation, schema,
  and real three-node/two-shard failover and FIFO tests.
- [x] M7-J: Run the Go 1.26.7 verification gate and publish final status.

## Safety boundaries

- One tenant and all of its queues, FIFO groups, redrive edges, move tasks,
  quota counters, and encrypted messages are authoritative in one shard.
- A persisted catalog revision is immutable. Startup and snapshot restore fail
  closed when a configured catalog could reinterpret existing data.
- Placement edits may alter replicas but MUST NOT add or remove shard IDs or
  change the default shard under an existing catalog revision.
- No M7 API moves a tenant between shard IDs. Online tenant resharding requires
  a later protocol with dual-write/cutover fencing and is deliberately deferred.
- Membership changes remain leader-only and cannot reduce a shard below three
  voters or place a quorum in one failure domain.
- A successful write still means the selected shard's Raft quorum durably
  committed it; another shard cannot acknowledge or recover that tenant's data.
