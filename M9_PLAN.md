# M9 implementation plan

Status: complete.

M9 adds an elastic logical shard catalog on top of M8 ownership epochs. A
version-2 deployment manifest declares the immutable cluster identity and the
allowed replica universe, while the default shard's Raft FSM decides which
non-default shard IDs are candidates, ready for new tenants, draining, or
retired. Operators may change the active catalog without changing the cluster
identity or allowing rendezvous hashing to move live data.

## Architecture

Every tenant receives an explicit ownership record before its first data access.
Existing M8 tenant digests are backfilled against the legacy catalog before the
first candidate shard may become READY. Once that barrier completes, catalog
generation changes affect only previously unseen tenants; existing tenants move
only through the M8 fenced relocation protocol.

Manifest v2 may omit a node from a non-default shard, enabling selective data
placement. Every API node still hosts the default control shard. Every possible
default-shard voter also remains a replica of every active/candidate data shard
so a control leader can observe M8 phase receipts without a cross-Raft
transaction. Other nodes may host only their selected data shards. A node that
does not host a tenant shard returns the shard's configured entry API URL; that
hosting node can return the exact current leader.

The deployment provisions and starts candidate replicas before activation. M9
does not open an arbitrary unreviewed network endpoint from an HTTP request.
The runtime topology state machine is:

```text
candidate: CANDIDATE -> READY
drain:     READY -> DRAINING -> RETIRED
directory: LEGACY -> BACKFILLING -> EXPLICIT
```

A retired shard ID and incarnation are tombstoned and never reused. Drain stops
new assignment immediately, then drives one M8 relocation at a time until no
ownership record names the shard. Physical data-directory deletion remains an
explicit deployment action after retirement.

## Detailed tasks

- [x] M9-A: Specify manifest v2, cluster identity, catalog generations,
  explicit assignment, selective placement, lifecycle, and recovery invariants.
- [x] M9-B: Add schema v11 catalog, topology-operation, and shard
  tombstone state with a validated schema-v10 backup.
- [x] M9-C: Replicate topology command protocol v3 and initialize an M8 catalog
  without changing existing tenant placement.
- [x] M9-D: Materialize first-use tenant ownership with CAS, batch-backfill all
  stored tenant digests, and close the explicit-directory barrier.
- [x] M9-E: Support selective local repositories plus safe remote entry hints
  while keeping the default control shard local on every API node.
- [x] M9-F: Implement idempotent candidate activation, new-tenant assignment to
  READY shards, drain, sequential M8 relocation, retirement, and ID tombstones.
- [x] M9-G: Add token-protected topology/catalog, operation, backfill, activate,
  drain, advance, and abort administration APIs.
- [x] M9-H: Preserve topology state in snapshots and recover command retries,
  process failure, leader changes, stale generations, and interrupted drains.
- [x] M9-I: Test upgrade, deterministic assignment, selective hosting,
  activation, no implicit movement, drain, retirement, and multi-node failure.
- [x] M9-J: Update status, tasks, and elastic-topology operations documentation.
- [x] M9-K: Pass the Go 1.26.7 verification gate and deliver through a feature
  branch pull request without pushing directly to main.

## Explicit non-goals

- Removing or replacing the default control shard.
- Reusing a retired shard ID or incarnation.
- Splitting one tenant across shards.
- Automatic metric-driven balancing or an unbounded relocation fan-out.
- Transparent server-side proxying of public queue requests.
- Activating arbitrary replica addresses that were not reviewed in manifest v2.
- Automatically deleting retired shard files.
