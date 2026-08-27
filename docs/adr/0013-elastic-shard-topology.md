# ADR 0013: Elastic logical shard topology

## Status

Accepted for M9.

## Decision

The default shard stores a replicated, generation-numbered logical catalog and
topology operations. Deployment manifest v2 supplies a stable cluster identity
and the complete allowed replica plan. Runtime administration may activate,
drain, and retire only non-default shard IDs present in that plan.

Tenant placement becomes explicit. Before any tenant operation reaches a data
shard, the control FSM compare-and-sets an ownership record. M8 tenants without
a record use the legacy READY set while the first-use assignment and background
backfill run. Candidate activation is forbidden until the backfill completion
barrier commits. Later catalog generations therefore affect only tenants with
no stored state.

Nodes may omit non-default replicas, but every node hosts the default control
group. Every possible control voter also keeps a replica of every data group so
the control leader can observe M8 data-phase state. A non-hosting API node fails
closed with a configured hosting-node entry hint and never creates local state
for that shard.

Shard creation is deployment-assisted: candidate replicas and Raft membership
are provisioned from the reviewed manifest before the control catalog makes the
shard READY. Draining removes the shard from new assignment, serially relocates
its explicit owners with M8, verifies zero remaining owners, and commits a
permanent retired-ID tombstone. File deletion is outside the replicated state
machine.

## Rationale

Changing the input set of rendezvous hashing would silently move existing
tenants. Explicit assignment makes catalog changes independent of live data.
Predeclared endpoints retain M7 placement review and prevent an administrator
request from making processes bind or dial arbitrary addresses. Serial reuse of
the M8 state machine keeps one tested ownership cutover instead of introducing
dual writes or a second transfer protocol.

## Dependency decision

M9 adds no dependency. It reuses Go 1.26.7, bbolt, and HashiCorp Raft v1.7.3.
Removing or replacing this design affects manifest parsing, schema-v11 control
records, routing, process startup, M8 relocation, snapshots, administration,
and recovery tooling.

## Alternatives

- Rehashing on catalog edits was rejected because it moves live tenants without
  a fence.
- Arbitrary runtime listener creation was rejected because it bypasses reviewed
  placement and complicates process-level port ownership and TLS identity.
- Concurrent drain fan-out was rejected because bounded serial movement gives
  deterministic load, recovery, and operator cancellation.
- A new external metadata store was rejected because the default shard already
  provides quorum replication and linearizable mutation ordering.
- Reusing retired IDs was rejected because stale snapshots and requests could
  alias a new shard incarnation.

## Compatibility

Manifest v1 opens exactly as M8 and treats every listed shard as initially
READY. Migration to schema v11 preserves the bound M8 catalog revision as the
default cluster identity. Manifest v2 makes the identity explicit and may add
CANDIDATE shards without changing it. Command protocol v3 is required before
topology mutations; mixed v2/v3 membership is rejected until compatibility is
explicitly widened.
