# M9 elastic topology operations

M9 changes which reviewed shard IDs may receive new tenants without allowing a
catalog change to rehash existing tenants. Every tenant has an epoch-1 control
record before its first queue mutation. Later ownership changes use the M8
fenced relocation protocol.

All endpoints below require `Authorization: Bearer <cluster-admin-token>`. A
mutation is sent to the default control-shard leader; `503` responses include
`X-SimQ-Leader` when a leader hint is available. Reuse the same `OperationId`
after timeouts or leader changes.

## Manifest v2

Manifest v2 adds a stable 32-character lowercase hexadecimal `cluster_id` and,
for every shard, an immutable `incarnation` plus `initial_state` of `READY` or
`CANDIDATE`. The default shard must be `READY` and hosted by every API node.
Every possible default-shard voter must also be a planned replica of every data
shard, allowing any control leader to drive M8 safely. Other nodes may omit
non-default groups and return the candidate's reviewed bootstrap API URL.

Provision and start every candidate Raft group before activation. HTTP topology
operations never introduce an address absent from the manifest.

## One-time explicit-directory backfill

Before the first activation:

1. `POST /v1/cluster/topology/backfill` with
   `{"OperationId":"to_<32 lowercase hex>"}`.
2. Repeatedly call `POST /v1/cluster/topology/operations/advance` with the same
   body until `Status.Operation.phase` is `COMPLETED`.
3. Confirm `GET /v1/cluster/topology` reports `directory_mode: EXPLICIT` and
   `backfill_complete: true`.

Backfill scans bounded batches on every initially ready shard and writes only a
missing epoch-1 owner. Concurrent first use commits its owner first, so a batch
can neither overwrite it nor create two owners.

## Activate a candidate

1. Confirm its replicas are provisioned, healthy, caught up, and match manifest
   identity and addresses.
2. `POST /v1/cluster/topology/shards/activate` with
   `{"OperationId":"to_<hex>","ShardId":"s2"}`.
3. Advance the operation once and confirm the shard is `READY`.

Only previously unseen tenants may now select the new shard. Existing ownership
records are unchanged.

## Drain and retire

1. `POST /v1/cluster/topology/shards/drain` with a new operation ID and shard.
   The atomic `DRAINING` transition immediately excludes it from new assignment.
2. Repeatedly call the advance endpoint. M9 starts at most one deterministic M8
   migration, reports the required shard leader, completes it, then selects the
   next owner.
3. When zero owners remain, advance commits `RETIRED` and a permanent shard ID
   and incarnation tombstone.

`POST /v1/cluster/topology/operations/abort` can cancel a planned activation or
a drain before any owner has moved. If a drain migration is already prepared,
continue advancing its M8 abort first. A drain with a completed move cannot be
aborted. SimQ never deletes retired shard directories; archive or delete them
only under a separate reviewed deployment procedure after backups and catalog
state are verified.

Use `GET /v1/cluster/topology/operations?Limit=100` and
`GET /v1/cluster/topology/operations/status?OperationId=...` to resume any
interrupted operation. Complete Raft snapshots contain catalog, cursor,
migration, and tombstone state.
