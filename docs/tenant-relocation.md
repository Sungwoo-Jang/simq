# M8 tenant relocation operations

M8 moves one authenticated tenant between two existing M7 shard IDs. It does
not change the shard manifest, add shard IDs, or move individual queues. Run
relocation only while every node uses the same catalog revision and command
protocol, and while the source, destination, and default control shard each
have quorum.

All endpoints require `Authorization: Bearer $SIMQ_CLUSTER_ADMIN_TOKEN`. The
tenant digest is the lowercase hexadecimal SHA-256 of the exact verified tenant
claim. Migration IDs must start with `tm_` followed by 32 lowercase hexadecimal
characters and must never be reused for another move.

## Start and advance a relocation

Create a durable migration on the default shard leader:

```sh
curl -X POST https://node.example/v1/cluster/tenant-migrations \
  -H "Authorization: Bearer $SIMQ_CLUSTER_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"MigrationID":"tm_0123456789abcdef0123456789abcdef","TenantDigest":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","DestinationShard":"s1"}'
```

Inspect the next durable action:

```sh
curl -H "Authorization: Bearer $SIMQ_CLUSTER_ADMIN_TOKEN" \
  "https://node.example/v1/cluster/tenant-migrations/status?MigrationId=tm_0123456789abcdef0123456789abcdef"
```

Send one advance request at a time to `NextLeaderUrl` until `Phase` is
`COMPLETED`:

```sh
curl -X POST https://next-leader.example/v1/cluster/tenant-migrations/advance \
  -H "Authorization: Bearer $SIMQ_CLUSTER_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"MigrationID":"tm_0123456789abcdef0123456789abcdef"}'
```

An advance performs exactly one idempotent action. A leader change or network
error is handled by reading status again and retrying the reported next action.
Tenant queue operations are temporarily unavailable after source freeze; they
never fall back to the hash-selected shard. A complete tenant bundle is limited
to 16 MiB, so reduce tenant state before retrying a move rejected for size.

## Abort before cutover

Abort is allowed only before the ownership epoch changes. Start abort on the
default shard leader, then keep using the normal advance endpoint until phase
`ABORTED` has no `NextAction`:

```sh
curl -X POST https://control-leader.example/v1/cluster/tenant-migrations/abort \
  -H "Authorization: Bearer $SIMQ_CLUSTER_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"MigrationID":"tm_0123456789abcdef0123456789abcdef"}'
```

After cutover, do not attempt manual rollback. Continue advancing so the
destination activates and the frozen source is cleaned.

## Recovery and diagnosis

- `GET /v1/cluster/tenant-migrations?Limit=100` lists durable migrations.
- A `503` with `X-SimQ-Leader` means retry the same idempotent request at that
  leader; otherwise read status to refresh `NextLeaderUrl`.
- `FREEZING`, `PREPARING`, or `CUTTING_OVER` may be aborted. An `ABORTED`
  migration can still report local cleanup work and must be advanced until
  `NextAction` is empty. `ACTIVATING` and `CLEANING` must be driven forward.
- Never delete fence, bundle, ownership, or migration records manually. Restore
  quorum and resume the state machine.
- Snapshot and restore remain per shard. Recover the default, source, and
  destination groups at compatible snapshot points, then read migration status
  before allowing normal operation.
- Keep the M6 encryption key ring available on the destination; relocation
  moves encrypted envelopes and does not decrypt or re-encrypt payloads.

Source cleanup leaves a durable moved fence, allowing stale routing to fail
closed. A later migration can safely return the tenant to that shard after the
previous cleanup has removed all tenant data.
