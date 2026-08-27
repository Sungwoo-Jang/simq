# M8 implementation plan

Status: complete.

M8 adds resumable, epoch-fenced relocation of a complete tenant between two
existing M7 shards. The fixed shard-ID catalog remains unchanged: adding or
removing shard IDs and selective shard hosting require a later catalog protocol.

## Architecture

The default shard is the replicated control authority for tenant ownership and
migration records. An absent ownership record means epoch 1 on the M7 rendezvous
shard. Starting a migration materializes that ownership before any data changes.

Source and destination shards persist local tenant fences. Queue routing reads
the locally applied control record, and every operation verifies that the
selected shard is active for the expected epoch. A stale router can therefore
cause a retry but cannot create a second writer.

Migration advances one idempotent phase at a time:

```text
FREEZING -> PREPARING -> CUTTING_OVER -> ACTIVATING -> CLEANING -> COMPLETED
      \-> ABORTING -> ABORTED
```

The source freeze and final tenant bundle capture are one replicated
transaction. The destination imports the complete bundle in a PREPARED state.
The control shard then commits ownership epoch `E+1`; only afterward may the
destination activate. Finally, the source data is removed. Crashes leave a
durable phase and idempotent next action.

## Detailed tasks

- [x] M8-A: Specify ownership epochs, single-writer fencing, migration phases,
  failure recovery, bounded bundles, and compatibility boundaries.
- [x] M8-B: Add schema v10 ownership, migration, fence, and bundle state with a
  validated schema-v9 backup.
- [x] M8-C: Replicate control, freeze/capture, prepare, activate, and cleanup
  operations through the existing deterministic Raft FSM.
- [x] M8-D: Route tenant operations through ownership overrides and reject a
  frozen, prepared, moved, or wrong-epoch shard.
- [x] M8-E: Implement idempotent phase advancement and next-leader hints across
  independent shard leaders.
- [x] M8-F: Add token-protected migration create, status, list, advance, and
  abort administration APIs.
- [x] M8-G: Preserve migration state in snapshots and recover every crash point
  without dual writers or partial publication.
- [x] M8-H: Add storage, routing, API, restart, and three-node/two-shard FIFO
  relocation tests.
- [x] M8-I: Update status, task, and tenant-relocation operations documentation.
- [x] M8-J: Pass the Go 1.26.7 verification gate and deliver through a PR.

## Safety boundaries

- One tenant ownership epoch has exactly one possible active writer shard.
- Source freeze commits before the final bundle is captured and no background
  expiry or move worker may mutate a frozen tenant.
- Destination data is not routable while PREPARED.
- Control cutover is a compare-and-swap from source epoch `E` to destination
  epoch `E+1`; it is never inferred from copied data.
- Old-epoch and wrong-shard requests fail closed with a retryable unavailable
  result and never fall back to rendezvous hashing.
- Bundle import is bounded, authenticated by SHA-256, and all-or-nothing.
- Abort is allowed only before ownership cutover. After cutover, recovery must
  finish destination activation and cleanup.
