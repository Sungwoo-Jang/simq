# M5 implementation plan

Status: complete and verified on 2026-08-25 with Go 1.26.7.

M5 adds an opt-in, single-Raft-group clustered deployment while preserving the
standalone memory and bbolt modes completed through M4.

## Architecture

Every clustered mutation is a versioned command containing a proposal ID and
all nondeterministic inputs already produced by the service. HashiCorp Raft
orders and durably commits the command on a voter quorum. Each FSM applies the
command to its node-local bbolt queue store. The queue mutation, proposal result,
and applied Raft index commit in one bbolt transaction.

Only the leader serves queue reads and mutations. Reads execute `VerifyLeader`
and `Barrier` before consulting local state. Followers and minority partitions
fail closed with a leader hint. SimQ does not use stale follower reads in M5.

One cluster is one queue shard. Queue placement and cross-shard rebalancing stay
deferred. Raft snapshots contain the complete schema-v7 queue state, proposal
replay records, and applied position. Raft configuration entries remain under
the consensus library's membership rules.

## Detailed tasks

- M5-A: Specify quorum, consistency, proposal identity, errors, configuration,
  snapshot, membership, and compatibility contracts.
- M5-B: Record the pinned consensus dependency and replacement boundary in ADR
  0009.
- M5-C: Add schema v7, schema-v6 backup migration, applied-index metadata,
  proposal replay records, atomic replicated apply, and snapshot import/export.
- M5-D: Implement the versioned command codec and a `queue.Repository` wrapper
  that routes every mutation through Raft and every read through a leader
  barrier.
- M5-E: Implement a durable bbolt Raft log/stable store, file snapshots, TCP
  transport, deterministic bootstrap, shutdown, and health.
- M5-F: Implement leader hints, request-scoped operation IDs, cluster status,
  snapshot triggering, and safe Raft membership changes.
- M5-G: Integrate opt-in cluster configuration into `cmd/simq`, including
  separate queue, Raft-log, and snapshot paths.
- M5-H: Add deterministic replay, quorum/minority, leader failover, stale-leader
  fencing, FIFO ordering across failover, snapshot restore, restart, membership,
  compatibility, corruption, and transaction rollback tests.
- M5-I: Add multi-process TCP smoke coverage for acknowledged writes and
  failover.
- M5-J: Run the complete Go 1.26.7 verification gate and update project status.

## Safety boundaries

- A successful clustered mutation has been committed by a Raft voter quorum and
  applied on the responding leader.
- Queue state and applied index never advance in separate transactions.
- A repeated proposal ID returns its first committed result without reapplying.
- A node that cannot prove current leadership serves neither reads nor writes.
- Membership changes use Raft `AddVoter`, `AddNonvoter`, `DemoteVoter`, and
  `RemoveServer`; SimQ never edits peer configuration directly.
- Snapshot restore validates schema and applied position before the node becomes
  ready.
- Command protocol compatibility is checked before a node is admitted.
