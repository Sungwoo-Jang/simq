# ADR 0008: Deterministic FIFO state-machine boundary

## Status

Accepted for M4 and constraining M5.

## Context

Strict FIFO ordering is unsafe if selection, sequence allocation, deduplication,
or visibility can be decided independently by multiple replicas. Retrofitting a
replicated log around code that reads local time or randomness would make replay
diverge.

## Decision

- Scope strict delivery order to queue generation and message group.
- Treat enqueue, claim, acknowledgement, visibility, expiry, DLQ transfer, and
  administration as deterministic commands with explicit IDs and time.
- Use logical durable sequence counters for public FIFO sequence numbers.
- Commit every group-head and deduplication decision with its message mutation.
- Keep FIFO state in schema-v6 side buckets so M1–M3 records remain compatible.
- Model the memory lock/bbolt update as `LocalCommitter`; M5 replaces it with a
  leader and quorum committer while reusing the state transition rules.
- Prefer CP behavior: without a majority, mutation and receive fail closed.

## Consequences

One slow group head blocks only its group. Different groups retain parallelism.
Queue-global FIFO requires one group and one serial path. Five-minute send and
receive deduplication consume durable metadata after active messages change, and
cleanup is driven only by explicit command timestamps.
