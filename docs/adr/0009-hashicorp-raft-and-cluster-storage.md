# ADR 0009: HashiCorp Raft and clustered storage

## Status

Accepted for M5.

## Decision

SimQ uses `github.com/hashicorp/raft v1.7.3` as its consensus implementation.
The version is pinned. SimQ implements the library's `LogStore` and
`StableStore` interfaces on the already selected `go.etcd.io/bbolt v1.5.0`, and
uses the library's file snapshot store and transports.

The Raft log database, snapshot directory, and schema-v7 queue-state database
are separate node-local durability domains. Raft decides ordering, quorum
commit, elections, and membership. The queue database is the deterministic FSM
materialization and snapshot payload; it does not decide replicated ordering.

## Rationale

Raft membership, elections, log repair, snapshot installation, and protocol
compatibility are consensus-critical infrastructure and must not be invented in
SimQ. HashiCorp Raft is a stable v1 Go module, has broad production use, exposes
the required FSM, snapshot, TCP, in-memory transport, and safe membership APIs,
and is distributed under MPL-2.0. Version 1.7.3 includes the post-1.7.0 Pre-Vote
fixes and is the selected release.

## Alternatives

- Implementing Raft locally was rejected because election, log-repair, joint
  safety, and snapshot edge cases would dominate M5 and violate CLU-009.
- `go.etcd.io/raft` was considered. It provides a strong consensus core but
  requires SimQ to build more transport, persistence, membership orchestration,
  and snapshot plumbing.
- An external consensus service was rejected because queue commits could not be
  made atomic with the authoritative applied position without a new distributed
  transaction boundary.
- `raft-boltdb` was not selected. Its small adapter surface is implemented
  locally so SimQ keeps exact bucket validation, failure injection, and one
  pinned storage dependency.

## Invariant coverage

- CLU-001: versioned deterministic command codec and explicit command inputs.
- CLU-002 through CLU-004: Raft quorum commit precedes FSM response.
- CLU-005: schema-v7 atomic queue mutation, proposal result, and applied index.
- CLU-006 and CLU-007: durable FSM state and proposal-result replay.
- CLU-008: complete queue-state snapshots plus validated restore.
- CLU-009: membership changes delegate to the library APIs.

## Maintenance and license

The selected module is actively maintained, has tagged releases and a Go module
definition, and uses MPL-2.0. M5 verification includes the dependency's behavior
through SimQ integration tests; it does not vendor or modify library source.

## Removal or replacement impact

Replacing HashiCorp Raft affects `internal/cluster`, clustered startup and
configuration, operational cluster endpoints, fault-injection tests, and this
ADR. It does not change the public queue service, standalone repositories, queue
schema semantics, or HTTP action payloads. Replacing the local Raft bbolt adapter
does not change the FSM or queue repository boundary.

