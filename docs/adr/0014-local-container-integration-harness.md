# ADR 0014: Local container integration harness

## Status

Accepted.

## Decision

SimQ uses Docker Compose v2 to run an isolated three-node Raft cluster for
local black-box integration tests. The test process is a Go 1.26.7 test binary
that talks only to public HTTP and cluster-administration endpoints and invokes
Docker Compose for bounded SIGKILL/restart fault injection. The harness does
not import SimQ implementation packages.

The initial scenario creates a FIFO queue, verifies follower leader hints,
commits two messages, kills the current leader, retries a proposal with the
same operation ID, consumes the messages in order, takes a snapshot, rejoins
the old leader, and repeats leader failover. Every run begins with empty named
volumes and removes only the `simq-integration` Compose project unless the
operator explicitly retains it for diagnosis.

Docker is an external development dependency rather than a linked production
dependency. Container builds pin Go 1.26.7 and a minor Alpine image line. The
Go harness uses only the standard library, so it adds no module or production
runtime dependency. Moby and Docker Compose are Apache-2.0 projects; Docker
Desktop use remains subject to Docker's separate product terms.

## Rationale

In-process tests already exercise queue state transitions thoroughly, but they
cannot demonstrate real listener binding, container process death, persistent
volume reuse, Raft election, or client-visible leader hints. A black-box test
also prevents test-only access to internal state from hiding a broken public
integration boundary.

This scenario covers `API-006`, `FIFO-001`, `FIFO-002`, `FIFO-006`, `CLU-004`,
`CLU-007`, `CLU-008`, `CLU-010`, and `CLU-011`. It complements rather than
replaces deterministic unit, repository-reopen, race, and invariant tests.

## Alternatives

- Testcontainers for Go was deferred because this first harness needs only a
  fixed topology, and its SDK and transitive dependencies do not improve the
  assertions or fault model yet.
- Robot Framework was deferred because it adds a Python runtime and a second
  assertion language while process control still has to delegate to Docker.
- Kubernetes or kind was deferred because scheduling and manifest behavior are
  not the subject of this test and would make the local feedback loop heavier.
- Shell-only HTTP assertions were rejected because response decoding, polling,
  cleanup, and failure messages are safer and easier to maintain in typed Go.
- In-process multi-node tests remain valuable, but cannot replace a test that
  kills and restarts independent operating-system processes.

## Compatibility and replacement

The harness does not change `SPEC.md`, stored data, or the production binary.
Removing Docker Compose affects `Dockerfile`, `deployments/local`, integration
scripts, and black-box tests only. Replacing it with Kubernetes or another
orchestrator must retain scoped cleanup, fresh persistent storage by default,
the same public-API assertions, and the mapped invariant coverage.
