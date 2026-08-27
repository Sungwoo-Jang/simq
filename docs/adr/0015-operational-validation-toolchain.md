# ADR 0015: Operational validation toolchain

## Status

Accepted for M10.

## Decision

M10 keeps application assertions in Go 1.26.7 and uses external tools only at
the orchestration boundary:

- Docker Engine and Compose v2 run isolated local process and network faults.
- GitHub Actions uses `actions/checkout` revision
  `11d5960a326750d5838078e36cf38b85af677262` (v4) and `actions/setup-go`
  revision `40f1582b2485089dde7abd97c1529aa768e1baff` (v5).
- Prometheus 3.12.0 `promtool` validates alert syntax and deterministic rule
  behavior from container digest
  `sha256:69f5241418838263316593f7274a304b095c40bcf22e57272865da91bd60a8ac`.
- kind 0.20.0 and its Kubernetes 1.27.3 node image provide the local
  Kubernetes test boundary. The local profile uses one Kubernetes node and
  three StatefulSet Pods. Newer and pinned kind/Kubernetes combinations were
  exercised on the current Windows Docker 20.10 cgroup v1 runtime, but their
  kubelets could not create the required nested cgroups. The Windows runner
  therefore rejects this host configuration before resource creation; a
  Docker backend with working nested cgroups (normally WSL2/cgroup v2) is
  required. Physical node-loss and failure-domain evidence remains a staging
  requirement.

The kind binary SHA-256 checksums are
`aa49e245e201583884fa079b64d8b648b2ef93edadea9d6dcca127114d87e5ca`
for Windows amd64 and
`513a7213d6d3332dd9ef27c24dab35e5ef10a04fa27274fe1c14d8a246493ded`
for Linux amd64. The node image is pinned to
`sha256:3966ac761ae0136263ffdb6cfd4db23ef8a83cba8a463690e98317add2c9ba72`.

Generated PKI, OIDC, JWT, and symmetric-key fixtures use the Go standard
library. They are short-lived, local-only, written below an ignored generated
directory with restrictive permissions where supported, and deleted by normal
cleanup. The secure fixture server is test infrastructure and is not included
in the production image.

Moby, Docker Compose, Prometheus, kind, Kubernetes, and the selected GitHub
Actions are Apache-2.0 projects. They are maintained upstream and widely used
for their respective infrastructure roles. No new Go module or production
runtime dependency is added.

## Rationale

Typed Go assertions retain the same strict JSON, retry, timeout, and invariant
language as the existing suite. Compose provides real process and network
boundaries without embedding a Docker SDK. Promtool evaluates PromQL semantics
instead of relying on textual rule inspection. kind exercises Kubernetes DNS,
service discovery, Pod replacement, and persistent volumes while remaining
disposable on a developer workstation.

## Alternatives

- Testcontainers was deferred because fixed reviewed topologies need no Docker
  SDK and the existing scripts already own lifecycle and diagnostics.
- Robot Framework was deferred because it would add Python and a second
  assertion language without improving the failure model.
- Toxiproxy was deferred because separate Compose networks can deterministically
  isolate Raft while preserving client reachability. Latency and packet-loss
  shaping may justify a later ADR.
- Minikube and k3d were deferred in favor of kind's small CI-focused boundary
  and digest-pinned node images.
- Hand-evaluating alert expressions was rejected because only Prometheus can
  authoritatively parse and evaluate its rule language.
- Online copying of live bbolt or Raft files was rejected. M10 recovery stops
  all writers before archiving volumes.

## Invariant coverage and replacement

The profiles exercise `DUR-001` through `DUR-006`, `FIFO-001`, `FIFO-002`,
`FIFO-006`, `CLU-003`, `CLU-004`, `CLU-007`, `CLU-008`, `CLU-010`, `CLU-011`,
`SEC-001`, `SEC-005` through `SEC-010`, `SHD-001` through `SHD-005`, and the
M8/M9 relocation and topology recovery boundaries when those scenarios are
enabled.

Replacing a selected tool affects only `.github/workflows`, `deployments`,
`monitoring`, `scripts`, and `tests/integration`. A replacement must preserve
version pinning, license review, bounded failure injection, scoped cleanup,
diagnostics, generated-secret exclusion, and the same black-box evidence.
