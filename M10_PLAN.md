# M10 implementation plan

Status: complete with the recorded local kind host limitation. Delivered in
pull request #4.

M10 turns the M1-M9 correctness suite into a repeatable operational-validation
platform. It does not widen the public queue API. It proves that reviewed
deployment artifacts, security fixtures, failure injection, offline recovery,
alerts, and Kubernetes process replacement preserve the existing invariants.

## Architecture

Validation is split by cost and fidelity:

1. Every pull request runs the Go 1.26.7 verification gate and the fast
   three-container Raft failure test.
2. The secure profile generates a private CA, short-lived OIDC signing key,
   JWTs, TLS identities, encryption key, operator tokens, and manifest v2 into
   an ignored directory. Nothing generated is committed.
3. Docker networks separate client and Raft traffic. Tests may isolate only
   the Raft network while keeping the minority API reachable, proving that it
   fails closed and that the majority continues.
4. Disaster recovery is an offline volume backup. The source cluster stops,
   all three node volumes are archived, the volumes are deleted and recreated,
   and the archives are restored before application-level verification.
5. Prometheus rules are syntax-checked and evaluated against deterministic
   input series with `promtool`.
6. A pinned single-node kind cluster validates stable-DNS bootstrap, persistent
   volume reuse, leader Pod deletion, and acknowledged-data recovery. It does
   not claim to reproduce node loss or independent physical availability zones;
   those require a staging cluster.

Every runner uses a unique, fixed project or cluster name, bounded waits,
diagnostic capture, and scoped cleanup. The fast default test remains separate
from the heavier secure, recovery, alert, and Kubernetes profiles.

## Detailed tasks

- [x] M10-A: Add least-privilege GitHub Actions jobs for Go verification,
  Docker integration, and alert-rule validation with pinned action revisions.
- [x] M10-B: Generate ephemeral CA/OIDC/JWT/encryption/operator fixtures and
  run a TLS 1.3, mTLS, OIDC, manifest-v2, two-shard cluster in Docker Compose.
- [x] M10-C: Split client and Raft networks and prove minority fencing,
  majority progress, FIFO preservation, reconnection, and convergence.
- [x] M10-D: Stop the cluster, archive every node volume, recreate empty
  volumes, restore the archives, and verify tenant/FIFO state through HTTPS.
- [x] M10-E: Add bounded SLO alert rules and deterministic `promtool` tests for
  target loss, readiness probe loss, server errors, and latency.
- [x] M10-F: Accept stable DNS advertisements at bootstrap, provide a pinned
  three-Pod kind deployment and leader-deletion test, and fail fast on the
  current workstation's unsupported Docker cgroup v1 boundary. A successful
  kind execution still requires a cgroup v2-capable host.
- [x] M10-G: Document local, CI, nightly, release, and staging evidence
  boundaries plus exact cleanup and diagnostics procedures.
- [x] M10-H: Pass Go 1.26.7 verify, every available local operational profile,
  secret scanning, and deliver through a feature-branch pull request.

## Completion boundary

M10 is complete only when every repository-provided local profile has executed
successfully on the supported workstation, or an unavailable external
capability is explicitly reported without claiming evidence. Real cloud AZ
independence, provider OIDC integration, managed secret injection, and long
soak/load results remain staging or release evidence, not local evidence.
