# Operational validation

M10 adds several deliberately separate validation profiles. A passing unit
suite is necessary, but it is not evidence that real processes, networks,
volumes, alert expressions, or Kubernetes replacement behave correctly.

## Evidence levels

| Level | Command or trigger | What it proves |
|---|---|---|
| Pull request | `scripts/verify.*`, `scripts/integration.*`, `scripts/alerts.*` | Exact Go toolchain checks, a real three-process Raft leader failure, and deterministic Prometheus rule evaluation |
| Local secure/DR | `scripts/secure-integration.*` | TLS 1.3, Raft mTLS, OIDC, two shards, Raft-only partitioning, minority fencing, majority FIFO progress, and offline three-volume restore |
| Local Kubernetes | `scripts/kind-integration.*` | Stable StatefulSet DNS, three Pods, PVC reuse, leader Pod deletion, and acknowledged-message recovery on one kind node |
| Staging | Environment-specific runbook | Independent nodes/failure domains, provider identity and secrets, ingress/load-balancer behavior, disruption budgets, and storage-class failure |
| Release/soak | Scheduled environment-specific workload | Capacity, latency SLOs, sustained fault recovery, backup retention, and restore-time objectives |

The local Kubernetes profile is not multi-node or multi-AZ evidence. A staging
run must distribute voters across real failure domains and inject node and
storage failures before making either claim.

## Commands

Windows requires Go 1.26.7, Docker Desktop with Compose v2, and PowerShell:

```powershell
.\scripts\verify.ps1
.\scripts\integration.ps1
.\scripts\alerts.ps1
.\scripts\secure-integration.ps1
.\scripts\kind-integration.ps1
```

Unix-like systems use the matching `.sh` scripts or these Make targets:

```sh
make verify integration alerts secure-integration kind-integration
```

All waits are bounded. Diagnostics are written below `.cache/integration*`,
which is ignored by Git. Normal completion removes named containers, networks,
volumes, generated certificates, signing material, tokens, and kind clusters.
Set `SIMQ_INTEGRATION_KEEP=true` on Unix or pass `-Keep` on Windows only when
interactive diagnosis is needed, then use the cleanup command printed by the
runner.

## Secure failure and recovery profile

The secure runner generates a fresh private CA, API/Raft identities, OIDC key
and discovery document, short-lived tenant JWTs, AES-256 key, and operator
tokens. The CA private key and OIDC private key are never persisted. Generated
client material lives only in `.cache/integration-secure/fixtures` and normal
cleanup deletes it.

The scenario keeps client networking available while disconnecting the Raft
leader from the Raft-only network. It asserts that the old minority returns a
service-unavailable result, that the majority commits FIFO messages, and that
order survives reconnection plus a majority-leader process kill. It then stops
all writers, archives all three named volumes, deletes and recreates empty
volumes, restores each archive, starts the cluster, and verifies the preserved
queue and message through the HTTPS API. Copying a live volume is unsupported.

## Alerts

`monitoring/prometheus/simq-alerts.yml` covers target loss, loss of a ready
leader, server errors, and elevated mean request latency. `promtool test rules`
runs against deterministic series in `simq-alerts.test.yml`; it validates rule
syntax and expected fire/clear behavior, not delivery to an Alertmanager or
on-call destination. Routing and notification delivery require staging tests.

## Kubernetes host requirement

The pinned kind runner needs a Docker engine whose nested kubelet cgroups work.
The Windows runner detects the known-incompatible Docker Desktop cgroup v1
configuration before creating resources and explains how to move to a WSL2 /
cgroup v2 backend. `-ForceUnsupportedCgroup` exists only to reproduce that
host-capability probe; a failed forced run is not recovery evidence.

When supported, the test builds `simq:kind`, creates only the `simq-m10` kind
cluster and namespace, applies the StatefulSet, sends an acknowledged FIFO
message, force-deletes the current leader Pod, and checks the same message after
a different Pod becomes ready. The runner captures resources, descriptions,
and Pod logs before deleting the named cluster.

## Continuous qualification

`.github/workflows/operational.yml` runs the heavy profiles when their direct
inputs change, on manual dispatch, and at 03:17 UTC every Monday on the default
branch. The secure job executes two clean end-to-end iterations by default:

```powershell
.\scripts\qualify-secure.ps1 -Iterations 2
```

```sh
make qualify-secure
```

The Linux runner uses a ten-minute outer timeout per secure iteration. The kind
runner downloads the reviewed Kubernetes 1.27.3 kubectl rather than relying on
the mutable runner image and has a fifteen-minute command timeout. GitHub job
timeouts remain a separate final bound.

Only `before-backup.log`, final Compose logs/status, Kubernetes resources,
descriptions, Pod logs, and small result summaries enter qualification artifact
directories. Volume archives, database files, PKI, JWTs, environment files,
tools, and build caches are excluded. `assert-safe-artifacts.sh` blocks upload
if a credential-shaped value is detected; passing diagnostics expire after 14
days.
