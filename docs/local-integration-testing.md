# Local integration testing

SimQ's local integration environment runs three independent server containers
and three persistent Raft volumes on an isolated Docker Compose network. A Go
black-box test drives the public HTTP API, discovers the leader, and forcibly
kills and restarts leaders while checking committed state.

## Requirements

- Docker Engine or Docker Desktop with Compose v2
- Go 1.26.7, or the repository-local toolchain at
  `.cache/toolchains/go1.26.7/go`
- host ports 19324, 19325, and 19326 available on loopback

The environment is local-development-only. Its fixed administration token and
unencrypted node traffic must not be copied into a shared or production
deployment.

## Run the suite

On Windows PowerShell:

```powershell
.\scripts\integration.ps1
```

On Unix-like systems:

```sh
./scripts/integration.sh
# or
make integration
```

The runner validates the toolchain and Docker daemon, removes stale resources
from the `simq-integration` project, builds the image, starts the cluster, runs
the tagged tests, captures diagnostics, and removes the containers and volumes.
It never removes unrelated Docker resources.

Diagnostics are written to `.cache/integration/compose-ps.txt` and
`.cache/integration/compose.log`, including after a failed test.

To retain the cluster after a Windows run:

```powershell
.\scripts\integration.ps1 -Keep
```

To reuse an image that has already been built:

```powershell
.\scripts\integration.ps1 -SkipBuild
```

On Unix-like systems, retain the cluster with:

```sh
SIMQ_INTEGRATION_KEEP=true ./scripts/integration.sh
```

When retained, the nodes are available at:

| Node | HTTP API | Raft address inside Compose |
|---|---|---|
| n1 | `http://127.0.0.1:19324` | `n1:7000` |
| n2 | `http://127.0.0.1:19325` | `n2:7000` |
| n3 | `http://127.0.0.1:19326` | `n3:7000` |

The Raft configuration retains these stable Compose DNS names. This is the
same address model used by the StatefulSet validation profile.

Stop and delete only this environment with:

```powershell
docker compose -p simq-integration -f deployments/local/compose.integration.yml down --volumes
```

## What the scenario proves

The current black-box scenario verifies:

- all three processes bind and answer health checks;
- only the leader serves linearizable queue actions and followers return the
  configured leader hint;
- acknowledged FIFO messages and sequence order survive an ungraceful leader
  kill;
- retrying the same operation ID after failover returns the original result
  without a duplicate enqueue;
- a snapshot succeeds and a stopped replica can rejoin from durable state;
- a second leader election preserves the stable queue URL.

This is a real three-process compatibility cluster for the default replicated
group. It is not yet the secure M9 multi-shard topology: that environment needs
test OIDC keys, TLS node identities, a reviewed manifest, and deterministic
tenant credentials. Those belong in a separate profile so the fast default
failure test remains understandable.

The integration suite complements `scripts/verify.ps1` or
`scripts/verify.sh`; it does not replace the unit, race, migration, or
invariant tests in that gate.
