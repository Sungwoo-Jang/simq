# SimQ

SimQ is a Go queue service with durable Standard and FIFO queues, visibility
timeouts, delayed delivery, dead-letter queues, redrive, tenant isolation,
encrypted payloads, Raft replication, and elastic multi-Raft sharding.
It also supports explicit tenant ownership, reviewed candidate-shard activation,
selective shard hosting, and resumable shard drain without implicit rehashing.

The HTTP API uses SimQ's JSON action contract. It is intentionally not an AWS
SQS wire-compatible emulator. Public behavior is defined in `SPEC.md`, while
the concurrency and durability rules are defined in `INVARIANTS.md`.

## Run locally

SimQ requires Go 1.26.7. From the repository root:

```sh
go run ./cmd/simq
```

The server listens on `http://localhost:9324` and stores durable state in
`./data/simq.db` by default. Check it with:

```sh
curl http://localhost:9324/healthz
curl http://localhost:9324/readyz
```

Create a queue:

```sh
curl -X POST http://localhost:9324/v1/sqs/CreateQueue \
  -H "Content-Type: application/json" \
  -d '{"QueueName":"orders"}'
```

For an ephemeral local instance, use the in-memory repository:

```sh
SIMQ_STORAGE=memory go run ./cmd/simq
```

On PowerShell, set the variable before running:

```powershell
$env:SIMQ_STORAGE = "memory"
go run ./cmd/simq
```

Stop the process with `Ctrl+C`. Run the complete verification gate with
`./scripts/verify.sh`, or `./scripts/verify.ps1` on Windows.

For a real three-process Raft cluster with persistent Docker volumes and a
black-box leader-failover test, run:

```powershell
.\scripts\integration.ps1
```

On Unix-like systems, use `make integration`. Requirements, retained-cluster
options, ports, cleanup, and tested failure cases are documented in
`docs/local-integration-testing.md`.

Cluster deployment and elastic-shard procedures are documented in
`docs/shard-operations.md` and `docs/topology-operations.md`.
