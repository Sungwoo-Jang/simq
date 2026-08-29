# Amazon SQS semantics PoC

This PoC runs one backend-neutral worker scenario suite against SimQ or Amazon
SQS. It tests acknowledged deletion, crash redelivery, visibility extension,
FIFO group progression, FIFO send deduplication, and dead-letter redrive. It
does not make SimQ wire-compatible with the AWS SDK; the two adapters translate
the same semantic operations to their respective APIs.

## Local three-node SimQ run

Requirements are Go 1.26.7, Docker, and Docker Compose v2. From the repository
root on Windows:

```powershell
.\scripts\sqs-poc.ps1
```

On Unix-like systems:

```sh
make sqs-poc
```

The script creates a clean three-node Raft cluster, waits for every node,
executes all six scenarios through the leader-aware SimQ adapter, writes the
redacted result to `.cache/sqs-poc/simq-report.json`, scans artifacts for
sensitive-value patterns, and removes containers and volumes. Use `-Keep` on
PowerShell or `SIMQ_SQS_POC_KEEP=true` on Unix to retain the cluster for
inspection.

## Explicit Amazon SQS run

An AWS run creates and deletes disposable queues and can incur service charges.
Use a dedicated test account or tightly scoped role. The identity needs these
actions for queue names beginning with the selected prefix:

- `sqs:CreateQueue`, `sqs:DeleteQueue`, and `sqs:GetQueueAttributes`;
- `sqs:SetQueueAttributes`;
- `sqs:SendMessage`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, and
  `sqs:ChangeMessageVisibility`.

The PoC accepts no access-key or secret-key flags. Configure credentials using
the standard AWS SDK credential chain, then run from the nested module:

```powershell
cd poc\sqssemantics
go run ./cmd/sqs-poc --backend aws --aws-region ap-northeast-2 --allow-aws-mutation
```

Without both an explicit region and `--allow-aws-mutation`, the AWS backend
fails before creating a queue. Queue URLs, ARNs, account IDs, message contents,
and receipts are omitted from the JSON report. Every successfully returned
queue is cleaned in reverse creation order; partial creation failures also
trigger best-effort cleanup. Check the account after an interrupted run for
the unique `simq-poc-*` prefix before considering the environment clean.

## Reading the result

A useful comparison is two independently captured JSON reports with all six
scenario statuses equal to `passed`. Durations are diagnostic only and must not
be compared as a performance benchmark. AWS Standard queues are at-least-once
and best-effort ordered, so application workers still require idempotent side
effects even after this PoC passes.

The exact contract, exclusions, and task checklist are in `SQS_POC_PLAN.md`.
Dependency and safety decisions are recorded in ADR 0017.
