# ADR 0017: Isolated Amazon SQS semantics PoC

## Status

Accepted for the SQS semantics PoC.

## Decision

The PoC is a nested Go module at `poc/sqssemantics`. Production SimQ code and
its root module remain dependency-free beyond the dependencies already
reviewed for core infrastructure. The nested module owns a small common queue
interface, a shared black-box scenario runner, a SimQ JSON adapter, and an
Amazon SQS adapter.

The Amazon adapter uses the official AWS SDK for Go v2 with the exact direct
module versions recorded in the nested `go.mod`: core v1.45.1, `config`
v1.33.1, and `service/sqs` v1.48.1. Transitive versions are locked in
`go.sum`. The SDK is
Apache-2.0 licensed, maintained by AWS, supports the project's Go toolchain,
and is used only by the disposable PoC executable. No AWS SDK type crosses the
common scenario interface or enters HTTP, queue-service, repository, or
production command packages.

An AWS execution requires all of the following: backend `aws`, a non-empty
region, and the literal `--allow-aws-mutation` flag. Credentials continue to
come from the SDK's standard external credential chain; the PoC accepts no
credential flags and writes no credentials. Queue names contain only a fixed
`simq-poc` prefix, UTC timestamp, and random suffix. Every created queue is
registered immediately for reverse-order best-effort cleanup. The runner uses
bounded request contexts and polling deadlines and emits no queue URL, ARN,
account ID, credential, message body, or receipt handle in its JSON summary.

The shared scenarios compare worker-visible invariants rather than identifiers
or precise timing. This reflects Amazon SQS's at-least-once Standard delivery,
visibility-based acknowledgement model, FIFO group ordering, five-minute FIFO
deduplication window, and dead-letter redrive behavior. SimQ remains a distinct
JSON HTTP API; SDK/CLI, AWS Query protocol, and SigV4 compatibility stay out of
scope in `SPEC.md`.

## Rationale

Running identical scenario code makes semantic drift visible without coupling
the production server to an SDK or pretending that matching operation names
means protocol compatibility. A nested module gives the PoC a realistic AWS
client while keeping dependency download, updates, licenses, and eventual
removal isolated from the product binary.

The mutation guard makes accidental cloud resource creation fail closed.
Disposable unique queues prevent collisions with application queues, while
immediate cleanup registration limits leaked resources after later failures.

## Alternatives

- AWS CLI subprocesses were rejected because machine-local installation,
  profiles, output formats, and error handling would become undeclared test
  inputs.
- Hand-written SigV4 and Query protocol support was rejected because it would
  duplicate security-sensitive SDK behavior and test the PoC client more than
  queue semantics.
- Adding the AWS SDK to the root module was rejected because no production
  SimQ layer needs it.
- Comparing exact IDs, receipts, timings, or Standard ordering was rejected
  because those are backend-specific or deliberately nondeterministic.
- Running AWS by default was rejected because it creates billable external
  resources and requires account authority unavailable to ordinary local CI.

## Invariant coverage and replacement

The scenarios cover acknowledgement deletion, visibility/redelivery, receipt
renewal, FIFO group exclusion and progression, FIFO deduplication, and DLQ
transfer. These map to the existing lifecycle, receipt-generation, FIFO, and
dead-letter invariants in `INVARIANTS.md`; they add black-box comparative
evidence without changing those rules.

Removing the PoC deletes only `poc/sqssemantics`, its dedicated scripts, this
ADR, and PoC documentation. Replacing the AWS SDK affects only the Amazon
adapter and nested module metadata. A replacement must retain external
credential sourcing, explicit mutation opt-in, bounded calls, cleanup, redacted
results, exact dependency pins, and the same shared scenarios.
