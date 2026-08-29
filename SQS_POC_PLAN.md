# SQS semantics PoC plan

Status: in progress.

This proof of concept answers a deliberately narrower question than protocol
compatibility: can the same worker-facing queue semantics be exercised against
SimQ and Amazon SQS without changing the scenario code? SimQ continues to use
the JSON HTTP API defined in `SPEC.md`; the PoC does not claim that an AWS SDK
can point directly at SimQ.

## Acceptance contract

The shared runner checks these observable invariants:

1. an acknowledged message can be received, processed, deleted, and does not
   reappear;
2. a worker that receives and exits without deleting causes the same logical
   message to be redelivered after its visibility timeout with a higher receive
   count and a fresh receipt;
3. extending visibility prevents premature redelivery and the message becomes
   available after the extension;
4. a FIFO in-flight group head blocks the next message in that group while an
   independent group can progress;
5. two FIFO sends with one deduplication ID create one deliverable message;
6. a repeatedly unacknowledged message moves to its configured dead-letter
   queue at the same receive-count boundary.

Message IDs, receipt handles, queue URLs, sequence-number formats, request IDs,
and exact sub-second timing are intentionally not compared. Standard queues
are treated as at-least-once and best-effort ordered.

## Detailed tasks

- [x] POC-A: merge the qualified M11 baseline and isolate the work on
  `test/sqs-semantics-poc`.
- [x] POC-B: record the shared semantic contract, dependency boundary, safety
  guard, and evidence limits.
- [x] POC-C: implement a backend-neutral runner and unit-test its assertions.
- [x] POC-D: implement a leader-aware SimQ JSON adapter.
- [x] POC-E: implement an Amazon SQS adapter in an isolated nested Go module
  using pinned official AWS SDK for Go v2 modules.
- [x] POC-F: add disposable-resource naming, guaranteed cleanup, bounded
  polling, JSON result output, and an explicit AWS mutation opt-in.
- [ ] POC-G: run the complete suite against a local three-node SimQ cluster,
  compile and test the AWS path, run repository verification and secret scans,
  then deliver through a pull request.

## Evidence boundary

A passing local run proves the six worker-facing invariants against the tested
SimQ commit and topology. An AWS run proves the same observations for one
account and region at that time. It does not prove wire/API compatibility,
capacity, multi-AZ behavior, service quotas, IAM policy correctness, cost,
latency SLOs, or production recovery objectives.
