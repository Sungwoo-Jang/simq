# ADR 0016: Continuous operational qualification

## Status

Accepted for M11.

## Decision

M11 keeps heavy operational evidence in a separate GitHub Actions workflow.
It runs on manual dispatch, weekly schedule, and pull requests that change the
workflow or its direct deployment, runner, and integration-test inputs. It has
read-only repository permission, per-ref concurrency cancellation, and
independent job timeouts.

The workflow pins these reviewed inputs:

- `actions/checkout` v5 at
  `fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09`;
- `actions/setup-go` v6 at
  `924ae3a1cded613372ab5595356fb5720e22ba16`;
- `actions/upload-artifact` v5 at
  `330a01c490aca151604b8cf639adc76d48f6c5d4`;
- kind v0.20.0 and Kubernetes v1.27.3 at the image digest recorded in ADR
  0015;
- kubectl v1.27.3 with SHA-256
  `fba6c062e754a120bc8105cde1344de200452fe014a8759e06e4eec7ed258a09`
  for Linux amd64 and
  `a43547c34b7cc73664b2e0c79b726dba36a5e1842f59ac6be49c813f675e8059`
  for Windows amd64.

Secure qualification repeats the complete M10 secure chaos and offline
restore profile twice by default. The allowed iteration count is 1 through 10;
every iteration has a ten-minute outer timeout and copies only three bounded
text diagnostics plus a result file. The kind job has a fifteen-minute command
timeout inside a twenty-minute job timeout.

Diagnostics are scanned before upload. Matches for private-key PEM blocks,
GitHub token families, JWT-shaped values, SimQ token assignments, or encryption
key assignments fail the job and prevent upload. Generated fixtures, volume
archives, databases, and tool downloads never enter the artifact path.
Successful bounded diagnostics are retained for 14 days.

## Rationale

The fast workflow answers whether each change preserves correctness. The heavy
workflow answers whether the reviewed operational topology still survives real
process, network, volume, DNS, scheduler, and PVC events. Separating them keeps
ordinary feedback fast while manual and weekly runs detect image, runner, and
orchestration drift.

Pinning kubectl removes an uncontrolled dependency on the hosted runner image.
Repeating the entire secure profile regenerates identities and rebuilds the
cluster, which catches lifecycle and cleanup coupling that an inner test loop
would miss. Sanitized diagnostics preserve enough failure context without
publishing generated credentials or state archives.

## Alternatives

- Running every heavy profile on every source-only pull request was rejected
  because it adds cost and latency without changing its inputs.
- Reusing one secure cluster for many loops was rejected because it would not
  exercise credential generation, clean bootstrap, or teardown repeatedly.
- Uploading the whole `.cache` directory was rejected because it contains
  generated credentials, database backups, downloaded tools, and build caches.
- Depending on the hosted runner's kubectl was rejected because its version is
  mutable and may drift from the pinned Kubernetes node.
- Treating a single-node kind result as multi-node or multi-AZ evidence remains
  forbidden; that requires a staging environment.

## Dependencies, license, and replacement

The three Actions are official GitHub-maintained actions. kind and Kubernetes
are maintained upstream CNCF projects. They use MIT or Apache-2.0 licenses and
remain orchestration-only dependencies; no Go module or production runtime
dependency is added.

Replacement affects `.github/workflows/operational.yml`, the qualification and
kind scripts, integration tests, and operational documentation. Any replacement
must retain immutable revision or checksum pinning, bounded execution, scoped
cleanup, secret-gated artifacts, and the same black-box invariant evidence.
