# M11 implementation plan

Status: complete. Delivered in pull request #5.

M11 converts the heavier M10 profiles into continuous operational
qualification. It does not add or change a public queue action. The milestone
adds repeated secure failure/recovery execution, successful Kubernetes
execution on a compatible Linux cgroup v2 runner, sanitized diagnostics, and
manual plus weekly evidence.

## Architecture

1. The fast pull-request workflow remains the required correctness gate.
2. A separate heavy workflow runs when its own deployment/test inputs change,
   on manual dispatch, and every Monday at 03:17 UTC on the default branch.
3. Secure qualification runs the complete generated-credential, two-shard,
   Raft-partition, FIFO-failover, and empty-volume restore scenario twice. Each
   iteration has an outer timeout and independent cluster teardown.
4. Kubernetes qualification downloads checksum-pinned kind and kubectl
   binaries, creates one named cluster, verifies leader Pod recreation and the
   unchanged PVC UID, captures diagnostics, and deletes the cluster.
5. Only bounded text diagnostics and result summaries are retained for 14
   days. Backups, database files, generated PKI, JWTs, and environment files are
   excluded. A secret-pattern guard blocks artifact upload.
6. GitHub job timeouts, command timeouts, test deadlines, and resource-specific
   cleanup remain independent so one failed layer cannot create an unbounded
   run.

This is process, network, restore, scheduler, DNS, and PVC evidence. A single
kind node is not evidence of independent nodes, storage systems, availability
zones, managed identity, ingress behavior, or production recovery objectives.

## Detailed tasks

- [x] M11-A: Merge M10, synchronize `main`, and create an isolated M11 branch.
- [x] M11-B: Pin kubectl to the Kubernetes node version and route every kind
  assertion through the reviewed binary.
- [x] M11-C: Add bounded repeated secure chaos/DR qualification with
  iteration-specific diagnostics.
- [x] M11-D: Add manual, scheduled, and path-scoped pull-request heavy CI with
  least-privilege permissions and pinned action revisions.
- [x] M11-E: Scan bounded diagnostics for private keys, JWTs, access tokens,
  and encryption configuration before a 14-day artifact upload.
- [x] M11-F: Execute local available profiles and obtain successful secure and
  kind evidence on the compatible GitHub-hosted Linux runner.
- [x] M11-G: Pass the Go 1.26.7 verification gate, secret scan, and deliver
  through a feature-branch pull request.

## Completion boundary

M11 is complete when both heavy jobs pass on the same reviewed commit, the
fast PR workflow remains green, diagnostics contain no sensitive-value
patterns, and local supported checks pass. Scheduled runs provide fresh
evidence after merge; their future success cannot be claimed in advance.

Reviewed commit `2ef7b45` passed all jobs in GitHub Actions runs
[`33059767444`](https://github.com/Sungwoo-Jang/simq/actions/runs/33059767444)
and
[`33059767309`](https://github.com/Sungwoo-Jang/simq/actions/runs/33059767309).
The heavy run retained only the secret-scanned secure and kind diagnostic
artifacts.
