# ADR 0001: Durable storage engine

Status: Accepted

Date: 2026-08-23

## Context

M1-A must make the existing M0 Standard Queue operations durable on one node.
Queue creation, enqueue, receive claim metadata, receipt history, and deletion
must cross one synchronous commit boundary before HTTP reports success. Recovery
must reject incompatible or corrupt state rather than treating it as an empty
installation. Clustering, replication, and leader election remain out of scope.

The standard library remains the preferred dependency surface, but it does not
provide an embedded transactional store with crash recovery and process locking.

## Decision

Adopt `go.etcd.io/bbolt` at exactly `v1.5.0` under the MIT license. The release
was published on 2026-06-21, is the current stable release as of this ADR, and
declares Go 1.25, which is compatible with SimQ's Go 1.26.7 verification
toolchain. bbolt is actively maintained by the etcd project, has a stable API and
file format, and is used in production by etcd and other systems.

The module graph also pins bbolt's only runtime transitive module,
`golang.org/x/sys`, at exactly `v0.45.0`. It uses the BSD 3-Clause license,
declares Go 1.25, and is actively maintained by the Go project; v0.45.0 was
tagged on 2026-05-21. bbolt uses it for supported operating-system primitives
behind file locking, mmap, and durable file operations. SimQ does not import it
directly. Replacing it independently with handwritten syscalls, a fork, or a
vendored copy would increase platform and security maintenance without changing
the storage semantics, so SimQ accepts bbolt's pinned compatible version.

bbolt is a pure-Go, embedded, ordered key/value store. Its ACID, fully
serializable transactions and single-writer/multiple-reader model match SimQ's
authoritative receive-claim boundary: the visibility recheck, selection,
receive-count and generation increments, receipt creation, first-receive time,
and visibility deadline are committed in one write transaction.

SimQ will use `DB.Update`, never `DB.Batch`. A `Batch` callback can be invoked
more than once, while M1-A has no durable operation identity with which to make
such re-execution safe. `DB.NoSync` remains false. Every `Update` and `Close`
result is checked and wrapped without exposing paths, bodies, or receipt handles
through the HTTP API.

## Alternatives considered

- Pebble is a mature Apache-2.0 pure-Go LSM store with good write throughput,
  snapshots, and batches. Its WAL, compaction, cache, and tuning surface are
  unnecessary for the small single-node correctness slice, and SimQ would need
  to build more transaction and schema discipline around it.
- SQLite is exceptionally mature and provides strong transactions and schema
  tooling. Common Go drivers either require CGO or introduce a substantially
  larger pure-Go dependency graph. SQL and connection-pool behavior add
  complexity without helping M1-A's ordered key/value access pattern. SQLite
  remains a viable future choice if query or operational requirements change.
- A standard-library append log plus snapshots would avoid an external module,
  but implementing crash-safe framing, checksums, compaction, locking, recovery,
  and atomic multi-record updates would create a new storage engine inside SimQ.

## Transaction and sync boundary

Each `CreateQueue`, `SendMessage`, `ReceiveMessage` claim, `DeleteMessage`, and
M1-B `ChangeMessageVisibility` uses exactly one `DB.Update` transaction.
Returning nil from its callback asks bbolt to commit; SimQ reports success only
if `DB.Update` itself returns nil. A visibility change reuses the version 1
message record's Unix-nanosecond deadline and existing receipt-history binding;
it does not require a schema or record-version change.
With `NoSync=false`, that commit includes bbolt's data and metadata synchronization
before the call returns. A callback, encoding, page allocation, write, commit, or
sync error therefore propagates to the service and becomes a non-2xx response.

Read operations use `DB.View`. A repository health check opens a read transaction
and validates the schema and records. Repository close is idempotent from SimQ's
perspective; once closed, health and queue operations report unavailable.

This boundary demonstrates DUR-001 through DUR-004 and preserves MSG-002,
MSG-006, RCP-001 through RCP-004, TIME-003, and CON-001 through CON-003.
DUR-005 is satisfied by bbolt recovery of committed pages plus a schema that has
no replay side effects. DUR-006 is enforced by fail-closed validation.

## On-disk schema and versioning

The database has a required metadata bucket containing a numeric schema version
and a random installation identifier. Required top-level buckets store queue
records, active message records, per-queue ordered message references, all
issued message IDs, and all issued receipt bindings.

Queue, message, message-ID, and receipt values are repository-owned versioned
records; domain `queue.Message` and `queue.Queue` structs are never serialized
directly. Records use strict JSON decoding for readability in M1-A while bucket
keys and ordered sequence keys remain binary-safe. Every timestamp is an
explicit Unix integer. Sent and public system timestamps use Unix milliseconds;
visibility deadlines use Unix nanoseconds so the exact eligibility boundary is
retained across restart.

Receipt history and message-ID history are retained after deletion in M1-A.
That preserves uniqueness and distinguishes a structurally valid but unissued
handle from an issued stale or consumed handle. Later retention/maintenance work
must define safe garbage collection before removing either history.

Schema changes require a new version and an explicit offline or copy-on-write
migration. An unknown version is incompatible, not implicitly upgradeable.

## Corruption policy

Open validates bbolt's physical page graph and the complete SimQ logical schema.
Missing buckets or metadata, unknown schema or record versions, malformed
records, invalid required fields, dangling/order-duplicate messages, mismatched
queue ownership, missing ID history, and inconsistent current receipt bindings
all fail startup. No bad record is skipped and no existing file is initialized
as empty.

The first creation uses an exclusive `0600` placeholder. Only the process that
created that path may initialize the database and schema. A power loss between
placeholder creation and the first committed schema can leave a zero-length or
schema-incomplete file; the next start fails loudly instead of initializing it.
Because no HTTP listener existed before schema commit, no acknowledged queue data
can be lost in that case. Operator removal of such a file is an explicit recovery
action, never an automatic startup behavior.

Runtime storage errors make readiness fail. Health remains a liveness signal.
Ordinary logs and HTTP errors contain operation context only, never message
bodies or receipt handles.

## Locking, mmap, size, and compaction constraints

bbolt permits one write transaction at a time and takes an exclusive file lock,
so only one SimQ process may open a data file. SimQ configures a finite one-second
open timeout by default; a second process fails rather than waiting forever.
Long read transactions must be avoided because they can delay remapping and
writes.

bbolt memory-maps the data file. Virtual address space follows the mapped file
size, and values returned by bbolt are valid only inside their transaction; the
adapter always decodes or copies them before returning. The file grows as pages
are allocated and does not shrink automatically after deletes. M1-A defines no
online compaction and no maximum data-file size. Operators must monitor disk and
address-space use; a future maintenance command must compact by copying into a
separate validated file during an offline or carefully coordinated replacement.

## Configuration and file permissions

- `SIMQ_STORAGE=bbolt|memory`, default `bbolt`.
- `SIMQ_DATA_PATH`, default `./data/simq.db`.
- `SIMQ_BBOLT_OPEN_TIMEOUT`, default `1s`, must be a positive Go duration.
- `memory` is explicit development/test mode and never claims restart durability.
- A newly created data directory is `0700`; an existing data directory must
  already be `0700`. A new or existing database file must be `0600`.
- Those numeric mode checks apply on Unix. Go on Windows exposes ACL-backed
  files without equivalent POSIX mode semantics, so Windows relies on the
  configured directory's inherited NTFS ACL and does not claim ACL-policy
  validation. Windows also cannot `fsync` a directory through `os.File.Sync`;
  SimQ still syncs backup files and every bbolt commit before publication.
- SIGINT or SIGTERM triggers HTTP graceful shutdown before repository close.

The Unix crash test uses SIGKILL. The Windows verification path triggers
`os.Exit(137)` inside the test-only child process, bypassing shutdown hooks,
defers, and repository close, then reopens the same bbolt file. This is the
supported Windows process-crash equivalence test; it does not claim to reproduce
Unix signal delivery.

## Backup and migration

Raw copying a live mmap file is not a supported backup method. A future backup
command will use a consistent bbolt read transaction (`Tx.WriteTo`/`CopyFile`),
write to a separate path, sync it and its directory, then verify physical and
logical schema before publication. Restore will be offline and validate the
entire candidate before replacing the configured file.

Migrations will read the old version into a new file rather than rewriting the
only copy in place. The original remains the rollback artifact until the new
file is fully synced, validated, and atomically published. No automatic migration
is included in M1-A.

## M5 role and replacement boundary

In M5, bbolt may remain a node-local materialized state store or snapshot store,
but it must not decide replicated ordering or quorum durability. The consensus
log and deterministic apply boundary become authoritative. If bbolt is replaced,
only `internal/storage/boltrepo`, command wiring/configuration, migration/backup
tools, and storage-specific tests should change. The HTTP layer and queue service
depend only on `queue.Repository` and must remain unaware of bbolt.

## Dependency removal impact

Removing or replacing `go.etcd.io/bbolt v1.5.0` affects the durable repository
adapter, on-disk migration and backup path, startup configuration/file locking,
health/close wiring, and durable adapter/integration tests. It does not change
the public HTTP contract, domain types, service validation, injected clock, or
the in-memory repository conformance implementation.

Removing `golang.org/x/sys v0.45.0` has no separate SimQ layer boundary: it must
occur through a compatible bbolt upgrade/replacement. Its invariant contribution
is indirect but material to DUR-001, DUR-004, DUR-005, DUR-006, and CON-001 via
the file sync, mmap, and process-lock implementations used by bbolt.

## References

- [bbolt v1.5.0 release](https://github.com/etcd-io/bbolt/releases/tag/v1.5.0)
- [v1.5.0 module Go version](https://github.com/etcd-io/bbolt/blob/v1.5.0/go.mod)
- [MIT license](https://github.com/etcd-io/bbolt/blob/v1.5.0/LICENSE)
- [Project status, transactions, locking, and caveats](https://github.com/etcd-io/bbolt/blob/v1.5.0/README.md)
- [`golang.org/x/sys v0.45.0` source and maintenance history](https://github.com/golang/sys/tree/v0.45.0)
- [`golang.org/x/sys v0.45.0` BSD 3-Clause license](https://github.com/golang/sys/blob/v0.45.0/LICENSE)
