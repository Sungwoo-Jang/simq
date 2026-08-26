# ADR 0006: Queue generations and administrative transactions

Status: accepted

Date: 2026-08-24

## Context

Name-only queue URLs cannot distinguish a deleted queue from a later queue with
the same name. M2 also needs pagination, deletion, purge, tags, and permission
metadata without weakening the M1 atomic claim and commit-before-success rules.

## Decision

Each live queue receives an opaque generation ID and public URLs include both
name and ID. Every queue-scoped repository mutation validates that pair in its
authoritative critical section or bbolt transaction. Migrated schema-v3 queues
receive deterministic installation-local IDs. A name with no deletion history
temporarily accepts its name-only compatibility alias; recreation permanently
disables that alias. Every URL returned by M2 includes the generation ID.

Schema v4 adds identity, tombstone, tag, and permission buckets plus a namespace
revision. Delete and purge are single repository transactions. Delete removes
all generation-owned state; purge removes messages and receipt history but
preserves configuration and metadata. Historical message IDs remain reserved.

List order is lexical by queue name. Continuation tokens bind prefix, cursor,
and namespace revision. Namespace changes invalidate tokens instead of allowing
an apparently successful traversal with gaps or duplicates.

Tags and permission statements are stored separately from queue configuration.
Permission statements are projected as deterministic JSON through the `Policy`
queue attribute, but are never consulted for authorization.

## Alternatives considered

- **Keep name-only URLs.** Rejected because no implementation can distinguish
  an old URL from the identical URL of a recreated name.
- **Resolve an ID in the HTTP layer and pass only the name.** Rejected because a
  delete/recreate race between resolution and mutation crosses generations.
- **Best-effort cursor pagination.** Rejected because concurrent namespace
  changes can silently skip or repeat queues.
- **Asynchronous AWS-style purge/delete delays.** Rejected for M2 because the
  local durable repository can provide a stronger immediate atomic boundary.
- **Enforce permission metadata.** Rejected because authentication, tenants,
  IAM principals, and policy evaluation remain explicitly out of scope.

## Consequences

Queue URLs change for M2, schema v3 requires a backed-up migration, and all
queue-scoped commands carry an optional queue ID for legacy direct-service
tests. The design adds storage validation and token invalidation work, but makes
deletion, recreation, and administrative races linearizable and testable.
