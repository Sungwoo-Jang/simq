# M2 implementation plan

Status: complete

## Scope

M2 implements generation-bound queue URLs, `ListQueues`, `DeleteQueue`,
`PurgeQueue`, `TagQueue`, `UntagQueue`, `ListQueueTags`, `AddPermission`, and
`RemovePermission`, plus deterministic `Policy` projection from permission
metadata.

## Delivery slices

1. **M2-A — identity and schema v4.** Add durable queue identities, namespace
   revision, metadata buckets, schema-v3 backup/migration, and queue-reference
   validation inside every repository transaction.
2. **M2-B — lifecycle administration.** Implement list, delete, purge,
   generation-aware URLs, pagination tokens, waiter wake-up, and concurrency
   tests.
3. **M2-C — tags.** Implement atomic tag replace/remove/list with limits and
   storage conformance.
4. **M2-D — permission metadata.** Implement atomic statement replace/remove,
   deterministic policy projection, and explicit no-authorization semantics.
5. **M2-E — hardening.** Cover migration, restart, injected failures, crash and
   race regressions, then run the complete Go 1.26.7 verification suite.

## Invariant mapping

| Work | Invariants |
|---|---|
| Generation-aware URL and transaction guards | QUEUE-001, QUEUE-002, QUEUE-003, QUEUE-008 |
| Delete and purge | MSG-001 through MSG-008, QUEUE-004, QUEUE-009 |
| List and tokens | QUEUE-001, QUEUE-003, QUEUE-010 |
| Tags and permissions | QUEUE-005, QUEUE-007, QUEUE-011 |
| Migration and durable commit | DUR-001 through DUR-010, MIG-001 through MIG-008 |

## Completion gate

M2 is complete only after its public contracts are in `SPEC.md`, design choices
are recorded in ADR 0006, both repositories pass the same lifecycle/metadata
conformance suite, schema migration and crash/restart tests pass, and
`scripts/verify.ps1` succeeds with Go 1.26.7.
