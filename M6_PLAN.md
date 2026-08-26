# M6 implementation plan

Status: complete. Verified on 2026-08-26 with Go 1.26.7 and
`./scripts/verify.ps1`.

M6 turns the M5 queue service into an opt-in product-security deployment while
preserving the unauthenticated local-development mode and all M1-M5 semantics.
When security mode is enabled, queue APIs are deny-by-default.

## Architecture

The HTTP boundary authenticates an OIDC JWT before deriving `PrincipalID`,
`TenantID`, and roles. Client-provided tenant headers are never trusted. A
tenant-scoped repository maps every public queue name to a SHA-256-derived
internal namespace before it reaches memory, bbolt, or Raft. The wrapper strips
that namespace from every result, so public payloads cannot cross tenants.

The same repository boundary encrypts message bodies and message attributes
with AES-256-GCM before persistence. The envelope records a key ID and random
  96-bit nonce; tenant namespace and message ID are AAD. New
writes use the active key and older keys remain read-only for rotation.

Authorization uses explicit SimQ roles rather than the M2 compatibility policy.
Rate limits are tenant-scoped and fail with HTTP 429. Authoritative storage
quotas and usage counters are replicated with queue mutations. Metrics use
bounded tenant hashes, and audit records never contain message bodies,
attributes, tokens, receipts, or encryption keys.

## Detailed tasks

- [x] M6-A: Specify product mode, identity, roles, TLS, tenant namespace,
  encryption, quota, metric, audit, and compatibility contracts.
- [x] M6-B: Record the standard-library OIDC, AES-GCM, TLS, rate-limit, and
  observability choices in ADR 0010.
- [x] M6-C: Implement strict RS256 JWT verification with trusted issuer, audience,
  discovery/JWKS caching, key rotation, temporal claim checks, and fail-closed
  authentication.
- [x] M6-D: Add request-scoped principal and tenant services plus deterministic
  tenant namespace mapping for every Repository and RedriveRepository method.
- [x] M6-E: Add schema v8 tenant usage metadata and bind the legacy namespace to
  an explicit configured tenant after preserving a schema-v7 backup.
- [x] M6-F: Add AES-256-GCM payload envelopes, key rings, current-key rotation,
  authenticated context, and corruption/unknown-key failure behavior.
- [x] M6-G: Add role authorization, authoritative storage quotas, per-leader token
  buckets, stable 401/403/429 errors, and batch-entry accounting.
- [x] M6-H: Add HTTPS serving, optional mutually authenticated Raft transport,
  bounded Prometheus metrics, structured audit events, and trace correlation.
- [x] M6-I: Add multi-tenant isolation, forged-claim, JWT algorithm/key rotation,
  encryption, quota-race, failover, snapshot, and secret-redaction tests.
- [x] M6-J: Run the Go 1.26.7 verification gate and update status and operations
  documentation.

## Safety boundaries

- Only a cryptographically verified token may select a tenant in security mode.
- Every queue key, receipt operation, pagination cursor, redrive edge, and move
  task is scoped before reaching authoritative storage.
- Plaintext message bodies and attributes never enter bbolt, Raft commands,
  snapshots, metrics, traces, audit events, or ordinary logs.
- Authentication, authorization, missing keys, corrupt ciphertext, and quota
  ambiguity fail closed.
- Tenant and role labels emitted to metrics are bounded hashes or fixed enums.
- Disabling security mode is an explicit local-compatibility choice, not a
  fallback after security configuration fails.
