# ADR 0010: Product security and observability boundary

## Status

Accepted for M6.

## Decision

SimQ implements its M6 security primitives with the Go 1.26.7 standard library.
OIDC discovery and JWKS retrieval use `net/http`; JWT parsing uses strict JSON
and base64url decoding; only explicitly configured RS256 signatures are
accepted and verified with `crypto/rsa`. Issuer, audience, subject, expiration,
not-before, tenant claim, and role claims are validated independently.

Message payloads use AES-256-GCM from `crypto/aes` and `crypto/cipher`. A random
nonce is generated for every envelope. Key material is supplied as a versioned
key ring; ciphertext carries only the non-secret key ID. HTTPS uses `crypto/tls`.
Metrics use the Prometheus text exposition format without adding a client
library. Audit output is newline-delimited structured JSON written to a
dedicated sink.

## Rationale

The required primitives are mature standard-library capabilities, so no new
core-infrastructure dependency is justified. Explicit algorithm selection
prevents JWT algorithm confusion. OIDC issuer discovery binds the configured
issuer to its JWKS URI. AES-GCM provides confidentiality and integrity while
authenticated context prevents ciphertext from being moved between tenants or
message IDs while allowing authorized redrive within one tenant.

## Alternatives

- A third-party JWT/OIDC SDK was deferred because M6 needs a deliberately small
  RS256 verification surface and the standard library covers it.
- Client-supplied tenant headers were rejected because they do not establish
  authority.
- Per-tenant database files were rejected because M5 requires one deterministic
  replicated FSM and atomic snapshots.
- Deterministic encryption was rejected because repeated payloads would leak
  equality. Compress-before-encrypt was also rejected.
- Unbounded tenant IDs in metric labels were rejected due to cardinality and
  information-disclosure risks.

## Invariant coverage

- SEC-004 and M6 identity invariants: verified issuer/audience JWT claims select
  the tenant; repository namespacing is authoritative.
- Encryption invariants: authenticated AES-GCM envelopes, random nonces,
  versioned read keys, and fail-closed decoding.
- Quota invariants: counters and mutations share the storage transaction;
  per-leader rate limiting cannot authorize storage mutations.
- Diagnostic invariants: bounded labels and redacted structured audit fields.

## Removal or replacement impact

Changing the identity provider protocol affects `internal/auth`, HTTP
middleware, runtime configuration, and authentication tests. Changing the
encryption provider affects `internal/tenant`, stored payload envelopes, key
rotation tooling, snapshots, and recovery tests. Queue semantics, Raft ordering,
and standalone repositories remain behind the Repository boundary.
