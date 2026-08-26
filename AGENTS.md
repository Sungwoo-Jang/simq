# SimQ contributor guidance

- Treat `SPEC.md` as the source of truth for public API behavior.
- Preserve the safety and concurrency rules in `INVARIANTS.md`, and map changes to invariant tests.
- Use Go 1.26.7 as the project verification toolchain.
- Keep HTTP, queue service, and repository responsibilities separate.
- Prefer the Go standard library. A mature, maintained core-infrastructure
  dependency may be added only after an ADR records the selection rationale and
  alternatives, exact pinned version, license, maintenance status, invariant
  coverage, and the layers affected by later removal or replacement.
- Run `./scripts/verify.sh` before reporting an implementation complete. On a
  Windows-only environment, run the equivalent `./scripts/verify.ps1` instead.
  `make verify` is an optional Unix wrapper.
