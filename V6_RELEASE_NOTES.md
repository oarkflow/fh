# MIRA-T/1 v6 — engineering release notes

## Transport and congestion
- Added conservative persistent-congestion detection that responds to multiple probe timeouts without confirmed ACK progress by collapsing its congestion window; a new successful ACK ends the episode.
- Added `PersistentCongestion` to transport session statistics. This is defensive behavior, not a conformance/fairness claim.

## Authentication and trust boundaries
- Implemented bounded, pinned-HTTPS JWKS rotation with atomic replacement, strict RS256/EdDSA public-key types, capped fetch size, no redirects, and fail-closed stale-cache behavior.
- Implemented optional same-origin OIDC discovery with exact issuer pinning.
- Added an Ed25519 signed, short-lived session+channel-bound principal delegation handshake for the browser gateway's service certificate.
- The multi-user gateway CLI now refuses startup when it has no signed-delegation key. The server CLI only accepts delegated principals on the explicitly configured trusted mTLS gateway service identity.
- Added standalone delegation key generator (PKCS8, owner-only private key).

## Diagnostics and build
- Added `cmd/mira-bench` for separately hosted mTLS echo tests with JSON throughput/p50/p95/p99/failures.
- Added `scripts/verify-v6.sh` and end-to-end tests for signed browser subject propagation.
- Wire header revision remains 4; the delegation handshake is application-level at channel start and only required for configured gateway service identities.

## Compatibility
- v5 clients still use existing UDP wire format. New multi-tenant browser gateway deployments must configure both ends with signing and verification keys; no insecure fallback is allowed in JWT CLI mode.
- `jwks-url` and `oidc-discovery` are optional alternatives to the previously pinned PEM signing key.

## Caveats
- No independent audit, field-network certification, 3-node live Postgres validation, physical mobile SDK build, cross-language transport interop or in-session fresh-DH ratchet is claimed.
