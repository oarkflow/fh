# v7 current outstanding release gates

See `V7_IMPLEMENTATION_STATUS.md` for the complete, up-to-date checked implementation matrix and concrete remaining work. All v5/v6 sections below are retained for historical traceability and are not updated to claim a completed audit.

# MIRA-T/1 v5 implementation status and remaining release gates

**Do not equate "implemented" with "independently production certified."** v5 extends the custom v4 UDP wire protocol and keeps wire version 4. This tree is suitable for controlled staging and security engineering, not sensitive public traffic before external review.

## Implemented and locally exercised

- [x] Bidirectional encrypted custom UDP session engine, multiplexed reliable/best-effort/latest/bulk channels, ACK/retry, flow control, bounded queues, fragmentation, per-device identities.
- [x] TLS 1.3 mTLS authenticated control bootstrap with exporter-derived UDP keys, anti-replay and symmetric epoch key evolution. Legacy PSK mode now refuses non-loopback addresses by default.
- [x] Automatic conservative PLPMTUD candidates and probe backoff; black-hole detection and recovery by refragmenting outstanding reliable messages using their original message ID; migration invalidates previous probes. Tested with an injected jumbo-drop proxy. **Not validated across real IPv6/NAT64/carrier network combinations.**
- [x] Native UDP socket migration and `RebindAndWait` with acknowledged, validated new path; Go mobile facade and test.
- [x] Browser HTTPS gateway with strict HTTPS origin allowlists, CSRF-oriented fetch-site checks, explicit CORS preflights, per-principal session limits and independently verified EdDSA/RS256 JWT access tokens (pinned public keys, fixed issuer/audience). JWT CLI mode is available.
- [x] Durable WAL, encrypted-at-rest key IDs, stable idempotency IDs, AckStored/AckProcessed, journal cursor replay, and new `resume` client for fresh-session reconnect/replay.
- [x] Cluster `ManagedLease` renews and fails closed, fenced routing, mTLS inter-node forwarding (two-node in-process TLS integration), database/sql PostgreSQL outbox and transactional consumer-progress/dedupe code.
- [x] Reloadable mTLS server certificate/client trust roots, reloadable device ACL with live revocation, loopback Prometheus metrics, systemd/container recipes, restoration/rollback runbook.
- [x] Go integration/race/fuzz-target tests, independent Python wire codec fixtures, TypeScript browser client compilation, load command, repeatable verification scripts.

## Partially implemented / validation blocked

- [ ] **Full WAN congestion algorithm conformance:** custom congestion/pacing/retry exists but persistent congestion detection, fairness to other flows, untrusted-network fairness and operational thresholds are not independently validated.
- [ ] **Standards-grade PLPMTUD:** local automatic probing and drop recovery are implemented, but NAT64/IPv6 fragmentation and long-lived carrier path behavior need device/WAN testing, and the probe policy is not an RFC 8899 conformance claim.
- [ ] **Live multi-node PostgreSQL integration:** fenced lease router, transactional consumer and forwarding code exists, but the repository intentionally has no PostgreSQL driver dependency or live DB test environment. Add an audited `database/sql` driver, real 3-node tests, partitions, DB failover, and durable delivery workers.
- [ ] **Mobile binary SDK builds:** Go facade and `gomobile` build scripts exist; this Linux environment lacks Android SDK/gomobile and macOS/Xcode, so no `.aar` or `.xcframework` artifact or device background test was produced.
- [ ] **JWT key rotation automation and OIDC:** pinned key verifier exists but production OIDC discovery/JWKS update/revocation and a secure identity assertion through the UDP backend remain application-integration requirements. Browser transport ends at the trusted gateway; app E2EE is separate.
- [ ] **Full interoperability:** Python parser verifies v4 framing, not an independent interoperable client/server implementation. Rolling wire version negotiation/downgrade-failure matrix remains unimplemented.
- [ ] **Supply-chain/CD:** Docker/systemd and CI checks included, but signed artifacts/SLSA attestation and external deployment rollback drills are not produced by this build.

## Not implemented: requires new design or external infrastructure

- [ ] Independently reviewed in-band fresh-DH/post-compromise ratchet. Existing mTLS bootstrap uses a fresh TLS 1.3 handshake on reconnect and native transport uses a symmetric key epoch chain; no in-band fresh DH protocol is claimed.
- [ ] Recovery of in-memory transport packet numbers, old channel instances, and in-flight ciphertext after process crash. `resume` restores durable APPLICATION messages over a fresh encrypted transport, not old UDP sessions.
- [ ] Cryptographically pinned KMS/HSM production operations, automated PKI rollover across organizations and tested backup/restore with real key escrow.
- [ ] Matched p50/p95/p99 fair benchmarks versus optimized secure WebSocket and QUIC on comparable 4G/5G/Wi-Fi paths at scale.
- [ ] Published 24–72h soak, IPv6/NAT64/carrier field validation, independent penetration tests, external cryptographic review and regulatory sign-off.

**Safety decision:** release only into an internal/staging environment until external cryptographic, networking and deployment-validation gates are satisfied. Production-critical systems should use a reviewed widely deployed transport until then.

## v6 implementation update

Implemented since v5: automatic pinned-HTTPS JWKS key rotation and same-origin OIDC discovery; signed per-channel browser-principal delegation (server verified before application messages); ED25519 key-generation CLI; fail-closed JWT mode if no delegation key; persistent congestion-window collapse heuristic; separate-process remote mTLS JSON benchmark tool; end-to-end regression tests. See `V6_IMPLEMENTATION_STATUS.md` for exact mapping. The external security audit, post-compromise in-session DH ratchet, old transport-state crash resurrection, 3-node live PostgreSQL failover, WAN/IPv6/NAT64 soak, signed artifact provenance, and Android/iOS physical SDK tests remain open. Do not change checkboxes above to complete solely because v6 builds.
