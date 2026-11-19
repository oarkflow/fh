# MIRA-T/1 v6 — implementation and release gates

This is a **working engineering release**, NOT independently certified production transport.
It extends the v5 code and retains wire header version 4. No new encryption scheme is invented.

## Added in v6 and locally verified

| Item | Source | Local verification |
|---|---|---|
| Pinned HTTPS JWKS refresh with bounded cache freshness | `gateway/jwks.go` | Key rotation, invalid-source, outage/expiration, and OIDC discovery tests |
| Strict OIDC same-origin discovery | `gateway/DiscoverOIDCJWKS` | Test issuer and JWKS URI matching |
| Authenticated delegation of browser subject over custom UDP | `delegation/`, `gateway.BindPrincipal` | Real encrypted MIRA client/server delegation test; gateway-to-backend test |
| Server-enforced delegated identity | `cmd/mira-tls-server` | Build plus delegated channel tests; server requires explicit gateway identity/key |
| CLI fail closed for multiuser JWT without delegation | `cmd/mira-gateway` | Build and gateway policy test |
| Ed25519 delegation key provisioning | `cmd/mira-delegation-key` | Generated 0600 owner-readable PKCS8 private and public PEM |
| Persistent congestion response heuristic | `mira/session.go` | Deterministic collapse/ACK recovery tests and full transport suite |
| Remote mTLS latency generator with JSON metrics | `cmd/mira-bench` | Independently running mTLS echo server; 40 successful 256-byte round trips |
| Full source verification script | `scripts/verify-v6.sh` | Go tests, race, build, vet, Python frames, TypeScript |

## Exists from previous releases; not completed by v6

- mTLS 1.3 exporter bootstrap + custom encrypted UDP; authenticated channel multiplexing, ACK/retries, MTU probes, migration, flow control and quotas.
- Durable application WAL, explicit stored/processed receipts and reconnect/replay.
- Cluster fenced lease + inter-node forwarding, PostgreSQL SQL adapters and transaction interfaces.
- Go mobile-facing facade and browser HTTPS/NDJSON gateway.

## Still NOT implemented or externally validated

1. **Independent cryptography/protocol audit** and pen test. This cannot be performed by the implementation author or inferred from passing tests.
2. **In-band fresh-DH post-compromise ratchet**. Reconnecting via TLS 1.3 establishes fresh session keys, but packet-level symmetric epochs are NOT the same as an in-session fresh-DH ratchet.
3. **Resurrecting old UDP session cryptographic state after process crashes**. Recover durable application messages over a NEW authenticated session. Persisting packet numbers/old ciphertext for reuse without a reviewed design is unsafe and should not be added casually.
4. **RFC-grade congestion conformance and fairness**. v6 adds a defensive persistent-loss response, but it is NOT validated as QUIC/RFC 9002 equivalent. Matched WebSocket and QUIC benchmarks are absent.
5. **Carrier/WAN, IPv6/NAT64/MTU field tests**, sustained network partitions and 24–72h soak.
6. **Live 3-node PostgreSQL failover** under database restarts and partitions. The SQL adapters compile but weren't tested against a live cluster here.
7. **Android AAR, iOS XCFramework and actual device background/battery tests**. Requires target SDKs/Xcode and phones.
8. **OIDC token revocation/introspection**, upstream subject-policy lifecycle, organization-wide key management/KMS/HSM procedures. v6 pins and rotates JWKS, and delegates verified subjects. It does not automate revoked bearer-token invalidation.
9. **Gateway assertion signing-key live rotation** and PKI/HSM production rollout. Current signing key is provisioned via a separate operator command and loaded on process start.
10. **Independent cross-language transport peer**. Python codec tests cover wire framing only, not a full independent client/server.
11. **Supply-chain certification, attestation and regulatory sign-off**. CI and source packaging do not amount to an independently verified provenance program.

Production certification requires those release gates. Do not deploy sensitive public-user traffic until reviewed. The recommended interim production path is mature TLS/QUIC/WebSocket transport with MIRA's app-level permission model, while testing MIRA in isolated staging.
