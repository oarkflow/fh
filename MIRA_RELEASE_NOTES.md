# MIRA-T/1 v5 — local engineering release

Wire version remains **4** (no new frame IDs). Source API adds fields/options, new packages, and more conservative security defaults.

**Added:** Automatic authenticated MTU probing, jumbo black-hole refragmentation, migration-aware fallback, verified handover acknowledgment, loopback-only legacy PSK, browser origin/CSRF controls and pinned JWT verifier, renewable fail-closed cluster ownership, fenced router and real mTLS forwarding test, transactional consumer group (not DB-tested), durable replay client, TLS certificate/CA and device ACL hot reload, metrics, systemd/Docker/mobile build recipes, and operational runbooks.

**Verification:** See `VERIFICATION.md`. Local tests and an independent process TLS echo are not substitutes for independent protocol auditing, PostgreSQL partition/failover tests, carrier-network validation, mobile package builds or WebSocket/QUIC comparisons.

---

# MIRA-T/1 v4 engineering release notes

**Classification:** working advanced protocol candidate, wire revision 4. No independent audit or public production certification.

## Added since v3

- mTLS 1.3 bootstrap over TCP with mutual CA validation and RFC 8446 exporter-derived UDP session traffic master; verifies candidate UDP return path before activating session. Separate development cert generation and runnable CLI examples.
- Encrypted traffic secret one-way epoch evolution, while maintaining authenticated old-epoch receive tolerance for packet reordering. This is **not** a full DH/PQ post-compromise rekey ratchet.
- `Session.RebindClient` to swap a client UDP socket with server path validation, tested by changing NAT port mid-connection.
- `Session.ProbeMTU`, explicit MTU-probe control frame and higher bounded fragment sizes after successful validation; not automatic PLPMTUD.
- Reliable ordered channel write-half-close (`CloseWrite`), receiver EOF after final ordered data and independent reverse traffic.
- Bounded ACK-range expansion (max 512 acknowledged packet numbers per frame) to mitigate expensive authenticated peer input.
- Optional at-rest AES-256-GCM envelopes around journal WAL with historical key IDs, restart, idempotence, and key-missing tests.
- Browser HTTPS JSON/NDJSON gateway, client TypeScript wrapper and end-to-end browser-to-UDP backend test.
- Multi-node building blocks: lease store with fencing, PostgreSQL SQL schema and transactional outbox, and mTLS forwarding adapter. **No automated cross-process failover/rolling upgrade integration.**
- Go mobile-facing façade; independent Python header/frame codec and v4 fixed fixtures; updated Wireshark version recognition.

## Verified within the local environment

- Separate-process cert-authenticated server/client echo and 1,400-byte MTU probe.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, `go build ./...`.
- Independent Python conformance parser and TypeScript compile check.
- Local integration tests for TLS authentication, socket rebinding, MTU/large payload, half-close, gateway, WAL encryption and cluster lease logic.

## Deployment restrictions

**DO NOT** treat the above as independent authentication-proof, network-grade congestion certification, WAN/mobile field test, Android/iOS packaging validation, PostgreSQL live multi-node failover, or a third-party security review. These remain in `TASKS.md`.
