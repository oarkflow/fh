## v7 operational security additions

- Delegation proof v2 includes the key ID in the Ed25519 signed transcript; rotation replaces signer and verifier snapshots atomically and unknown/retired IDs fail closed. The prior proof v1 remains supported only with an explicitly configured legacy public key.
- JWT revocation is local signed-token post-validation policy. It is NOT global IdP introspection. Distribute and reload files atomically on each instance; successful reload invalidates existing gateway sessions. Failure to reload preserves the last valid snapshot only until its max age.
- Gateway connection quotas now cover in-flight upstream handshakes, not just established sessions. Requests continue to have request-timeout and backend-limit boundaries.
- Durable replay checkpoint files require access-controlled directories and an idempotent application callback. They do NOT persist UDP encryption state.
- Python encrypted peer is a test-only **legacy PSK** implementation and is hard-coded to loopback; do not expose it on public networks or treat it as independently reviewed crypto.
- This build has not received an independent cryptographic audit, fuzzing campaign certification, or public Internet congestion fairness assessment.

# v5 security change summary

- **Deployment policy:** PSK listeners and dialers now reject non-loopback addresses by default. `AllowLegacyPSKOnPublicIPs` exists only as an explicitly UNSAFE compatibility switch. Public-facing native clients should use mTLS bootstrap.
- **Device ACL:** `cmd/mira-tls-server` accepts `-allow-file` and reloads the ACL on SIGHUP. Removed identities are actively revoked from live sessions. Invalid files are rejected; existing policy is retained.
- **TLS rotation:** SIGHUP reloads server private key/certificate and client trust roots for *new* mTLS connections. Old established sessions are not implicitly revoked.
- **Browser:** Pin exact HTTPS origins, verify signed RS256/EdDSA tokens with fixed issuer/audience and current exp, and enforce authorization on the backend for every identity. A gateway uses a shared backend service certificate unless your application explicitly propagates and authorizes individual principals.
- **Session recovery:** `resume` starts new TLS sessions and replays stored events. Never reuse old AEAD packet counters or mistake a transport ACK for a durable commit. Apply idempotent effects on the consumer side.
- **Cryptography:** No claim of third-party audit or post-compromise fresh-DH ratchet. Public sensitive workloads require review of the protocol design, authentication composition, and path migration/anti-amplification behavior.

---

# MIRA-T/1 v4 security considerations

**UNREVIEWED CUSTOM TRANSPORT — do not use for sensitive public production communication without external security and network assessment.** This file lists protection mechanisms and their remaining trust assumptions, not a security certification.

## Recommended authenticated bootstrap

- Use `mira.ListenTLS` / `mira.DialTLS` with a CA-managed TLS 1.3 server and unique client certificates. TLS is terminated only for session establishment. A domain-separated TLS exporter derives the secret for encrypted MIRA UDP traffic; no cleartext application payloads are sent on TCP.
- Do not enable `InsecureSkipVerify` in TLS clients. The server verifies client certificate chains; the application must also authorize mapped identities and channel/payload actions. The development certificate generator is **never** a public CA.
- UDP return-path proof is required before confirming an authenticated session; identity, connection IDs and keying material bind the control and data paths.
- The legacy device PSK bootstrap remains bespoke. In secure deployments run TLS-only listeners; do not expose legacy PSK to arbitrary public clients.

## Key and packet lifecycle

- Data uses AES-256-GCM with 26-byte header as associated data, independently keyed send/receive directions, monotonic per-direction packet numbers, and authenticated replay windows.
- Session traffic keys evolve in 4096-packet epochs via a one-way chain with bounded previous-key receive allowance. This can reduce past-key retention but **does not** provide post-compromise recovery or independent ephemeral DH exchanges during the session.
- Packet ciphertext is retransmitted without resealing under the same nonce. No arbitrary 0-RTT application sends are permitted. TLS session resumption, when supported by the host TLS stack, establishes **new** MIRA traffic keys and a new UDP session.
- Protocol and authenticated handshake composition, custom loss response, key ratchet and optional at-rest envelope must be audited by independent cryptographers/security engineers.

## Network and peer abuse

- Authentication, anti-replay, bounded message sizes, queues, channels, sessions, flow credits, quotas and rate limits limit resource abuse; their limits are not a DDoS guarantee.
- An authenticated new source address must pass PATH_CHALLENGE/PATH_RESPONSE before being accepted. ProbeMTU uses authenticated probe acknowledgments; there is **no** comprehensive probe-loss blackhole algorithm. Never assume 1400 bytes works on all paths.
- Ensure UDP and TCP bootstrap endpoints have consistent firewall/TLS policy and NAT reachability. Prefer allowlists in preaudit deployments. Use externally managed mitigations against spoofing, floods and handshake exhaustion.

## Browser gateway boundary

- `gateway.Config.Authenticate` must verify every HTTPS request, and `DialBackend` must authorize the mapped principal **at the backend**. Do not treat a gateway bearer as permission to impersonate arbitrary principals.
- The example `cmd/mira-gateway` is a single-service-credential **demonstration**, not multi-tenant production auth. Set strict CORS, CSRF considerations, origin/authorization policies, secure TLS, and logs that do not reveal tokens or plaintext.
- Browser-to-gateway HTTPS and gateway-to-MIRA UDP are **two separate security segments**; gateway access to plaintext is unavoidable unless the application uses independent E2E encryption.

## Journal and cluster

- Single-writer journal provides local fsync persistence, idempotency, compaction and replay; encrypted wrapper protects WAL message payloads, not necessarily all WAL metadata. Keys must be 0600 managed, periodically rotated, securely backed up, and retained while records need decryption.
- Treat the at-rest deterministic AEAD nonce construction as **audit-required**. Do not infer protection against malicious privileged host administrators or disk rollback.
- PostgreSQL fencing tokens and transaction-coupled outbox are skeletons for distributed ownership. Applications must enforce fenced writes and exactly-once effects inside their own DB transaction. No multi-node safety claim is supported without failure-injection tests against a live Postgres cluster.

## Mandatory external release gates

1. Independent review of TLS-to-UDP key exporter binding, legacy PSK, AEAD epochs, at-rest envelope, packet replay and state machine (including downgrade/credential rotation).
2. Security pen-test and stateful fuzz/coverage-driven audit of parsing, handshake, ack ranges, path migration, quotas and broker.
3. WAN/mobile fairness and persistent-loss measurements, IPv4/IPv6, NAT64, dual-stack, path MTU blackholes and handovers.
4. Real multi-node failover, PostgreSQL WAL/outbox transaction integration, backup recovery, secret rotation and incident response drills.
5. Operational threat model, alerting/SLOs, 24–72h soak and user-appropriate regulatory/security sign-off.

## v6 authentication boundary

- Browser multi-user JWT CLI mode now requires a separate Ed25519 gateway delegation private key. The backend must explicitly configure both `-gateway-identity` (mTLS certificate identity) and `-gateway-delegation-key` (pinned public verification key). A gateway service using an ordinary user certificate is a deployment mistake; provision an explicit service identity.
- Every gateway channel consumes a signed, session-ID/channel-ID-bound assertion before user application payload; verifiers check expiry, audience, signature and bounded replay state. Proof verification is an application boundary and does not replace per-message subject authorization, app E2EE or IDP token revocation.
- Remote OIDC/JWKS refresh uses exact pinned HTTPS endpoints, refuses redirects, validates key shapes, and will stop accepting tokens once the last successful key refresh exceeds MaxStaleness. Real IdP operational validation is still required.
- The runtime's v6 persistent-congestion response is a defensive heuristic, not evidence of standardized congestion behavior or WAN fairness.
- Delegation service signing keys are process-loaded; safely coordinated hot signing-key rollover and KMS/HSM controls are still outstanding.
