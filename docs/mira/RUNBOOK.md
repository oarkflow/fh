# MIRA-T/1 operations and incident playbook

## Monitoring

Aggregate Prometheus-style endpoint: `http://127.0.0.1:9109/metrics`. Alert on rising handshake failures, sudden drop in active sessions, anomalous packet drops, pressure on the session quota, crash loops and increased retransmissions where instrumented. Do not export session keys, device IDs, authentication tokens or payloads as metric labels.

## Suspected credential compromise

1. Immediately remove the certificate identity from the allowlist, atomically replace the file and send `SIGHUP` to the service. Verify the identity is denied and active sessions disappear.
2. Reissue the device certificate and update CA roots if the issuing authority is compromised. Rotation of a server certificate does not revoke an already-established encrypted MIRA UDP session; revoke identities or restart workers where needed.
3. Rotate encrypted WAL keyrings with historical keys retained until compaction/backup policy permits their safe retirement. Track affected exports without exposing secret material in telemetry.
4. Preserve minimal audited metadata of the incident under your retention and incident-response policy.

## Packet-loss incident / MTU black hole

Watch retransmissions and RTT. The session automatically probes authenticated UDP datagram sizes and falls back to 900-byte conservative fragments if jumbo datagrams repeatedly fail. It does **not** guarantee reliable transfer over every path or congestion pattern; halt rollout if p99 latency or failure rate exceeds pre-agreed limits. Record IPv4/IPv6, NAT, MTU and carrier type from non-sensitive diagnostics.

## Database / cluster failover

1. Verify the shared lease store health. Ownership renewal fails closed; a stale node must NOT commit writes after losing its fence.
2. Let a new node obtain a higher fence and have clients reconnect through fresh TLS bootstrap. Do not copy in-memory packet counters or AEAD secrets between processes.
3. Replay application events from durable cursors; downstream database effects must be idempotent and transactional. Old in-flight UDP data cannot be resurrected verbatim.
4. During suspected split brain, stop writes until the database fencing state is authoritative and all stale owners are stopped.

## Backup and restore

- Quiesce or snapshot the single-writer WAL consistently, retain backup of encryption keyring under independent controls and verify CRC/recovery by opening a restored copy.
- Back up PostgreSQL tables through standard supported tooling with tested point-in-time recovery; keep consumer progress and outbox tables consistent.
- Restore to a staging isolated network first; verify cursor monotonicity, certificate trust, service permissions, and recovery of idempotent operations before cutover.

## Rollout and rollback

Wire revision remains **4** for this backwards-compatible source update. Never enable silent downgrade to legacy experimental PSK. Drain connections, deploy a small canary percentage, verify latency/error/CPU/network metrics, expand only after steady-state measurements, and retain an explicit rollback build. Rollback must not overwrite newer durable cursor or WAL data. If protocol negotiation changes, introduce a separate explicit wire version and tested version matrix.
