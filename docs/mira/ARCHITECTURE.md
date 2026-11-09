# MIRA-T/1 v4 — implementation architecture

## Goals and trust boundary

A native full-duplex custom Go UDP transport with concurrent sessions, multiplexed logical channels, per-channel delivery policies, packet/ack/credit/loss logic, authenticated path mobility, optional durable application receipts, and a browser bridge. This is **not** QUIC or WebSocket. Browsers must connect through an HTTPS gateway; UDP encryption terminates at that gateway's backend. The authenticated application principal must be checked again for every channel and message operation.

```text
       Native Go / mobile Go API          Browser JS/TS
                   |                         |
            TLS 1.3 mTLS bootstrap     HTTPS fetch + NDJSON
            (control/auth only)               |
                   |                    Authenticated gateway
                   +----- MIRA v4 UDP -------+
                         encrypted packets
                               |
             Session actor / channels / ACK ranges
                  |         |         |
            Delivery    Pacing     Path migration / MTU
            /receipts   /credit    /replay window
                  |
          Application handler / authorization
             |                    |
        WAL journal       Optional fenced cluster
       encrypted payload    lease/SQL outbox/forward
```

## Wire/runtime boundaries

- `mira/tlsbootstrap.go`: TLS 1.3 strict mutual CA verification, identity from verified client certificate, TLS exporter domain separation, random connection identifiers and UDP return-path validation. Normal data never rides TLS/TCP.
- `mira/transport.go`: native UDP listening, peer/session management, admission controls and native PSK profile (legacy experimental only).
- `mira/handshake.go`: AEAD and directional key evolution; legacy X25519/PSK handshake remains bespoke. TLS session crypto bypasses bespoke peer authentication but **not** the MIRA packet format.
- `mira/session.go`: per-session actor, scheduler, ACK ranges, retransmission, congestion/pacing budget, address validation and PMTU probes, UDP socket migration, session close. Retained ciphertext is retransmitted identically.
- `mira/channel.go`, `mira/receive.go`: channel open/close/write-half-close, ordered/unordered delivery, frame fragmentation/reassembly and per-message receipts. Sender's IDs advance only after send admission.
- `durable/`, `journal/`: WAL fsync, CRC/compaction, per-principal replay and optional authenticated at-rest envelope. These guarantee persistence only at **explicit application commit**, not at transport receipt.
- `gateway/`, `browser/`: authenticated HTTPS bridge to a Go MIRA client. Requests authenticate separately and gateway sessions bind to the authenticated principal.
- `cluster/`: rendezvous owner selection is advisory; lease fencing is authoritative. SQL outbox joins the same application business transaction; inter-node mTLS forwards explicit owned requests. The example server does not yet use these for transparent failover.
- `mobile/`: pure Go public wrapper; platform SDK distribution and background mode integration remain downstream work.

## Connection state machines

**TLS profile (recommended):** TLS listener accepts verified TLS 1.3 cert peer and selects `mira-t/1-udp-bootstrap` ALPN. The client/server derive independent symmetric traffic secrets from a bound TLS exporter and negotiate a new UDP connection. Client UDP proves reachability, server creates the session, and both sides enter concurrent channel processing. A previously disconnected client establishes a **fresh** TLS and UDP session. TLS resumption (if negotiated) does **not** resurrect old inflight MIRA state.

**Legacy profile:** per-device PSK + X25519 challenge/finish, retained for controlled experiments and tests. Do not expose legacy bootstrap publicly without an independent audit.

**Channel lifecycle:** local open request -> authenticated OPEN -> remote authorization -> OPEN_ACK; then DATA/ACK/RECEIPT. `CloseWrite` sends the last ordered message ID, remote Receive ends with EOF only after ordered delivery reaches that ID. A complete CLOSE/RESET ends both directions and cleans state with bounded tombstones.

## Delivery/recovery

- ACK_RANGES is a transport in-memory acceptance signal; 32 maximum ranges and 512 aggregate acknowledged numbers limit per-frame CPU work.
- Retransmission is time- and reordering-triggered with coarse AIMD window and pacing; no independent demonstration of friendliness to TCP/QUIC under persistent loss.
- `AckStored` and `AckProcessed` are **receiver-asserted**; receiver calls them only after the storage or business effects actually finish.
- Applications resume committed messages using `journal.Request` replay cursor after a new authenticated session. There is **no** encrypted transport ticket / inflight state resurrection.
- One-way 4096-packet epoch key evolution offers past-key erasure where possible; it does not restore security after compromise of current traffic key.

## Mobility and MTU

A packet arriving at a new UDP source address is not blindly accepted. Server sends authenticated challenge and switches only after an authenticated response, with amplification bounds. A Go client can switch its own socket via `Session.RebindClient(ctx)` without replacing the entire logical session. `Session.ProbeMTU(ctx, n)` explicitly tests a larger packet target (bounded by packet parser maximum); on success the sender may use larger fragments. No continuous MTU discovery or blackhole detection is implemented.

## Cluster deployment semantics

The durable lease store needs a production PostgreSQL driver, database migrations from `cluster/sql/schema.sql`, and a single authenticated cluster identity namespace. Every business write that depends on ownership must check the current lease fence inside the database transaction; the provided `Outbox.PublishTx` does this for the outbox insert. In-memory leases are strictly for tests. A production session router needs separate integration work to connect leases, node membership, a cross-node forwarding address, transactionally persisted delivery and reconnection. A partitioned node must be fenced before its effects can commit.

## Network/security constraints

A custom unreviewed UDP protocol needs stricter admission, congestion and cryptographic validation than an ordinary Go HTTP service. Use TLS-authenticated bootstrap, explicit principal authorization, safe resource caps, trusted time/cert management and crash-recoverable durable messages. Test hostile input, parser fuzzing, forced reordering and actual WAN path behavior. Do not infer independent security review from passing race or unit tests.
