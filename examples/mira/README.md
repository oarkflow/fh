# MIRA-T/1 for fh: guide and examples

For a step-by-step path from local demos to real application deployments, see
the [MIRA real-use guide](../../docs/mira/REAL_USE_GUIDE.md). This page is the
example catalog and technical reference.

MIRA is an opt-in, encrypted, multiplexed **UDP transport** for Go services and
clients, built into fh. It gives you authenticated sessions, many independent
channels per session, a delivery mode per channel, and application-level
receipts, without HTTP, WebSocket or QUIC underneath. It is separate from fh's
HTTP/1.1, HTTP/2, WebSocket and browser `securetransport` paths; one fh `App`
can run both side by side.

> **Status: controlled-staging quality, not audited.** The code has an
> extensive test suite (unit, fuzz, loss, race, an independent Python peer), but
> **no independent cryptographic review**, no WAN or Internet-congestion
> certification, and **no head-to-head benchmark against QUIC or WebSocket**.
> Do not put sensitive public traffic on it until you have done the review and
> validation in [`docs/mira/DEPLOYMENT.md`](../../docs/mira/DEPLOYMENT.md) and
> [`MIRA_TASKS.md`](../../MIRA_TASKS.md). Measured numbers are in
> [section 7](#7-performance-measured-and-how-to-reproduce-it); read their
> limits before relying on them.

## Contents

1. [What you get](#1-what-you-get)
2. [Where the code lives](#2-where-the-code-lives)
3. [Run something in 30 seconds](#3-run-something-in-30-seconds)
4. [Real-world use cases and what each example proves](#4-real-world-use-cases-and-what-each-example-proves)
5. [How the protocol works](#5-how-the-protocol-works)
6. [Choosing delivery modes and receipts](#6-choosing-delivery-modes-and-receipts)
7. [Performance, measured, and how to reproduce it](#7-performance-measured-and-how-to-reproduce-it)
8. [Production checklist](#8-production-checklist)
9. [Troubleshooting](#9-troubleshooting)
10. [Verification](#10-verification)

---

## 1. What you get

| You need | MIRA gives you | You still build |
|---|---|---|
| A private channel between your own clients and servers | mTLS-authenticated, AES-256-GCM encrypted UDP session; identity comes from the client certificate | Certificate issuance and rotation, authorization policy |
| Different traffic with different rules on one connection | Many channels per session; each picks reliable-ordered, reliable-unordered, best-effort, latest-state or bulk | Message schemas and meaning |
| Fresh telemetry that never queues behind commands | `LatestState` channel: only the newest snapshot matters, stale ones are dropped | Snapshot content |
| Proof a command actually took effect | Three receipt levels; `AckProcessed` is sent by the *receiver's application* after it applied the command | Safe, idempotent business logic |
| No lost data across crashes | `journal`/`durable`/`resume`: fsynced encrypted WAL, idempotent publish by ID, cursor replay | Idempotent handling in your effects |
| Bounded resource use | Hard limits: sessions, per-identity sessions, channels, message size, queued bytes, in-flight packets, inbound rate | Sizing for your load |
| Network changes without a new login | Authenticated path challenge/response while the process is alive | Reconnect UX after process death |

What it is **not**: a message broker, a user directory, a push-notification
service, or a replacement for HTTP. Browsers cannot send raw UDP; they reach
MIRA through the HTTPS [gateway](../../gateway) (the gateway sees plaintext).

## 2. Where the code lives

Reusable features are library packages. `examples/` only holds thin programs
that use them.

| Package | Purpose |
|---|---|
| [`mira`](../../mira) | The transport: `ListenTLS`, `DialTLS`, `Session`, channels, pacing, congestion control, PMTU, migration, limits |
| [`protocol`](../../protocol) | Public contracts: `Channel`, `ChannelConfig`, `Delivery`, `ReceiptLevel`, `Principal` |
| [`mira/mtls`](../../mira/mtls) | Load production PEM credentials into the TLS 1.3 mutual-auth configs MIRA requires |
| [`mira/devpki`](../../mira/devpki) | In-memory throwaway CA for tests and demos. **Never a production trust root** |
| [`mira/fleet`](../../mira/fleet) | Device-fleet pattern: server `Hub` (registry, policy, confirmed dispatch) and reconnecting device `Agent` |
| [`mira/transfer`](../../mira/transfer) | Verified file/stream transfer with a commit receipt |
| [`journal`](../../journal), [`durable`](../../durable), [`resume`](../../resume) | Durable messaging: encrypted WAL, idempotent publish, cursor replay, checkpoints |
| [`gateway`](../../gateway), [`delegation`](../../delegation), [`browser`](../../browser) | HTTPS bridge for browsers, signed principal delegation, TypeScript client |
| [`cluster`](../../cluster) | Fenced leases and outbox building blocks for multi-node ownership |
| [`mobile`](../../mobile) | gomobile facade |
| root `fh` | `App.ListenMIRATLS`, `App.ServeMIRA`, `NewMIRAClient`, `DialMIRATLS`: lifecycle integration (closing the `App` closes MIRA) |

## 3. Run something in 30 seconds

Every example below runs with a bare `go run` and generates its own throwaway
CA in memory. Run them from the repository root.

```sh
go run ./examples/mira/quickstart            # smallest complete program
go run ./examples/mira/fleet -devices 3      # HTTP control plane + MIRA devices
go run ./examples/mira/fieldsync             # crash-safe durable capture
go run ./examples/mira/filetransfer -size 64 # verified 64 MiB upload
```

Fleet, in a second terminal:

```sh
curl localhost:8080/fleet
curl -X POST localhost:8080/fleet/device-1/commands \
  -H 'Content-Type: application/json' \
  --data '{"op":"set_setpoint","value":25}'
# {"message_id":1,"processed":true}   <- the device confirmed it applied it
```

Keep `set_setpoint` exactly as written; do not put a backslash before the
underscore. The body must be valid JSON, and the command operation name is
case-sensitive.

## 4. Real-world use cases and what each example proves

| Example | Real-world use | What the user achieves | Core packages |
|---|---|---|---|
| [`quickstart`](quickstart) | First integration | mTLS bootstrap, a reliable channel, a receipt and live session stats in about 100 lines | `fh`, `mira/devpki` |
| [`fleet`](fleet) | IoT, robotics, building control, fleet dashboards, game servers | Thousands of devices keep one encrypted session each. Telemetry never blocks commands. An operator calls a plain HTTP API and gets `processed:true` only after the device applied the command; offline devices get 404, unconfirmed ones 504. Unauthorized certificates cannot even open a channel | `mira/fleet`, `fh.App` |
| [`fieldsync`](fieldsync) | Field data capture, point-of-sale, inspection apps, anything on flaky links | Publish keeps working through a server crash. Retries with the same ID never duplicate. Replay resumes from a checkpoint with zero loss and zero redelivery. The WAL is encrypted at rest | `journal`, `durable`, `resume` |
| [`filetransfer`](filetransfer) | Log/backup shipping, media upload, firmware distribution | Upload of any size with end-to-end SHA-256. The client sees success only after the server fsynced and atomically renamed the file; a failed commit or digest mismatch is reported to the sender and leaves no partial file | `mira/transfer` |
| [`native/server`, `native/client`](native) | Plain mTLS echo with an identity allow-list, using flags and PEM files | The same wiring with flag-driven PEM loading and `SetChannelAuthorizer`, in its simplest form (it predates `mira/mtls` and loads PEMs itself) | `fh`, `protocol` |
| [`browser`](browser) | Operations console in a web page | A TypeScript page that talks to a backend MIRA session through the HTTPS gateway | `gateway`, `browser` |
| [`durable`](durable) | The same durable journal against the stock `mira-tls-server` binary | Operating the WAL, key file and cursor files as a service | `cmd/mira-tls-server` |
| [`cluster`](cluster), [`mobile`](mobile) | Multi-node ownership; Android and iOS | Configuration guides. They need your database driver or platform toolchain | `cluster`, `mobile` |

`quickstart`, `fleet`, `fieldsync` and `filetransfer` each have a test
(`go test ./examples/mira/...`) that exercises the real transport over loopback
UDP, including failure paths: an unconfirmed command and an offline device
(`fleet`), a crashed server and duplicate retries (`fieldsync`), and a failed
commit (`filetransfer`). The library tests in `mira/fleet` and `mira/transfer`
also cover unauthorized identities, a corrupted digest, reconnect and oversize
rejection.

### The three patterns, in code

**Server (any example):** the `App` owns the listener; channel opens fail closed
until you install a policy.

```go
app := fh.New()
server, _ := app.ListenMIRATLS(":7447", ":7448", tlsConfig, fh.MIRAOptions{MaxSessionsPerIdentity: 2})
server.SetChannelAuthorizer(func(identity string, cfg protocol.ChannelConfig) bool {
    return strings.HasPrefix(identity, "device-") && cfg.MaxMessage <= 16<<10
})
go app.ServeMIRA(ctx, server, func(ctx context.Context, identity string, ch fh.MIRAChannel) error {
    // identity is the authenticated certificate name, never a payload field
    return handle(ctx, identity, ch)
})
```

**Confirmed command (receiver applies, then acknowledges):**

```go
// sender
id, _ := ch.Send(ctx, cmd)
_, err := ch.AwaitReceipt(ctx, id, protocol.AckProcessed) // err == nil: it took effect

// receiver
msg, _ := ch.Receive(ctx)
if apply(msg.Payload) == nil {
    ch.Acknowledge(ctx, msg.MessageID, protocol.AckProcessed)
}
```

**Latest-state telemetry:** open it with `protocol.LatestState`; each `Send` may
replace an older unsent snapshot. Keep snapshots under 900 bytes (one datagram).

## 5. How the protocol works

### 5.1 Connection and security flow

```text
Client                              Server
  |--- TLS 1.3 over TCP, mutual ----->|  both certificates verified; server name checked
  |<-- connection ID -----------------|  a TLS exporter derives fresh per-direction UDP keys
  |                                   |
  |=== encrypted UDP probe ==========>|  proves the client can receive at its claimed address
  |<== authenticated path response ===|
  |                                   |
  |--- OPEN (mode, priority, limits) ->|  server runs your ChannelAuthorizer for this identity
  |<-- OPEN_ACK ----------------------|
  |<== DATA / ACK_RANGES / RECEIPT ===>|  both sides send concurrently
```

The TCP/TLS connection exists only to authenticate and to derive keys; the key
material is never sent over UDP. The certificate CommonName (or URI SAN) becomes
the session identity. The TLS socket closes after bootstrap, and all application
data then moves over UDP. There is also a legacy PSK/X25519 handshake for
loopback development only; do not expose it.

### 5.2 UDP packet format (wire version 4)

Every datagram starts with a fixed 26-byte header, big-endian:

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 2 | Magic `0x4D54` | ASCII `MT` |
| 2 | 1 | Version `4` | other versions are rejected |
| 3 | 1 | Kind | `1` HELLO, `2` RETRY, `3` HELLO_ACK, `4` FINISH, `5` DATA, `6` RESET |
| 4 | 1 | Flags | for DATA, bit 0 = ACK required |
| 5 | 1 | Header length | always `26` |
| 6 | 8 | Connection ID | random per session |
| 14 | 8 | Packet number | per direction; never reused under the same key |
| 22 | 2 | Key epoch | selects the directional traffic key |
| 24 | 2 | Body length | exact bytes after the header |

A DATA body is AES-256-GCM ciphertext plus a 16-byte tag. **All 26 header bytes,
including the body length, are authenticated as associated data**, so a modified
header fails decryption. The parser accepts datagrams up to 1,472 bytes and
rejects wrong magic, version, length or kind before any frame is parsed.

### 5.3 Frames inside a DATA packet

The decrypted body is one or more frames:

```text
TYPE (1) | FLAGS (1) | BODY_LENGTH (canonical varint) | BODY
```

| Type | Frame | Role |
|---:|---|---|
| 1 | DATA | channel ID, message ID, total length, offset, fragment size, bytes |
| 2 | ACK | legacy single-packet ack |
| 3, 4 | OPEN, OPEN_ACK | negotiate a channel's mode, priority, limits, lease |
| 5, 6 | CLOSE, RESET | close normally or abort a channel |
| 7 | CREDIT | raise the absolute receive-byte limit (flow control) |
| 8 | PING | liveness |
| 9 | RECEIPT | application receipt (`AckStored`, `AckProcessed`) |
| 11, 12 | PATH_CHALLENGE, PATH_RESPONSE | validate a changed network address |
| 14 | GOAWAY | end the session |
| 15 | ACK_RANGES | bounded inclusive ranges of acknowledged packet numbers |
| 16 | MTU_PROBE | authenticated padded packet to test path size |
| 17 | HALF_CLOSE | end one direction of an ordered channel |

IDs 10 (CANCEL) and 13 (KEY_UPDATE) are parsed but not active operations.
Non-canonical varints, truncated bodies, unknown types and oversize bodies are
rejected.

### 5.4 Reliability, flow control and keys

1. **Fragmentation.** Reliable messages are split into 900-byte fragments (up to
   1 MiB per message). Best-effort and latest-state messages must fit one datagram.
2. **ACK ranges.** Receivers batch accepted packet numbers into ACK_RANGES frames,
   with a 1 ms delayed-ack timer so bursts coalesce.
3. **Retransmission.** Lost packets are resent as the *same ciphertext*, so a
   nonce is never reused under one key.
4. **Congestion control and pacing.** A congestion window bounds bytes in flight;
   a virtual-time pacer spaces sends (short bursts allowed) so a window is not
   dumped onto the path at once.
5. **Flow control.** CREDIT frames bound how many bytes a receiver will buffer;
   senders see `ErrBackpressure` instead of unbounded queues.
6. **Replay protection.** A sliding window rejects duplicate or too-old packet
   numbers before frames are processed.
7. **Key evolution.** Directional keys advance every 4,096 packets. This is
   forward evolution of a symmetric chain, **not** a fresh-DH ratchet: it does not
   repair a compromised current key.
8. **Path MTU.** Sessions start conservative (1,200) and probe upward with
   authenticated probes, with black-hole recovery.
9. **Migration.** If the peer's address changes while the process lives, a
   challenge/response validates the new path. After process death, the client
   re-authenticates with TLS and opens a new session; durable replay is the
   application's job (`journal`/`resume`).

## 6. Choosing delivery modes and receipts

| `Delivery` | Use when | Trade-off |
|---|---|---|
| `ReliableOrdered` | commands, events that must arrive in order | a lost earlier message delays later ones on that channel |
| `ReliableUnordered` | every message matters, order does not | you must tolerate reordering |
| `BestEffort` | freshness over completeness | no retransmission; one datagram max |
| `LatestState` | only the newest snapshot matters | replaces obsolete *queued* state; cannot retract what was already sent |
| `Bulk` | large reliable payloads | needs explicit size, quota and storage policy |

Use separate channels for traffic with different needs (the fleet example opens
one `ReliableOrdered` and one `LatestState` channel per device). Channels share
the session's congestion window and path, so a channel split changes queueing
policy, not physics.

| Receipt | Meaning | Who sends it |
|---|---|---|
| `AckTransport` | the peer's transport accepted the bytes | automatic |
| `AckStored` | the receiver durably stored it | receiver's application (`ch.Acknowledge`) |
| `AckProcessed` | the receiver applied it | receiver's application |

A transport receipt alone never means "the work happened". For exactly-once
*effects* you also need idempotency or an outbox/inbox in the receiver's own
transaction.

## 7. Performance, measured, and how to reproduce it

Two environments, both **single hop with no loss and no competing traffic**, so
they show CPU and scheduling behavior, not Internet behavior. They are **not** a
comparison with QUIC or WebSocket.

- **Laptop:** Apple silicon, darwin/arm64, Go 1.27.3, loopback UDP.
- **Linux server:** x86-64, 4 vCPU, Linux 6.8, shared with ~20 unrelated
  containers, server in Docker (hardened, 512 MiB / 1.5 CPU limits), clients in
  a second container on a Docker bridge network
  (see [`deploy/mira`](../../deploy/mira)).

Echo round trip including the transport receipt (`cmd/mira-bench`):

| Workload | Laptop p50 / p99 | Linux server p50 / p99 | Linux server throughput |
|---|---|---|---|
| 256 B, 1 session | 1.30 / 1.61 ms | 1.31 / 1.95 ms | ~740 round trips/s |
| 256 B, 8 sessions | | 1.60 / 2.65 ms | ~4,600 round trips/s |
| 16 KiB, 1 session | 1.09 / 2.19 ms | 2.14 / 3.85 ms | ~376 round trips/s |
| 64 KiB, 1 session | | 10.4 / 13.7 ms | ~88 round trips/s |

Bulk upload, 64 MiB over one channel (`examples/mira/filetransfer`, laptop):
~20 MiB/s with `MaxInflight` 128, ~9 MiB/s at 32 (the default was 32 when this was measured; it is now 256, see below).

Also verified on the Linux server: a client certificate from a foreign CA is
refused at the TLS bootstrap; removing an identity from the allow-list and
sending SIGHUP refuses it on the next connection, and restoring it admits it
again; the container stayed `healthy` with ~8 MiB RSS and ~0.05% CPU idle. One
50-message run after a reload showed a single 106 ms outlier that I did not
reproduce or explain; treat p99 on short runs as a maximum.

### Long paths (134 ms RTT): measured on the real link, reproduced in an emulator

Between a laptop and the Linux server above, over the real internet (ICMP RTT
134 ms, no loss observed), **before** the long-path fixes:

| Echo + receipt | p50 | RTTs |
|---|---:|---:|
| 256 B | 133 ms | 1.0 |
| 4 KiB | 173 ms | 1.3 |
| 16 KiB | 630 ms | 4.7 |
| 64 KiB | 2,134 ms | 16 |

`internal/netsim` is a deterministic UDP path emulator (delay, jitter, loss,
bottleneck queue) that reproduces this on a laptop in seconds. With a 134 ms
path and no loss, before and after the fixes below:

| Echo message | Before | After |
|---|---:|---:|
| 16 KiB | 3.6 RTT | **2.3 RTT** |
| 64 KiB | 7.3 RTT | **4.2 RTT** |
| 256 KiB | 18.6 RTT | **8.0 RTT** |
| Session setup (dial + open a channel) | | 279 ms (2.1 RTT) |

The emulator reproduced the *shape* of the real-link result (small messages
cost one RTT, large ones many). **The after-fix numbers above are emulated:
they have not been re-measured on the real link**, because that needs the
firewall opened again. Treat them as strong evidence, not as a field result.

Impaired paths still behave poorly. Measured in the emulator (134 ms RTT, 256 KiB
echo), **unchanged by the fixes below**:

| Path | 256 KiB echo |
|---|---:|
| 1% loss | ~29 RTT |
| 3% loss | ~52 RTT |
| 30 ms jitter, no loss | ~40 RTT |
| 10 MB/s bottleneck, 256 KiB queue | ~9.6 RTT |

Two causes, both open:

- **Loss response is Reno-like** (halve on loss, then +1 packet per RTT). At
  134 ms and 1% loss, Reno-class control is inherently limited to ~100 KB/s. A
  CUBIC or BBR controller would help.
- **Reordering is misread as loss.** Fast retransmit uses a fixed 3-packet
  threshold while pacing sends bursts, so jitter alone triggers needless
  retransmits and window cuts. It needs an adaptive reordering threshold.

Correctness under loss and jitter is covered by `TestImpairedPathsStayCorrect`;
speed there is not.

Still **not** measured: real-link behavior after the long-path fixes, real loss
or reordering, many thousands of sessions, long soak, NAT and mobile networks,
and any comparison with other transports.

### What changed in this integration (transport fixes)

Profiling the imported code found scheduling and congestion defects that capped
every number above. All are in `mira/session.go` and covered by the full suite
(race detector, plus loss/congestion tests repeated 8–10 times):

| Defect | Cause | Effect before → after |
|---|---|---|
| Paced sends had no wake-up | After one packet the pacer delayed the next, but nothing woke the session loop until the next inbound packet or its 5 ms ticker | bulk 0.17 → ~10 MiB/s |
| ACKs left only on a 5 ms ticker | Every transport receipt waited for the next tick | echo + receipt p50 5.0 → 1.29 ms |
| Pacing rate used an RTT floored at 25 ms | Rate was `cwnd / 25 ms` even on a 1 ms path, throttling multi-fragment messages | 16 KiB echo p50 7.8 → 1.09 ms, p99 38 → 2.2 ms (laptop) |

| Startup control frames counted as congestion | The server deliberately ignores traffic from a client's UDP path until validation finishes, so the first CREDIT/PATH_RESPONSE packets are lost by design. Their retransmission halved `cwnd` and ended slow start for the whole session | 64 KiB echo 7.3 → 4.2 RTT, 256 KiB 18.6 → 8.0 RTT (emulated 134 ms) |
| RTO had no margin, and the first RTO was 250 ms | `srtt + 4·rttvar` collapses to ~RTT on a stable path; the first RTO fired before any ACK was possible | spurious retransmits removed |
| Slow start paced at 1× | The window could not double each RTT | part of the figures above |
| `MaxInflight` default 32 | Capped a 134 ms path near 0.3 MB/s | default is now 256 |

Regression guards: `mira/transfer` `TestRoundTripLargeFile` fails below 1 MiB/s
on loopback; `mira` `TestLargeMessageRoundTripBudget` fails if a 64 KiB or
256 KiB echo exceeds 6 or 11 RTT on the emulated 134 ms path. Run the full
before/after reports with `MIRA_WAN_REPORT=1 go test ./mira -run WAN -v`.

### Tuning

| Option | Default | Raise when | Cost |
|---|---:|---|---|
| `MaxInflight` | 256 (was 32) | very fast or very long paths; throughput is capped near `MaxInflight × MTU / RTT` (256 allows ~2.7 MB/s at 134 ms RTT, 32 only ~0.3 MB/s) | up to ~380 KB of un-acked ciphertext per session |
| `MaxMessage` | 256 KiB (cap 1 MiB) | large reliable messages | receive buffering |
| `MaxQueuedBytes` | 4 MiB | bursty producers | send-side memory |
| `MaxReceiveBytes` | 8 MiB | slow consumers on fast links | receive-side memory |
| `MaxChannels` | 32 | many logical streams | per-channel state |
| `MaxSessionsPerIdentity` | 0 (unlimited); `mira-tls-server` sets 4 | one identity legitimately runs many sessions (a gateway, a load test) | one compromised credential can open more sessions |

`mira-tls-server` exposes these as flags: `-max-sessions`,
`-max-sessions-per-identity`, `-max-channels`, `-max-inflight`, `-idle-timeout`
and `-max-session-lifetime`.

Reproduce:

```sh
go run ./cmd/mira-dev-certs -out ./dev-certs -client bench-1
go run ./cmd/mira-tls-server -udp 127.0.0.1:7447 -tcp 127.0.0.1:7448 \
  -cert dev-certs/server.pem -key dev-certs/server.key -ca dev-certs/root.pem \
  -allow bench-1 -metrics ""
go run ./cmd/mira-bench -tcp 127.0.0.1:7448 -udp 127.0.0.1:7447 \
  -cert dev-certs/client.pem -key dev-certs/client.key -ca dev-certs/root.pem \
  -name server.local -clients 4 -messages 1000 -bytes 256
```

Before claiming MIRA is faster than anything else for your workload, benchmark
your real topology (WAN RTT, loss, middleboxes) against the alternative you
would otherwise use.

## 8. Production checklist

- [ ] **Real PKI.** Issue server and client certificates from your CA; load them
      with `mira/mtls` (or `internal/certconfig` for SIGHUP reload as in
      `cmd/mira-tls-server`). Never ship `mira/devpki`.
- [ ] **Authorizer installed.** `ListenTLS` already sets `RequireChannelAuthorizer`;
      your callback should check identity *and* the requested `ChannelConfig`
      (mode, `MaxMessage`).
- [ ] **Limits sized.** `MaxSessions`, `MaxSessionsPerIdentity`, `MaxChannels`,
      `MaxMessage`, `MaxInboundBytesPerSecond`, `IdleTimeout`, `MaxSessionLifetime`.
- [ ] **Identity is never taken from a payload.** Use the identity passed to your
      handler. The fleet `Hub` overwrites it on purpose.
- [ ] **Revocation path.** `server.RevokeIdentity(id)` drops live sessions; persist
      revocations in your credential source too.
- [ ] **Durable data.** Encrypted WAL key file `chmod 600`, kept apart from WAL
      backups; effects idempotent; cursor files single-writer.
- [ ] **Observability.** `server.WritePrometheus(w)` for metrics; `Session.Stats(ctx)`
      for RTT, cwnd, retransmissions, MTU, key epochs.
- [ ] **Firewall.** Allow the UDP data port and the TCP bootstrap port; UDP
      often needs explicit NAT keepalive tuning on mobile networks.
- [ ] **Container deployment.** See [`deploy/mira`](../../deploy/mira): scratch image, healthcheck, read-only root, all capabilities dropped, resource limits, loopback-only ports by default.
- [ ] **Graceful shutdown.** Close the `App` (`app.Shutdown()`); it closes MIRA
      listeners and sessions.
- [ ] **Independent review** of the cryptographic design and a WAN soak test on
      your real network, per [`MIRA_TASKS.md`](../../MIRA_TASKS.md).

## 9. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `Connect` fails immediately | Wrong CA, wrong `ServerName`, or the server did not require/receive a client certificate. MIRA needs TLS 1.3 and mutual auth |
| `OpenChannel` returns `mira: closed` | Your `ChannelAuthorizer` refused the identity or config, or `MaxChannels` was exceeded |
| `ErrBackpressure` on send | Send queue full. Slow down, raise `MaxQueuedBytes`, or wait: this is the intended bound |
| `LatestState`/`BestEffort` send error | Payload over 900 bytes: it must fit one datagram |
| Command times out although device is online | Receiver never called `Acknowledge(AckProcessed)` (or its apply step failed) |
| Works on loopback, stalls across a network | UDP blocked or NAT timeout; check the UDP port and `IdleTimeout`; inspect `Session.Stats` retransmissions |
| `go test` flakes under heavy CI load | Timing-based tests; rerun with `-count=3`; investigate any *repeatable* failure |

## 10. Verification

```sh
go build ./...
go vet ./mira/... ./journal/... ./examples/mira/...
go test -race -count=1 ./mira/... ./internal/... ./gateway/... ./resume/... \
  ./journal/... ./cluster/... ./durable/... ./delegation/... ./examples/mira/...
python3 -m unittest discover -s conformance -p 'test_wire_v4.py' -v
```

`conformance/test_peer_v7.py` runs an independent Python encrypted peer against
the Go implementation (needs the `cryptography` package). None of these is a
cryptographic audit or an Internet certification.

## Further reading

- [Architecture](../../docs/mira/ARCHITECTURE.md), [runtime specification](../../docs/mira/RUNTIME_SPEC.md)
- [Complete user and operator guide](../../docs/mira/USER_GUIDE.md), [quick start](../../docs/mira/QUICK_START.md)
- [Deployment](../../docs/mira/DEPLOYMENT.md), [runbook](../../docs/mira/RUNBOOK.md)
- [fh integration and trust boundary](../../docs/mira/transport.md)
- [Security notice](../../MIRA_SECURITY.md), [open validation tasks](../../MIRA_TASKS.md)
