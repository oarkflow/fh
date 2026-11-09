# Running MIRA for real application use cases

This guide takes you from a local working demo to choosing and operating a MIRA
integration. Commands are run from the repository root. The runnable programs
are the source of truth for the examples; links below point to their code.

> **Deployment status:** MIRA is suitable for development and gated internal
> staging. It is a custom transport without an independent cryptographic audit,
> WAN fairness certification, or mobile carrier validation. Do not put
> sensitive public traffic on it until the release gates in
> [DEPLOYMENT.md](DEPLOYMENT.md) and [MIRA_TASKS.md](../../MIRA_TASKS.md) have
> been completed.

## 1. Choose a starting point

| Application need | Start here | What the example demonstrates |
|---|---|---|
| Learn the protocol and embed it in a Go service | [quickstart](../../examples/mira/quickstart/main.go) | TLS 1.3 mutual authentication, a reliable channel, request/response and a receipt |
| Control connected devices or workers | [fleet](../../examples/mira/fleet) | Device identity policy, latest telemetry, command application acknowledgement and an HTTP operator API |
| Accept events from intermittently connected clients | [fieldsync](../../examples/mira/fieldsync/main.go) or [durable server/client](../../examples/mira/durable/README.md) | Encrypted WAL, stable publish IDs, restart/reconnect and application cursor replay |
| Upload large files | [filetransfer](../../examples/mira/filetransfer/main.go) | Chunked reliable transfer, digest verification and commit after fsync |
| Serve a browser UI | [browser example](../../examples/mira/browser) | HTTPS frontend and gateway boundary; browsers do not open native MIRA UDP |
| Run a certificate-authenticated server process | [native server/client](../../examples/mira/native) | PEM credentials, allow-list and separate TCP bootstrap / UDP data listeners |
| Own sessions across multiple server nodes | [cluster guide](../../examples/mira/cluster/README.md) | Fenced leases and forwarding building blocks; the application supplies PostgreSQL integration |
| Build a mobile binding | [mobile guide](../../examples/mira/mobile/README.md) | gomobile facade; platform lifecycle and packaging still need device validation |

## 2. Run the demos locally

The quickest demos create an in-memory throwaway CA and run entirely on
loopback. From the repository root:

```sh
go run ./examples/mira/quickstart
```

Expected result: the client connects, exchanges a message, receives the echo,
and reports a receipt. The process exits when the demonstration finishes.

### Device fleet

Terminal A:

```sh
go run ./examples/mira/fleet -devices 3
```

Terminal B:

```sh
curl -sS localhost:8080/fleet
curl -sS -X POST localhost:8080/fleet/device-1/commands \
  -H 'Content-Type: application/json' \
  --data '{"op":"set_setpoint","value":25}'
```

The command response returns only after the simulated thermostat applies and
confirms the command. Try a missing device (`device-99`) to see the offline
response. The example's HTTP listener is an operator API; put your own auth,
authorization and network controls in front of it before exposing it.

### Durable field sync

```sh
go run ./examples/mira/fieldsync
```

This self-contained scenario writes events, stops and restarts its local server
against the same encrypted WAL, retries an event using the same ID, then
replays from a checkpoint. It demonstrates why a transport receipt is not the
same as durable application processing. Adapt the patterns in
[fieldsync/main.go](../../examples/mira/fieldsync/main.go) or run the separate
server/client instructions in [durable/README.md](../../examples/mira/durable/README.md)
when you need a process you can keep running.

### File upload

Generate and upload a 16 MiB sample:

```sh
go run ./examples/mira/filetransfer -size 16
```

Upload an existing file:

```sh
go run ./examples/mira/filetransfer -file ./archive.tar
```

The demo uses a temporary destination and removes it at exit. In a service,
choose a controlled storage directory, enforce per-identity quotas, scan or
validate content as required, and only publish a file after verifying the
commit. The example writes a temporary file, fsyncs it, then atomically renames
it after the transfer digest is verified.

## 3. Connect a real certificate-authenticated client

For local integration only, generate disposable credentials:

```sh
go run ./cmd/mira-dev-certs -out ./dev-certs
```

Start the example server in terminal A:

```sh
go run ./examples/mira/native/server \
  -udp 127.0.0.1:7447 -tcp 127.0.0.1:7448 \
  -cert ./dev-certs/server.pem -key ./dev-certs/server.key \
  -ca ./dev-certs/root.pem -allow device-1
```

Connect from terminal B:

```sh
go run ./examples/mira/native/client \
  -tcp 127.0.0.1:7448 -udp 127.0.0.1:7447 \
  -cert ./dev-certs/client.pem -key ./dev-certs/client.key \
  -ca ./dev-certs/root.pem -name server.local \
  -message 'reading=21.5C'
```

Expected output includes `MIRA echo: reading=21.5C`. Delete `dev-certs/` when
finished. These test keys and the generated CA are not suitable for staging or
production.

In your Go application, the basic server lifecycle is:

```go
app := fh.New()
server, err := app.ListenMIRATLS(udpAddr, tcpAddr, serverTLS, fh.MIRAOptions{
    MaxSessions: 1000, MaxSessionsPerIdentity: 2, MaxChannels: 8,
})
if err != nil { return err }
server.SetChannelAuthorizer(func(identity string, cfg protocol.ChannelConfig) bool {
    return allowedIdentity(identity) && cfg.MaxMessage <= 16<<10
})
go app.ServeMIRA(ctx, server, func(ctx context.Context, identity string, ch fh.MIRAChannel) error {
    return serveApplicationChannel(ctx, identity, ch)
})
```

The certificate identity comes from the verified TLS certificate, not from a
message field. Authorize each channel at open and authorize each application
operation as well. Follow [quickstart](../../examples/mira/quickstart/main.go)
for imports, client setup and complete error handling. `fh.App` owns and closes
the MIRA listener as part of its lifecycle.

## 4. Pick delivery and acknowledgement semantics

Configure a channel for its data, not for convenience:

| Data | Delivery choice | Application rule |
|---|---|---|
| Commands where order matters | `ReliableOrdered` | Include a command ID; apply idempotently; return `AckProcessed` only after application succeeds |
| Independent events that must arrive | `ReliableUnordered` | Deduplicate by stable event ID |
| Replaceable sensor/dashboard snapshot | `LatestState` | Keep snapshots small and include a sequence or timestamp |
| Disposable high-rate hints | `BestEffort` | Application must tolerate loss |
| Large transfer | `Bulk` or the `mira/transfer` helper | Verify length and digest, then commit to final storage |

Transport delivery says the peer transport received data. `AckProcessed` means
the receiving application explicitly reported processing. `AckStored` means
the journal path explicitly persisted the event. None of these alone makes an
external database update or third-party API call exactly once; use an
idempotency key or transactional inbox/outbox at that boundary.

## 5. Make events survive process restarts

MIRA session keys and in-flight UDP state are intentionally fresh after a
process restart. Persist application events separately, reconnect with a new
mTLS session, and replay from an application cursor. For a runnable persistent
server, use [examples/mira/durable](../../examples/mira/durable/README.md):

1. Create local credentials with `go run ./cmd/mira-dev-certs -out ./dev-certs`.
2. Create a private WAL directory and keyring as shown in that guide.
3. Start `cmd/mira-tls-server` with `-wal` and `-wal-keys`.
4. Run the durable client with a stable `-id` and an absolute `-checkpoint` path.
5. Restart the server, then rerun the client; it reconnects and resumes replay.

The WAL is single-writer. Keep its keyring separate from the WAL, protect both
with owner-only permissions, back them up together under your recovery policy,
and test restore from a copy. Never share one WAL file among multiple server
processes. For business effects in a database, update the effect and consumed
cursor in one transaction or make the effect idempotent.

## 6. Use MIRA behind a browser or gateway

Browser JavaScript cannot open arbitrary UDP sockets. Use the HTTPS gateway
pattern: browser to gateway over HTTPS, then a gateway service identity to the
MIRA server. The gateway is a trusted application boundary and can see
plaintext. Validate browser origins, user authorization and signed principal
delegation; do not treat a shared demo bearer token as multi-user auth. See the
[browser example](../../examples/mira/browser) and gateway configuration in
[USER_GUIDE.md](USER_GUIDE.md).

## 7. Stage a server deployment

Use your organization's CA to issue a TLS server certificate with the correct
DNS SAN and separate client certificates for devices/services. Do not use the
development CA. For the stock server, create an allow-list with one verified
certificate identity per line, then run:

```sh
go build -trimpath -o ./mira-tls-server ./cmd/mira-tls-server
./mira-tls-server \
  -udp :7447 -tcp :7448 \
  -cert /etc/mira/server.pem -key /etc/mira/server.key \
  -ca /etc/mira/client-ca.pem -allow-file /etc/mira/allow.txt \
  -max-sessions 1000 -max-sessions-per-identity 4 -max-channels 16 \
  -metrics 127.0.0.1:9109
```

The matching client uses `cmd/mira-tls-client` with its own cert/key, the
server CA, and the server DNS name. Route TCP 7448 and UDP 7447 to the same
stateful node. Keep metrics on loopback or a protected management network.
Use the provided [systemd unit](../../deploy/mira-tls.service) or container
assets as a starting point; review service identity, secret mounts, file modes,
firewall rules, logging, monitoring, backups and rotation for your environment.

Before exposing a staging service, follow the full [deployment reference](DEPLOYMENT.md)
and [operator runbook](RUNBOOK.md). Public production remains gated on the
independent review, WAN/mobile soak, IPv6/NAT64 and fairness testing, and the
other checks listed there. A passing local example is not evidence that these
gates have passed.

## 8. Operating checklist

- Issue and rotate unique client certificates; authorize the certificate identity server-side.
- Expose only the TLS bootstrap TCP port and MIRA UDP port to intended networks.
- Route both ports for a session to the same node; live UDP session state is node-local.
- Set session, per-identity, channel, message and idle-time limits for expected use.
- Keep metrics private; alert on session failures, retransmits, resource ceilings and WAL space.
- Bound application queues and make retries idempotent with stable operation IDs.
- Test shutdown, reconnect, certificate rotation, ACL revocation, packet loss, disk full, backup and restore.
- For browsers, secure the HTTPS gateway separately and enforce per-user authorization there and at the backend.
- For mobile, implement foreground/background policy and reconnect after OS process eviction; resume durable data from an application cursor.
- Do not claim audited security, exactly-once external effects, WAN fairness, or QUIC/WebSocket performance without the corresponding validation.

## Further references

- [Full developer and operator guide](USER_GUIDE.md)
- [Deployment reference and release gates](DEPLOYMENT.md)
- [Operations runbook](RUNBOOK.md)
- [Architecture](ARCHITECTURE.md) and [runtime specification](RUNTIME_SPEC.md)
- [Runnable examples index](../../examples/mira/README.md)
