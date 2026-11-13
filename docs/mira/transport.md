# MIRA-T/1 native transport

fh includes the MIRA-T/1 v7 Go implementation as an **opt-in native UDP
transport**. It is independent of fh's HTTP/1.1, HTTP/2, WebSocket, and secure
browser envelope protocols. The MIRA wire format remains version 4.

## Choose the transport

- Use fh HTTP/1.1, HTTP/2, or WebSocket for ordinary web traffic.
- Use `mw/securetransport` with `wasm/` when browser Fetch needs its existing
  device-bound encrypted request envelope. Its v1 wire format is unchanged.
- Use `mira/` for native Go clients requiring encrypted UDP sessions and
  multiplexed channels. Browsers do not send MIRA UDP; they use the HTTPS
  bridge in `gateway/` and `browser/client.ts`.

The browser bridge terminates the MIRA transport at the gateway. HTTPS protects
the browser-to-gateway hop; mTLS protects the gateway-to-MIRA hop. Applications
must authorize the browser principal and each channel operation at the backend.
The bridge does not provide end-to-end transport confidentiality from the
browser to the backend.

Gateway event history is bounded by both event count and retained bytes. By
default, each session retains at most 1 MiB and the gateway retains at most
64 MiB across all sessions. `gateway.Config.MaxHistoryBytes` and
`MaxTotalHistoryBytes` tune those limits. If a message cannot fit or a cursor
falls behind evicted history, pull requests and streams with a stale initial
cursor return HTTP 409 so the application can recover from its durable backend.
An already-started stream closes if it later overruns retained history.

## Minimal native server

MIRA's TLS bootstrap accepts a standard `*tls.Config`, so callers can build the
certificate policy with fh's TLS helper and pass it to `mira.ListenTLS`:

```go
tlsConfig, err := fh.NewServerTLSConfig(fh.ServerTLSOptions{
	Certificates:             []tls.Certificate{serverCert},
	ClientCAs:                clientCAs,
	RequireClientCertificate: true,
})
if err != nil {
	log.Fatal(err)
}

server, err := mira.ListenTLS(
	":7447", ":7448", tlsConfig,
	mira.Options{MaxSessions: 1000, MaxSessionsPerIdentity: 4},
)
if err != nil {
	log.Fatal(err)
}
defer server.Close()
```

The listener is separate from `fh.App`'s HTTP listener. Certificate identity
must also be authorized for session and channel access; successful mTLS alone
does not grant application permissions. See [MIRA quick start](QUICK_START.md),
[deployment](DEPLOYMENT.md), and [runbook](RUNBOOK.md).

## Supporting packages

| Package | Role |
|---|---|
| `protocol/` | Channel, session, receipt, authorization, and durable store contracts |
| `mira/`, `internal/wire/` | UDP runtime, handshake, encrypted frames, flow control, loss recovery, mobility, MTU |
| `durable/`, `journal/`, `resume/` | WAL, optional encrypted storage, application replay after reconnect |
| `gateway/`, `browser/` | Authenticated HTTPS bridge and TypeScript browser client |
| `cluster/` | Fenced ownership, PostgreSQL lease/outbox adapters, mTLS forwarding |
| `mobile/` | Go facade intended for gomobile bindings |
| `delegation/` | Short-lived gateway principal assertions |

fh's `pkg/storage/kv` and `pkg/cluster` serve other middleware and HTTP
workloads. MIRA keeps its WAL and fenced lease contracts because their append,
transaction, and fencing semantics differ from those generic APIs. MIRA's
PostgreSQL adapter accepts the application's `*sql.DB`; the driver and schema
migrations remain application/deployment responsibilities.

## Verification and limits

Run `go test ./...`, `go test -race ./delegation ./gateway ./mira ./resume ./cluster`,
and `bash scripts/verify-v7.sh` on a host that permits local TCP/UDP sockets.
`make mira-mobile-android` and `make mira-mobile-ios` require gomobile and their
respective platform toolchains. The independent Python encrypted-peer test
requires Python's `cryptography` package.

The imported implementation is a staging-grade custom protocol, not an
independently audited replacement for QUIC, TLS, or WebSocket. Local tests do
not establish cryptographic security, Internet congestion fairness, complete
PLPMTUD behavior, production PostgreSQL failover, cellular/mobile lifecycle
behavior, or real-network interoperability. See [MIRA v7 status](../../V7_IMPLEMENTATION_STATUS.md)
and [MIRA security notes](../../MIRA_SECURITY.md) before deployment.
