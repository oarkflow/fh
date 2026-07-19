# HTTP/2

fh implements HTTP/2 (RFC 7540 / RFC 9113) entirely from scratch, including HPACK compression (RFC 7541).

## Features

- **TLS ALPN Negotiation** — Automatic h2 negotiation over TLS (requires `ListenTLS`)
- **h2c Prior Knowledge** — HTTP/2 over cleartext TCP
- **h2c Upgrade** — Upgrade from HTTP/1.1 to HTTP/2 via `Upgrade: h2c` header
- **Full Frame Support** — DATA, HEADERS, PRIORITY, RST_STREAM, SETTINGS, PUSH_PROMISE, PING, GOAWAY, WINDOW_UPDATE, CONTINUATION
- **Stream Multiplexing** — Multiple concurrent streams over a single TCP connection
- **Flow Control** — Per-stream and per-connection flow control with WINDOW_UPDATE
- **HPACK** — Custom HPACK encoder/decoder with Huffman coding
- **Server Push** — PUSH_PROMISE support
- **Connection Management** — SETTINGS exchange, PING keepalive, GOAWAY graceful shutdown
- **Extended CONNECT (RFC 8441)** — bidirectional-stream tunnels over a single HTTP/2 stream, e.g. WebSocket without falling back to HTTP/1.1

## Configuration

HTTP/2 is enabled by default. Disable it:

```go
app := fh.NewWithConfig(fh.Config{
    DisableHTTP2: true,
})
```

### HTTP/2-Specific Settings

```go
app := fh.NewWithConfig(fh.Config{
    MaxConcurrentStreams: 256, // default: 128
    HTTP2IdleTimeout:     60 * time.Second,
    RequestBodyTimeout:   10 * time.Second,
})
```

### HTTP/2 Constants

| Constant | Default | Description |
|----------|---------|-------------|
| Initial Window | 65535 | Initial flow control window size |
| Default Frame Size | 16384 | Default DATA frame size |
| Max Window Size | 2147483647 | Maximum flow control window |
| Settings Timeout | 10s | Timeout for SETTINGS ACK |
| Max Continuation Frames | 64 | Max CONTINUATION frames |
| Max RST_STREAM per min | 60 | Max reset frames per minute |

## Usage Modes

### TLS with ALPN (Recommended)

```go
app.ListenTLS(":443", "cert.pem", "key.pem")
// Automatically negotiates h2 or http/1.1 via ALPN
```

### h2c (Cleartext)

```go
// Server starts HTTP/1.1, client sends HTTP/2 preface
// Server auto-detects and switches to HTTP/2
app.Listen(":8080")
```

Cleartext HTTP/2 should normally be limited to a trusted internal network. To
serve HTTP/2 only through TLS/ALPN, use `fh.WithDisableH2C(true)`. The
`SecureByDefault` profile applies this setting automatically.

### h2c Upgrade

```go
// Client sends:
// GET / HTTP/1.1
// Upgrade: h2c
// HTTP2-Settings: <base64>

// Server responds:
// HTTP/1.1 101 Switching Protocols
// Connection: Upgrade
// Upgrade: h2c
```

### Extended CONNECT (RFC 8441)

fh advertises `SETTINGS_ENABLE_CONNECT_PROTOCOL` and accepts extended CONNECT
requests (`:method: CONNECT` with a `:protocol` pseudo-header) as
bidirectional tunnels — the same primitive HTTP/2 clients use to run
WebSocket without an HTTP/1.1 fallback. It is exposed through the same
`c.Upgrade(protocol, handler)` API used for HTTP/1.1 upgrades in
[websocket.go](../websocket.go):

```go
app.Get("/ws", func(c fh.Ctx) error {
    return c.Upgrade("websocket", func(conn net.Conn) error {
        // conn works identically whether the client spoke HTTP/1.1
        // (Connection: Upgrade) or HTTP/2 (extended CONNECT) — no branching
        // needed in handler code.
        ...
    })
})
```

`c.ConnectProtocol()` reports the negotiated `:protocol` value for an HTTP/2
extended-CONNECT request (`""` for HTTP/1.1 requests, where the equivalent
signal is the `Upgrade` header instead). `pkg/websocket` already uses both of
these internally, so existing `app.Get("/ws", websocket.New(...))` handlers
serve HTTP/2 clients with zero code changes — see [WebSocket](websocket.md).

Because an extended-CONNECT request addresses a resource like any other
request (unlike classic proxy-style CONNECT, which only carries an
`:authority`), fh routes it against the same table as a `GET` to that path —
register the handler once with `app.Get`, exactly as for HTTP/1.1 upgrades.

## Testing

```bash
# TLS with ALPN (requires TLS certs)
go run examples/http2/main.go

# h2c prior knowledge
curl --http2-prior-knowledge http://localhost:8080

# h2c upgrade
curl --http2 http://localhost:8080
```

## Internal Architecture

- `http2.go` — Main HTTP/2 implementation
- `pkg/hpack/` — HPACK encoder/decoder with Huffman encoding
- Frame read/write, stream management, flow control, SETTINGS exchange
- Automatic detection: reads first bytes, checks for HTTP/2 client preface (`PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n`)
