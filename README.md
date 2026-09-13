# fh — Zero-Dependency Go Web Framework

**fh** is a standalone, high-performance HTTP/1.1 + HTTP/2 + WebSocket web framework for Go with **no third-party dependencies beyond `golang.org/x/crypto`** (used only for optional OCSP stapling and ACME certificate automation). It implements HTTP parsing, routing, HTTP/2 framing, HPACK, and WebSocket protocols from scratch — no wrappers around `net/http` or `fasthttp`.

Full reference documentation lives in [`docs/`](docs/README.md).

## Features

- **Minimal dependencies** — the Go standard library plus `golang.org/x/crypto`, both optional (OCSP stapling, ACME)
- **HTTP/1.1** — full request/response parsing, chunked transfer, trailers
- **HTTP/2** — TLS ALPN, optional h2c prior knowledge/upgrade, stream multiplexing, flow control, RFC 8441 extended CONNECT
- **WebSocket** — RFC 6455 server implementation with `EventHub` pub/sub layer, transparently served over HTTP/1.1 or HTTP/2
- **Trie-based router** — radix tree with named (`:param`) and wildcard (`*wild`) parameters
- **Codec system** — pluggable body parsers for JSON, XML, form, multipart, CSV, NDJSON, text, binary
- **70+ built-in middleware packages** — see [Middleware](#middleware) below
- **Typed endpoints & OpenAPI 3.1** — generic request/response handlers with auto-generated specs
- **Reliability layer** — request journaling, idempotency, durable async queue, outbox/inbox, DLQ
- **Compliance layer** — Business/Professional/Enterprise/Security profiles, audit ledger, route security metadata
- **Opt-in fail-closed baseline** — `fh.WithSecureByDefault(true)` bounds every protocol input, enables strict parsing, recovery, redaction, and hardened response headers
- **Template engine** — agnostic interface, any engine implementing `Render(w, name, data, layout...)`
- **Static file serving** — direct streaming, range requests, streaming gzip, precompressed Brotli/gzip, cache control
- **Streaming uploads** — opt-in incremental HTTP/1 body consumption with bounded draining and trailer support
- **Graceful shutdown** — `app.ServeContext(ctx, listener)`, `app.ListenContext(ctx, addr)`, `app.ListenUnixContext(ctx, path)`, `app.ShutdownWithContext(ctx)`, or `app.ListenWithGracefulShutdown(addr)`
- **Graceful TLS shutdown** — `app.ListenTLSWithGracefulShutdown(addr, certFile, keyFile)`
- **Pool-based zero-allocation** — `sync.Pool` for contexts, byte buffers, HPACK decoders
- **Hardened TLS/mTLS** — TLS 1.3 config builder, verified peer state in request contexts, atomic certificate reload
- **ACME / Let's Encrypt** — `app.ListenAutoTLS(domains, cacheDir)` issues and renews certificates automatically via TLS-ALPN-01
- **Prefork & zero-downtime restarts** — `app.ListenPrefork(addr)` runs a multi-process `SO_REUSEPORT` supervisor; `SIGHUP` rolls out a new binary with zero dropped connections
- **Outbound HTTP client** — connection pooling, retries, circuit breaker, SSRF protection (`fh.NewClient`)
- **Linux kernel-assisted transport** — raw sockets, sharded epoll or io_uring, SO_REUSEPORT CPU steering, socket tuning, and optional XDP admission with safe fallback

## Installation

```bash
go get github.com/oarkflow/fh
```

Requires Go 1.26.5 or later.

## Quick Start

```go
package main

import (
    "log"

    "github.com/oarkflow/fh"
)

func main() {
    app := fh.New()

    app.Get("/", func(c fh.Ctx) error {
        return c.SendString("Hello, World!")
    })

    log.Fatal(app.ListenWithGracefulShutdown(":8080"))
}
```

## Cross-platform kernel-assisted transport

`fh` keeps protocol parsing, TLS, routing, middleware, reliability and handlers in memory-safe Go while using the native kernel network facility on each supported server OS:

- Linux: raw nonblocking sockets with sharded `epoll`; opt-in probed `io_uring`; optional `SO_REUSEPORT` BPF steering and XDP admission.
- macOS and BSD: raw nonblocking sockets with sharded `kqueue` accept reactors.
- Windows: IOCP/overlapped networking through Go's native network poller.
- Solaris/illumos: event ports through Go's native network poller.
- AIX: pollset through Go's native network poller.
- Other server-capable targets: functional native listener backend.

The balanced production profile does not automatically select the newer custom
`io_uring` path. Use the throughput profile or explicitly request `io_uring`
after benchmarking and canary testing it on the deployment kernel.

```go
kernel := fh.ProductionKernelConfig()
kernel.Required = true

app := fh.NewProduction(fh.WithKernel(kernel))
if err := app.ValidateKernelProduction(); err != nil {
    log.Fatal(err)
}
app.Get("/", func(c fh.Ctx) error { return c.SendString("kernel-assisted") })
log.Fatal(app.ListenWithGracefulShutdown(":8080"))
```

For an aggressive throughput candidate:

```go
kernel := fh.HighPerformanceKernelConfig()
// On Linux this permits probed io_uring auto-selection. Benchmark it against
// ProductionKernelConfig before deployment.
```

Inspect `app.KernelRuntimeInfo()` and `app.KernelReadiness()` at runtime. They
report the backend that actually started, fallbacks, connection admission,
socket-option failures and deployment warnings. Readiness always requires a
workload benchmark because no static configuration is universally fastest.

Probe Linux capabilities without changing the host:

```bash
go run ./cmd/fh-kernelctl probe
```

Optional XDP support is Linux-only and never attached unless explicitly enabled.
See [kernel-assisted transport](docs/kernel-transport.md) and the
[complete example](examples/kernel_server).

## Routing

```go
app.Get("/path", handler)
app.Post("/path", handler)
app.Put("/path", handler)
app.Delete("/path", handler)
app.Patch("/path", handler)
app.Head("/path", handler)
app.Options("/path", handler)
app.All("/path", handler)          // register all methods
app.Add("GET", "/path", handler)   // explicit method string

// Route parameters
app.Get("/users/:id", func(c fh.Ctx) error {
    return c.SendString("User: " + c.Params("id"))
})
app.Get("/files/*path", func(c fh.Ctx) error {
    return c.SendString("File: " + c.Params("path"))
})

// Named routes
app.Get("/users/:id", handler).Name("user.show")
c.RedirectTo("user.show", fh.Map{"id": "42"})

// Route groups
api := app.Group("/api")
api.Get("/users", listUsers)
admin := api.Group("/admin", adminMiddleware)
admin.Get("/dashboard", dashboardHandler)
```

Typed endpoints (`GetTyped`, `PostTyped`, ... `AllTyped`) provide automatic JSON parsing, validation, struct binding (`param`/`query`/`header`/`cookie` tags), and OpenAPI schema generation. See [Native Features](docs/native-features.md).

See [Routing](docs/routing.md) for the full reference.

## Middleware

```go
app.Use(logger.New(), recover.New(), cors.New(cors.Config{
    AllowOrigins: []string{"https://example.com"},
}))

// Route- or group-level
app.Get("/dashboard", authMiddleware, dashboardHandler)
admin := app.Group("/admin", authMiddleware, adminLogger)
```

Commonly used packages:

| Package | Description |
|---|---|
| `mw/basicauth` | HTTP Basic Authentication (single-user, multi-user, storage-backed) |
| `mw/apikey` | API key authentication via header or query |
| `mw/cors` | Cross-Origin Resource Sharing |
| `mw/csrf` | CSRF protection |
| `mw/ratelimiter` | Rate limiting |
| `mw/cache` | Response caching with TTL |
| `mw/compress` / `mw/decompress` | Gzip response compression / bounded request decompression |
| `mw/security` | Security headers (CSP, HSTS, XFO, etc.) |
| `mw/session` | Cookie-based sessions with HMAC signing |
| `mw/logger` | Request logging (common, combined, tiny, json, custom) |
| `mw/recover` | Panic recovery |
| `mw/requestid` / `mw/correlationid` | Request tracking and correlation |
| `mw/realip` | Trusted proxy-chain parsing, client-IP normalization |
| `mw/timeout` / `mw/bodylimit` | Request timeout and body-size limits |
| `mw/circuitbreaker` / `mw/bulkhead` / `mw/loadshed` | Overload and fault protection |
| `mw/proxy` | Reverse proxy and API gateway handlers |
| `mw/mtls` | Verified client-certificate authorization |
| `mw/httpsignature` | Nonce-bound RFC 9421 Ed25519 response signatures |
| `mw/metrics` | Prometheus-style metrics endpoint |

This is a subset — fh ships **70+ middleware packages** under `mw/`, each with
its own `README.md`. See [`docs/middleware.md`](docs/middleware.md) for the full
reference and recommended ordering, or [`mw/README.md`](mw/README.md) for the
package index.

## Body Parsing & Codecs

`BodyParser` automatically selects the right codec based on `Content-Type`:

```go
var user User
if err := c.BodyParser(&user); err != nil {
    return err
}
```

| Content-Type | Codec |
|---|---|
| `application/json` | JSON |
| `application/xml`, `text/xml` | XML |
| `application/x-www-form-urlencoded` | Form |
| `multipart/form-data` | Multipart |
| `text/csv` | CSV |
| `application/x-ndjson` | NDJSON |
| `text/plain` | Plain text |
| `application/octet-stream` | Binary |

Register custom codecs with `fh.RegisterCodec(&MyCodec{})`. See [Codecs](docs/codecs.md).

## Responses

```go
c.SendString("text")
c.SendBytes([]byte("data"))
c.SendStream(reader)
c.JSON(data)
c.XML(data)
c.HTML(html)
c.SendFile("path/to/file.pdf")
c.Redirect("/new-path")
c.RedirectTo("route.name", fh.Map{"id": "42"})
c.Status(201).JSON(createdResource)
```

See [Request & Response](docs/response.md) for the full method reference.

## Static Files

```go
import "os"

app.Static("/static", "./public")

app.StaticFS("/", os.DirFS("./public"), fh.StaticConfig{
    Compress:     true,
    Browse:       true,
    IndexFiles:   []string{"index.html", "index.htm"},
    CacheControl: "public, max-age=3600",
})
```

## HTTP/2

fh supports TLS + ALPN (`app.ListenTLS(":443", "cert.pem", "key.pem")`), h2c prior knowledge, and h2c upgrade from HTTP/1.1. Use `fh.WithDisableH2C(true)` on cleartext listeners that should accept only HTTP/1; `WithSecureByDefault(true)` applies that restriction automatically. fh also implements RFC 8441 extended CONNECT, so WebSocket (and other `c.Upgrade`-based protocols) tunnel over a single HTTP/2 stream instead of requiring an HTTP/1.1 fallback. See [HTTP/2](docs/http2.md).

## WebSocket

```go
import "github.com/oarkflow/fh/pkg/websocket"

app.Get("/ws", websocket.New(func(conn *websocket.Conn) error {
    opcode, payload, err := conn.ReadMessage()
    if err != nil {
        return err
    }
    return conn.WriteMessage(opcode, payload)
}))
```

For pub/sub with rooms, topics, auth, and heartbeats, use `pkg/websocket.EventHub`:

```go
import "github.com/oarkflow/fh/pkg/websocket"

hub := websocket.NewEventHub(websocket.EventHubConfig{
    Auth: func(client *websocket.EventConn, env websocket.Envelope) error {
        // Revalidate authorization for every non-ack event.
        return nil
    },
})
defer hub.Close()

hub.On("chat.message", func(ctx *websocket.HandlerContext) (any, error) {
    return map[string]any{"accepted": true}, nil
})

wsConfig := websocket.DefaultConfig()
wsConfig.AllowedOrigins = []string{"https://app.example.com"}
app.Get("/ws", hub.Handler(wsConfig, nil))

_ = hub.BroadcastEvent("chat", "general", "chat.message", "Hello everyone!")
```

See [WebSocket](docs/websocket.md).

## Error Handling

fh includes a production-safe error framework based on RFC 9457 Problem Details, with typed errors, validation errors, panic recovery, request ID correlation, retryability metadata, and secret redaction.

```go
return fh.NotFound("User not found")
return fh.Unauthorized("Sign in required")
return fh.NewHTTPError(fh.StatusConflict, "USER_EXISTS", "User already exists")

app := fh.NewWithConfig(fh.Config{
    ErrorHandler: func(c fh.Ctx, err error) { _ = c.ErrorResponse(err) },
    NotFoundHandler: func(c fh.Ctx) error {
        return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "missing"})
    },
})
```

See [Error Framework](docs/ERROR_FRAMEWORK.md).

## Configuration

```go
app := fh.NewWithConfig(fh.Config{
    ReadTimeout:          10 * time.Second,
    WriteTimeout:         10 * time.Second,
    IdleTimeout:          120 * time.Second,
    RequestBodyTimeout:   10 * time.Second,
    TLSHandshakeTimeout:  10 * time.Second,
    HTTP2IdleTimeout:     60 * time.Second,
    MaxConnections:       10_000,
    MaxRequestBodySize:   4 * 1024 * 1024, // 4MB
    MaxConcurrentStreams: 128,
    ErrorHandler:         customErrorHandler,
    TemplateEngine:       myEngine,
    Debug:                false,
})
```

See [Configuration](docs/configuration.md) for the full field reference, and [Startup Banner](docs/STARTUP_BANNER.md) for the ASCII banner shown on `Listen`.

## Sessions

```go
import (
    "github.com/oarkflow/fh/mw/session"
    "github.com/oarkflow/fh/pkg/storage/kv"
)

smw := session.New(session.Config{
    Store:  kv.NewMemoryStore(),
    Secret: "your-256-bit-secret",
})
app.Use(smw.Middleware)

app.Get("/login", func(c fh.Ctx) error {
    sess := session.Get(c)
    sess.Set("user_id", 42)
    return sess.Save()
})
```

## Graceful Shutdown

```go
// One-liner with SIGINT/SIGTERM handling
app.ListenWithGracefulShutdown(":8080")

// Manual
go app.Listen(":8080")
quit := make(chan os.Signal, 1)
signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
<-quit
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
app.ShutdownWithContext(ctx)

// Context-owned serving for embedded servers and process managers.
// Canceling ctx drains active connections and closes the listener.
ln, _ := net.Listen("tcp", ":8080")
serveCtx, cancel := context.WithCancel(context.Background())
defer cancel()
app.ServeContext(serveCtx, ln)
```

## Prefork & Zero-Downtime Restarts

```go
// Multi-process SO_REUSEPORT supervisor instead of a single process.
// The binary re-executes itself once per worker, so main() (including route
// registration) naturally runs again in every worker.
app.ListenPrefork(":8080")
```

Send `SIGHUP` to the master process to roll out a new binary with zero
dropped connections: it spawns a fresh generation of workers, waits for them
to report a bound listener, then gracefully drains and terminates the
previous generation. `SIGINT`/`SIGTERM` stops the whole supervisor. On
Windows (no `SIGHUP`), call `app.Reload()` instead. See
[Prefork](docs/prefork.md).

## ACME / Automatic TLS

```go
// Certificates issued and renewed automatically via TLS-ALPN-01
// (golang.org/x/crypto/acme/autocert — already a dependency, no net/http
// required). CacheDir persists them across restarts.
app.ListenAutoTLS([]string{"example.com"}, "/var/lib/fh/acme-cache")
```

See [ACME](docs/acme.md).

## Reliability Layer

An optional, stdlib-only runtime for request journaling, idempotency, and a durable async job queue — no external queue dependency required.

```go
app := fh.NewWithConfig(fh.Config{
    Reliability: fh.ReliabilityConfig{
        Enabled:            true,
        DataDir:            ".fh-data",
        JournalEnabled:     true,
        IdempotencyEnabled: true,
        QueueEnabled:       true,
        QueueWorkers:       2,
        QueueMaxAttempts:   5,
    },
})
```

```bash
curl -i -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-create-001' \
  -d '{"item":"book"}'
```

Storage is pluggable (`pkg/storage/memory`, `pkg/storage/sql` for PostgreSQL/MySQL/SQLite, or implement the interfaces yourself). See [Reliability Layer](docs/reliability.md).

## Compliance Layer

A built-in Business/Professional/Enterprise/Security compliance layer on top of the router, middleware, reliability, OpenAPI, queue, and security primitives.

```go
app := fh.NewWithConfig(fh.Config{
    Mode: fh.ModeProduction,
    Compliance: fh.ComplianceConfig{
        Enabled:         true,
        Profile:         fh.ComplianceEnterprise,
        Strict:          true,
        ExposeEndpoints: true,
    },
    Audit: fh.AuditConfig{Enabled: true, FilePath: ".fh-data/audit.jsonl", Redact: true},
})
```

Profiles: `business`, `professional`, `enterprise`, `security_strict`, `financial`, `healthcare`, `government`, `internal_service`, `public_api`, `webhook_receiver`.

Annotate routes with security metadata so tooling can prove which controls apply:

```go
app.Post("/payments", fh.RequireAuth(), fh.RequireScope("payments:create"), createPayment).
    WithRouteSecurity(fh.RouteSecurityConfig{
        AuthRequired:        true,
        Scopes:              []string{"payments:create"},
        IdempotencyRequired: true,
        AuditRequired:       true,
        DataClass:           "regulated",
    })
```

When `Compliance.ExposeEndpoints` is enabled, fh registers `/_fh/compliance`, `/_fh/compliance/controls`, `/_fh/compliance/findings`, `/_fh/config/safe`, `/_fh/runtime`, `/_fh/routes`, `/_fh/health`, `/_fh/live`, `/_fh/ready`, and (when the queue is enabled) `/_fh/queue/stats` and admin queue ops endpoints.

## Outbound HTTP Client

A production-grade HTTP/1.1 + HTTP/2 client lives directly in the root `fh` package — no separate `client/` module.

```go
client := fh.NewClient(fh.ClientConfig{})
resp, err := client.Get(ctx, "https://api.example.com/users")

user, err := fh.GetJSON[User](ctx, client, "https://api.example.com/users/1")
```

Includes fluent request building, typed helpers (`GetJSON[T]`, `PostJSON[Req,Res]`), retry policies with jitter backoff, circuit breaker, bulkhead, rate limiting, outbound SSRF protection, and streaming/atomic downloads. See [HTTP Client](docs/http-client.md).

## Secure WASM Transport

`mw/securetransport`, the shared `pkg/securetransport` protocol, and a TypeScript/JavaScript Go-WASM Fetch client under `wasm/` provide device-signed session establishment, X25519 key agreement, AES-256-GCM encrypted bodies/headers, replay prevention, and pluggable stores. The secure WASM example additionally negotiates RFC 9421/RFC 9530 Ed25519 signatures over ciphertext and verifies them before decryption.

```bash
make wasm
```

See [Secure WASM Transport](docs/secure-wasm-transport.md) and [`examples/secure_wasm`](examples/secure_wasm).

## Signed HTTP Responses

`mw/httpsignature` and `pkg/httpsignature` implement a strict RFC 9421 Ed25519 response-signature profile. A fresh client nonce and the originating method and target URI bind each signed response to its request; RFC 9530 `Content-Digest` binds the exact response bytes.

See [RFC 9421 Response Signatures](docs/rfc9421-response-signatures.md) and the runnable Go and WebCrypto clients in [`examples/rfc9421`](examples/rfc9421).

## Examples

Full working examples in [`examples/`](examples/):

| Example | Description |
|---|---|
| `basic` | Minimal "Hello World" |
| `flash-messages` | Session-backed one-time flash messages and redirects |
| `http-modern` | Modern HTTP helpers and protocol behavior |
| `kernel_server` | Kernel-assisted server configuration and readiness |
| `prefork` | Multi-process prefork serving |
| `secure_wasm` | Session + secure WASM client demo for encrypted API calls |
| `rfc9421` | RFC 9421 signed-response server plus fail-closed Go and WebCrypto clients |

## Testing & Benchmarks

```bash
go test ./...
go test -bench=. -benchmem ./...
```

See [`benchmarks/`](benchmarks/README.md) for cross-framework comparisons against Fiber and fasthttp, and [Performance](docs/performance.md) for hot-path configuration.

## Documentation

Full reference documentation is in [`docs/README.md`](docs/README.md), covering configuration, routing, codecs, middleware, the reliability layer, HTTP/2, WebSocket, native features (typed endpoints, OpenAPI, SSE), security, and performance.

## Known Limitations

fh is a production-oriented, pre-v1 framework. The core HTTP/1.1, HTTP/2 and
WebSocket paths have extensive unit, integration, race and fuzz coverage, but a
specific deployment is production-ready only after the release gates in
[`docs/production-readiness.md`](docs/production-readiness.md) are satisfied.
The following product limitations must also be planned around:

- **No HTTP/3 / QUIC.** Only HTTP/1.1 and HTTP/2 are implemented. Terminate HTTP/3 at an edge proxy (e.g. a CDN) in front of fh if you need it.
- **No OpenTelemetry (OTLP) export.** `mw/tracing` propagates/parses `traceparent` headers and `mw/metrics` exposes a hand-rolled Prometheus text endpoint, but neither ships an OTLP exporter to a collector (Grafana Tempo/Datadog/etc.). Bridge these yourself, or scrape the Prometheus endpoint and configure trace propagation compatible with your existing collector.
- **Process-local defaults.** Middleware and cluster state use the shared `pkg/storage/kv.Store` interface. Defaults are in-process, so rate limits, caches, sessions, replay markers, and cluster leases are per instance unless you provide a shared `kv.Store` backend (Redis, SQL, Consul, etc.).
- **No gRPC or GraphQL protocol handlers.** Only MIME-type constants exist for GraphQL; there's no built-in gRPC server. Both are addressable via a reverse-proxy route (`mw/proxy`) to a dedicated service if needed.
