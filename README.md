# fh — Zero-Dependency Go Web Framework

**fh** is a standalone, high-performance HTTP/1.1 + HTTP/2 + WebSocket web framework for Go with **zero external dependencies**. It implements HTTP parsing, routing, HTTP/2 framing, HPACK, and WebSocket protocols from scratch — no wrappers around `net/http` or `fasthttp`.

Full reference documentation lives in [`docs/`](docs/README.md).

## Features

- **Zero dependencies** — only the Go standard library
- **HTTP/1.1** — full request/response parsing, chunked transfer, trailers
- **HTTP/2** — TLS ALPN, h2c prior knowledge, h2c upgrade, stream multiplexing, flow control
- **WebSocket** — RFC 6455 server implementation with `EventHub` pub/sub layer
- **Trie-based router** — radix tree with named (`:param`) and wildcard (`*wild`) parameters
- **Codec system** — pluggable body parsers for JSON, XML, form, multipart, CSV, NDJSON, text, binary
- **65+ built-in middleware packages** — see [Middleware](#middleware) below
- **Typed endpoints & OpenAPI 3.1** — generic request/response handlers with auto-generated specs
- **Reliability layer** — request journaling, idempotency, durable async queue, outbox/inbox, DLQ
- **Compliance layer** — Business/Professional/Enterprise/Security profiles, audit ledger, route security metadata
- **Template engine** — agnostic interface, any engine implementing `Render(w, name, data, layout...)`
- **Static file serving** — directory listings, compression, cache control, range requests
- **Graceful shutdown** — `app.ShutdownWithContext(ctx)` or `app.ListenWithGracefulShutdown(addr)`
- **Pool-based zero-allocation** — `sync.Pool` for contexts, byte buffers, HPACK decoders
- **Hardened TLS/mTLS** — TLS 1.3 config builder, verified peer state in request contexts, atomic certificate reload
- **Outbound HTTP client** — connection pooling, retries, circuit breaker, SSRF protection (`fh.NewClient`)

## Installation

```bash
go get github.com/oarkflow/fh
```

Requires Go 1.23.2 or later.

## Quick Start

```go
package main

import "github.com/oarkflow/fh"

func main() {
    app := fh.New()

    app.Get("/", func(c *fh.Ctx) error {
        return c.SendString("Hello, World!")
    })

    app.Listen(":8080")
}
```

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
app.Get("/users/:id", func(c *fh.Ctx) error {
    return c.SendString("User: " + c.Params("id"))
})
app.Get("/files/*path", func(c *fh.Ctx) error {
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
| `mw/jwt` | Dependency-free JWT verification, principal population |
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
| `mw/metrics` | Prometheus-style metrics endpoint |

This is a subset — fh ships **65+ middleware packages** under `mw/`, each with its own `README.md`. See [`docs/middleware.md`](docs/middleware.md) for the full reference and recommended ordering, or [`mw/README.md`](mw/README.md) for the package index.

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
app.Static("/static", "./public")

app.StaticFS("/", fh.StaticConfig{
    Root:         "./public",
    Compress:     true,
    Browse:       true,
    IndexFiles:   []string{"index.html", "index.htm"},
    CacheControl: "public, max-age=3600",
})
```

## HTTP/2

fh supports TLS + ALPN (`app.ListenTLS(":443", "cert.pem", "key.pem")`), h2c prior knowledge (automatic on `app.Listen`), and h2c upgrade from HTTP/1.1 — all handled transparently. See [HTTP/2](docs/http2.md).

## WebSocket

```go
app.Get("/ws", func(c *fh.Ctx) error {
    conn, err := c.Upgrade()
    if err != nil {
        return err
    }
    msg, err := conn.ReadMessage()
    return conn.WriteMessage(msg)
})
```

For pub/sub with rooms, topics, auth, and heartbeats, use `pkg/websocket.EventHub`:

```go
import "github.com/oarkflow/fh/pkg/websocket"

hub := websocket.NewEventHub()

app.Get("/ws", func(c *fh.Ctx) error {
    conn, err := c.Upgrade()
    if err != nil {
        return err
    }
    hub.Serve(conn)
})

hub.Publish("chat:general", "Hello everyone!")
hub.OnConnect(func(ctx *websocket.Context) {
    ctx.Join("room:golang")
})
```

See [WebSocket](docs/websocket.md).

## Error Handling

fh includes a production-safe error framework based on RFC 9457 Problem Details, with typed errors, validation errors, panic recovery, request ID correlation, retryability metadata, and secret redaction.

```go
return fh.NotFound("User not found")
return fh.Unauthorized("Sign in required")
return fh.NewHTTPError(fh.StatusConflict, "USER_EXISTS", "User already exists")

app := fh.New(fh.Config{
    ErrorHandler: func(c *fh.Ctx, err error) { _ = c.ErrorResponse(err) },
    NotFoundHandler: func(c *fh.Ctx) error {
        return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "missing"})
    },
})
```

See [Error Framework](docs/ERROR_FRAMEWORK.md).

## Configuration

```go
app := fh.New(fh.Config{
    ReadTimeout:          10 * time.Second,
    WriteTimeout:         10 * time.Second,
    IdleTimeout:          120 * time.Second,
    MaxConnections:       256 * 1000,
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
import "github.com/oarkflow/fh/mw/session"

smw := session.New(session.Config{
    Store:  session.NewMemoryStore(),
    Secret: "your-256-bit-secret",
})
app.Use(smw.Middleware)

app.Get("/login", func(c *fh.Ctx) error {
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
```

## Reliability Layer

An optional, stdlib-only runtime for request journaling, idempotency, and a durable async job queue — no external queue dependency required.

```go
app := fh.New(fh.Config{
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

`mw/securetransport`, the shared `pkg/securetransport` protocol, and a TypeScript/JavaScript Go-WASM Fetch client under `wasm/` provide device-signed session establishment, X25519 key agreement, AES-256-GCM encrypted bodies/headers, replay prevention, and pluggable stores.

```bash
make wasm
```

See [Secure WASM Transport](docs/secure-wasm-transport.md) and [`examples/secure_wasm`](examples/secure_wasm).

## Examples

Full working examples in [`examples/`](examples/):

| Example | Description |
|---|---|
| `basic` | Minimal "Hello World" |
| `header_parser` | Binding request headers into a struct via `header` tags |
| `query` | `app.Query`/`app.QueryTyped` handlers |
| `validation` | Typed endpoint request validation |
| `http_client` | Outbound `fh.NewClient` usage |
| `budget` | Hierarchical per-request execution budgets |
| `configreload` | Atomic config/route/cert reload |
| `merkle_audit` | Tamper-evident audit logging with Merkle checkpoints |
| `privacy` | Privacy-aware telemetry filtering (logs/traces/metrics/audit) |
| `requestdedup` | Request deduplication |
| `scheduler` | Priority-based request scheduling with concurrency pools |
| `slo` | Route-level SLO monitoring with burn-rate alerts |
| `secure_wasm` | Session + secure WASM client demo for encrypted API calls |
| `production` | Combined production middleware stack |
| `workflow` | Checkout workflow: sequential/parallel/branch steps, retry, timeout, compensation, async job handoff |
| `webhook-receiver` | Secure, idempotent webhook ingestion: signature verification, replay protection, business-level dedup |
| `api-gateway` | Public API edge: security headers, CORS, rate limiting, API key auth, circuit breaker, reverse proxy, metrics |
| `multi-tenant-api` | Multi-tenant SaaS API: JWT auth, tenant resolution, per-tenant concurrency isolation, audit trail |
| `resilient-upstream` | Fault-isolated upstream calls: load shedding, bulkhead, timeout, circuit breaker, reverse proxy |

## Testing & Benchmarks

```bash
go test ./...
go test -bench=. -benchmem ./...
```

See [`benchmarks/`](benchmarks/README.md) for cross-framework comparisons against Fiber and fasthttp, and [Performance](docs/performance.md) for hot-path configuration.

## Documentation

Full reference documentation is in [`docs/README.md`](docs/README.md), covering configuration, routing, codecs, middleware, the reliability layer, HTTP/2, WebSocket, native features (typed endpoints, OpenAPI, SSE), security, and performance.
