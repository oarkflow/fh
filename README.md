# fh — Zero-Dependency Go Web Framework

**fh** is a standalone, high-performance HTTP/1.1 + HTTP/2 + WebSocket web framework for Go with **zero external dependencies**. It implements HTTP parsing, routing, HTTP/2 framing, HPACK, and WebSocket protocols from scratch — no wrappers around `net/http` or `fasthttp`.

## Features

- **Zero dependencies** — only the Go standard library
- **HTTP/1.1** — full request/response parsing, chunked transfer, trailers
- **HTTP/2** — TLS ALPN, h2c prior knowledge, h2c upgrade, stream multiplexing, flow control
- **WebSocket** — RFC 6455 server implementation with `EventHub` pub/sub layer
- **Trie-based router** — radix tree with named (`:param`) and wildcard (`*wild`) parameters
- **Codec system** — pluggable body parsers for JSON, XML, form, multipart, CSV, NDJSON, text, binary
- **Middleware** — built-in: CORS, CSRF, rate limiter, session, cache, compress, logger, recover, security headers, basic auth, body limit, rewrite, timeout, IP whitelist, request ID, early data, skip
- **Template engine** — agnostic interface, any engine implementing `Render(w, name, data, layout...)`
- **Static file serving** — directory listings, compression, cache control, range requests
- **Graceful shutdown** — `app.Shutdown(ctx)` with context deadline
- **Pool-based zero-allocation** — sync.Pool for contexts, byte buffers, HPACK decoders
- **Hardened TLS/mTLS** — TLS 1.3 config builder, verified peer state in request contexts, atomic certificate reload
- **Modern HTTP integrity/query** — RFC 9530 Content-Digest and RFC 10008 Accept-Query middleware
- **Trusted proxy identity** — RFC 7239 `Forwarded` plus right-to-left trusted-hop validation

## Installation

```bash
go get github.com/oarkflow/fh
```

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

### HTTP Methods

```go
app.Get("/path", handler)
app.Post("/path", handler)
app.Put("/path", handler)
app.Delete("/path", handler)
app.Patch("/path", handler)
app.Head("/path", handler)
app.Options("/path", handler)

// Register all methods at once
app.All("/path", handler)

// Register with explicit method string
app.Add("GET", "/path", handler)
```

### Route Parameters

```go
app.Get("/users/:id", func(c *fh.Ctx) error {
    id := c.Params("id")
    return c.SendString("User: " + id)
})

app.Get("/files/*path", func(c *fh.Ctx) error {
    path := c.Params("path")
    return c.SendString("File: " + path)
})
```

### Named Routes

```go
app.Get("/users/:id", handler).Name("user.show")
// Redirect by route name
c.RedirectTo("user.show", fh.Map{"id": "42"})
```

### Route Groups

```go
api := app.Group("/api")
api.Get("/users", listUsers)
api.Post("/users", createUser)
api.Get("/users/:id", getUser)

// Nested groups with middleware
admin := api.Group("/admin", adminMiddleware)
admin.Get("/dashboard", dashboardHandler)
```

## Middleware

### Global Middleware

```go
app.Use(logger.New())
app.Use(recover.New())
app.Use(cors.New(cors.Config{
    AllowOrigins: []string{"https://example.com"},
}))
```

### Route-level Middleware

```go
app.Get("/dashboard", authMiddleware, dashboardHandler)

// Group middleware
admin := app.Group("/admin", authMiddleware, adminLogger)
admin.Get("/", adminHandler)
```

### Built-in Middleware

| Package | Description |
|---|---|
| `mw/basicauth` | HTTP Basic Authentication with single-user, multi-user, storage-backed and dynamic provider modes |
| `mw/bodylimit` | Request body size limit |
| `mw/acceptquery` | RFC 10008 Accept-Query advertisement and QUERY content-type enforcement |
| `mw/cache` | Response caching with TTL |
| `mw/compress` | Gzip response compression |
| `mw/contentdigest` | RFC 9530 request integrity verification and response digests |
| `mw/cors` | Cross-Origin Resource Sharing |
| `mw/csrf` | CSRF protection |
| `mw/earlydata` | TLS 1.3 Early Data (0-RTT) protection |
| `mw/decompress` | Bounded gzip request decompression with expansion-ratio protection |
| `mw/ipwhitelist` | IP allowlisting |
| `mw/logger` | Request logging (common, combined, tiny, json, custom) |
| `mw/ratelimiter` | Rate limiting |
| `mw/recover` | Panic recovery |
| `mw/requestid` | Request ID injection |
| `mw/realip` | Trusted proxy-chain parsing and effective client-IP normalization |
| `mw/rewrite` | URL path rewriting |
| `mw/security` | Security headers (CSP, HSTS, XFO, etc.) |
| `mw/session` | Cookie-based sessions with HMAC signing |
| `mw/skip` | Conditional middleware skipping by path |
| `mw/timeout` | Request timeout |
| `mw/mtls` | Verified client-certificate authorization |

## Body Parsing & Codecs

fh uses an interface-based codec system. `BodyParser` automatically selects the right codec based on `Content-Type`:

```go
var user User
if err := c.BodyParser(&user); err != nil {
    return err
}
```

### Built-in Codecs

| Content-Type | Codec |
|---|---|
| `application/json` | JSON |
| `application/xml`, `text/xml` | XML |
| `application/x-www-form-urlencoded` | Form |
| `multipart/form-data` | Multipart |
| `text/csv` | CSV |
| `application/x-ndjson` | NDJSON (newline-delimited JSON) |
| `text/plain` | Plain text |
| `application/octet-stream` | Binary |

### Custom Codecs

```go
type MsgPackCodec struct{}

func (c *MsgPackCodec) ContentTypes() []string {
    return []string{"application/msgpack"}
}

func (c *MsgPackCodec) Decode(data []byte, v interface{}) error {
    // decode msgpack into v
}

fh.RegisterCodec(&MsgPackCodec{})
```

## Responses

```go
c.SendString("text")
c.SendBytes([]byte("data"))
c.SendStream(reader)
c.JSON(data)
c.JSONPretty(data, "  ")
c.XML(data)
c.XMLPretty(data, "  ")
c.HTML(html)
c.SendFile("path/to/file.pdf")
c.Redirect("/new-path")
c.RedirectTo("route.name", fh.Map{"id": "42"})
c.RedirectBack()

// Set status code
c.Status(201).JSON(createdResource)
```

## Static File Serving

```go
// Serve a single directory
app.Static("/static", "./public")

// With configuration
app.StaticFS("/", fh.StaticConfig{
    Root:         "./public",
    Compress:     true,
    Browse:       true,
    IndexFiles:   []string{"index.html", "index.htm"},
    CacheControl: "public, max-age=3600",
})
```

## HTTP/2

fh supports three HTTP/2 modes:

### TLS + ALPN (standard)

```go
app.ListenTLS(":443", "cert.pem", "key.pem")
```

### h2c Prior Knowledge (cleartext)

```go
app.Listen(":8080") // implicit if client sends PRI * HTTP/2.0
```

### h2c Upgrade (from HTTP/1.1)

```go
// Client sends Upgrade: h2c, server responds with 101 Switching Protocols
// fh handles this automatically
```

## WebSocket

### Low-level Upgrade

```go
app.Get("/ws", func(c *fh.Ctx) error {
    conn, err := c.Upgrade()
    if err != nil {
        return err
    }
    // conn is a *websocket.Conn
    msg, err := conn.ReadMessage()
    conn.WriteMessage(msg)
})
```

### EventHub (pub/sub)

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

// Broadcast to a topic
hub.Publish("chat:general", "Hello everyone!")

// With auth and rooms
hub.SetAuth(func(ctx *websocket.Context) error {
    if ctx.Token != "valid-token" {
        return errors.New("unauthorized")
    }
    return nil
})

hub.OnConnect(func(ctx *websocket.Context) {
    ctx.Join("room:golang")
})
```

## Template Rendering

Any engine implementing the `TemplateEngine` interface:

```go
type TemplateEngine interface {
    Render(w io.Writer, name string, data interface{}, layout ...string) error
}
```

```go
app.SetTemplateEngine(myEngine)

app.Get("/page", func(c *fh.Ctx) error {
    return c.Render("index", fh.Map{
        "title": "Hello",
        "body":  "World",
    })
})

// With layout
c.Render("content", data, "layouts/main")
```

## Error Handling

```go
// Return typed HTTP errors
return fh.NotFound("User not found")
return fh.Unauthorized("Sign in required")
return fh.NewHTTPError(fh.StatusConflict, "USER_EXISTS", "User already exists")

// Custom server fallbacks and error responses
app := fh.New(fh.Config{
    ErrorHandler: func(c *fh.Ctx, err error) {
        _ = c.ErrorResponse(err)
    },
    NotFoundHandler: func(c *fh.Ctx) error {
        return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "missing"})
    },
    MethodNotAllowed: func(c *fh.Ctx, allowed []string) error {
        return c.Status(fh.StatusMethodNotAllowed).JSON(fh.Map{"allowed": allowed})
    },
    OptionsHandler: func(c *fh.Ctx, allowed []string) error {
        return c.SendStatus(fh.StatusNoContent)
    },
})
```


## Pretty startup banner

`fh` now prints a Fiber-style ASCII startup banner when the server starts. It is configurable, custom-renderable and can be disabled for strict JSON logging environments.

```go
app := fh.New(fh.WithStartupBanner(fh.StartupBannerConfig{
    Name:     "billing-api",
    Version:  "v2.4.1",
    Subtitle: "Orgware billing service",
    Color:    true,
}))
```

Disable it:

```go
app := fh.New(fh.WithStartupBannerDisabled(true))
```

See [`docs/STARTUP_BANNER.md`](docs/STARTUP_BANNER.md) and [`examples/startup_banner`](examples/startup_banner).

## Configuration

```go
app := fh.New(fh.Config{
    ReadTimeout:          10 * time.Second,
    WriteTimeout:         10 * time.Second,
    IdleTimeout:          120 * time.Second,
    ReadBufferSize:       4096,
    MaxConnections:       256 * 1000,
    MaxRequestBodySize:   4 * 1024 * 1024, // 4MB
    MaxHeaderListSize:    64 << 10,
    MaxConcurrentStreams: 128,
    DisableKeepAlive:     false,
    DisableHTTP2:         false,
    ErrorHandler:         customErrorHandler,
    NotFoundHandler:      customNotFoundHandler,
    MethodNotAllowed:     customMethodNotAllowedHandler,
    OptionsHandler:       customOptionsHandler,
    TemplateEngine:       myEngine,
    Logger:               logger,
    Debug:                false,
})
```

## HTTP/1.1 Features

- Request/response header parsing (zero-alloc byte comparisons)
- Chunked transfer encoding (RFC 9112)
- Trailers
- Keep-alive connection reuse
- `Expect: 100-continue`
- `Host` header validation
- `Content-Length` enforcement

## Sessions

```go
import "github.com/oarkflow/fh/mw/session"

store := session.NewMemoryStore()
smw := session.New(session.Config{
    Store: store,
    Secret: "your-256-bit-secret",
})

app.Use(smw.Middleware)

app.Get("/login", func(c *fh.Ctx) error {
    sess := session.Get(c)
    sess.Set("user_id", 42)
    sess.Set("role", "admin")
    return sess.Save()
})

app.Get("/profile", func(c *fh.Ctx) error {
    sess := session.Get(c)
    uid := sess.Get("user_id")
    return c.JSON(fh.Map{"user_id": uid})
})
```

Session stores: `MemoryStore` (default), `FileStore`. Custom stores implement `session.Store`.

## Graceful Shutdown

```go
// Option 1: one-liner with signal handling (SIGINT/SIGTERM)
app.ListenWithGracefulShutdown(":8080")

// Option 2: manual signal handling
go app.Listen(":8080")

quit := make(chan os.Signal, 1)
signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
<-quit

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
app.ShutdownWithContext(ctx)
```

---

# Technical Architecture

## Core Types

```
App — application container, route registry, lifecycle
Ctx — per-request context (request/response, params, locals)
Router — radix tree with parameter extraction
Codec — pluggable body codec interface
RequestHeader — zero-alloc HTTP/1.1 header parser
```

## Router Design

The router is a **compressed radix tree** (trie) where each node represents a path segment. Parameter nodes are prefixed with `:` and wildcard nodes with `*`.

- **Insert**: `O(n)` where n is path segment count. Paths are split on `/` and merged into existing nodes when possible.
- **Lookup**: `O(n)` with backtracking for parameter extraction. No reflection or regex.
- **Named routes**: Routes can be named via `.Name("name")` and looked up at runtime for redirect generation.

## Connection Lifecycle

1. `Accept()` — accept TCP connection
2. `serveConn()` — goroutine per connection
3. `readRequest()` — parse HTTP/1.1 or detect HTTP/2
4. Route dispatch — find handler via trie
5. Middleware chain — sequential handler execution
6. Response write — headers + body
7. Recycle — release `Ctx` to pool, keep-alive or close

## Codec System

`Codec` interface:

```go
type Codec interface {
    ContentTypes() []string
    Decode(data []byte, v interface{}) error
}
```

Optional interfaces: `ContentTypeAwareCodec` (response encoding), `EncoderCodec` (separate encode/decode), `ResettableCodec` (pool recycling).

`BodyParser` iterates registered codecs, matches by `Content-Type`, and delegates decoding.

## HTTP/2 Implementation

Self-contained in `http2.go` with `pkg/hpack`:

- **Framing**: DATA, HEADERS, PRIORITY, RST_STREAM, SETTINGS, PUSH_PROMISE, PING, GOAWAY, WINDOW_UPDATE, CONTINUATION
- **HPACK**: Custom implementation (~36% lower-latency static-table decode vs `golang.org/x/net/http2/hpack`)
- **Stream multiplexing**: `sync.Map`-backed stream table with flow control
- **H2C**: Prior knowledge (direct `PRI * HTTP/2.0`) and upgrade (`Upgrade: h2c`)
- **TLS ALPN**: Automatic via `ListenTLS` with `h2` protocol negotiation

## WebSocket Implementation

- **Low-level**: `pkg/websocket.Conn` — read/write with masking, control frames (ping/pong/close), rate limiting
- **High-level**: `pkg/websocket.EventHub` — pub/sub with auth, authorize, rooms, topics, channels, acknowledgements, heartbeat, reconnect

## Testing

```bash
go test ./...
go test -bench=. ./...
go test -v -run TestRouter ./...
```

### Test Structure

| File | Scope |
|---|---|
| `fasthttp_test.go` | Integration tests with real `net.Conn` |
| `codec_test.go` | Form/codec unit tests |
| `fs_test.go` | Static file serving tests |
| `protocol_test.go` | HTTP/1.1 + HTTP/2 protocol (chunked, trailers, h2c, TLS) |
| `hardening_test.go` | Security/fuzzing/hardening |
| `router_features_test.go` | Router (params, wildcards, named routes) |
| `middleware_features_test.go` | Middleware integration |
| `template_test.go` | Template engine |

## Performance Considerations

- **Pool-based Ctx allocation** avoids per-request heap allocation
- **Byte buffer pools** at 4 sizes (512B, 4K, 16K, 64K) match common I/O sizes
- **Zero-alloc header parsing** uses `[]byte` comparisons from the raw buffer
- **Unsafe string/byte conversions** (`b2s`, `s2b`) avoid copying where safe
- **HPACK decoder pool** reuses decoder state across HTTP/2 streams

---

# Examples

Full working examples in `examples/`:

| Example | Description |
|---|---|
| `basic` | Minimal "Hello World" |
| `codecs` | All built-in codecs + custom codec + QueryParser |
| `files` | File upload/download with range/conditional requests |
| `http2` | TLS ALPN, h2c prior knowledge, h2c upgrade |
| `middleware` | Body limits, rewrite, skip, CSRF, cache, Early-Data, CORS |
| `redirect` | 301/302 redirects, named route redirect, redirect back |
| `sink` | Comprehensive: sessions, all methods, groups, codecs, streaming, WebSocket |
| `websocket` | Event-driven WebSocket with EventHub (auth, topics, channels, heartbeat) |
| `fiber` | Comparative example using `gofiber/fiber/v3` |

## Built-in reliability layer

`fh` includes an optional stdlib-only reliability runtime for applications that must safely handle request-response workflows and durable asynchronous work without adding an external queue dependency.

Enable it from application configuration:

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

What it provides:

- `X-Request-ID` generation and propagation for every request.
- Durable request-cycle journal in JSONL format: `received` and `completed` events with method, path, status, body hash, remote IP, and timestamp.
- Idempotency support for unsafe methods (`POST`, `PUT`, `PATCH`, `DELETE`) using `Idempotency-Key`.
- Safe replay of completed idempotent responses.
- Conflict protection when the same idempotency key is reused with a different payload.
- Embedded durable queue with pending/processing/done/failed directories, crash recovery, retries, backoff, worker registration, and an append-only `queue/events.jsonl` audit trail.
- Clean shutdown through the existing application lifecycle.

Example retry-safe request:

```bash
curl -i -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-create-001' \
  -d '{"item":"book"}'
```

Running the reliability example:

```bash
cd examples/reliability
go run . -addr :3000 -reliable=true -data .fh-data -queue-workers 2
```

Queue example:

```bash
curl -i -X POST http://localhost:3000/email \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: email-001' \
  -d '{"to":"user@example.com","subject":"Hello","message":"Queued safely"}'

curl http://localhost:3000/queue/stats

# Inspect durable queue state and activity. With active workers, pending can be
# empty because jobs may move to done almost immediately.
find .fh-data/queue -maxdepth 2 -type f -print
cat .fh-data/queue/events.jsonl
```

Reliability model:

- The server never reports durable acceptance for async work until the job is written to disk.
- Queue jobs are at-least-once, so external side effects should use the queue job ID as their own idempotency key.
- If a process crashes with jobs in `processing`, they are moved back to `pending` on restart.
- `queue/events.jsonl` records enqueue, claim, retry, completion, failure, and recovery events so queue activity remains visible even when workers process jobs immediately.
- If a client times out after the server completed a request, retrying with the same `Idempotency-Key` returns the stored response instead of duplicating the operation.

## Advanced server/runtime capabilities

This version adds an extension layer for modern HTTP server and application-runtime features while preserving the existing route method API. All route behavior changes can be applied through middleware/handlers.

Added capabilities include:

- `reliability.New(policy)` per-route reliability middleware from `mw/reliability`.
- `reliability.Endpoint[Req, Res]` typed reliable endpoint abstraction under `mw/reliability`.
- Built-in metrics under `mw/metrics`.
- Reverse proxy and API gateway handlers under `mw/proxy`.
- Circuit breaker middleware under `mw/circuitbreaker`.
- Native SSE using `ctx.SSE`.
- Advanced static file handler through `static.New` under `mw/static`.
- First-class access logs under `mw/logger`.
- Transactional reliability API through `Reliability.BeginTx`.
- Outbox and inbox helpers for reliable event publishing and webhook dedupe.
- Dead-letter queue retry/discard helpers for file-backed queue storage.
- Queue priority, delayed jobs, and concurrency keys.
- Signed request middleware using HMAC-SHA256.
- Secret redaction and security event stream.
- Request lifecycle state machine and compensation hooks.
- Data sensitivity policies and secure data envelopes.
- Deterministic idempotency helper.
- Request-to-job atomic handoff helper.
- Policy-based route behavior and API contract firewall.
- Actor-per-key middleware for stateful serialized route handling.
- Simple HTTP-native workflow engine.
- Queue/journal repair and compaction hooks.
- API evolution/version compatibility middleware.

See `examples/advanced-platform` for a runnable end-to-end example.

Middleware is organized exclusively under `mw/*`. Root-package response handlers
and reliability runtime adapters remain in the core because they require private
server state; reusable request-chain middleware belongs in its matching `mw`
package.

## Error framework

FH includes a production-safe error framework based on RFC 9457 Problem Details. It supports typed errors, validation errors, panic recovery, request ID correlation, retryability metadata, severity/kind classification, secret redaction, and environment-aware debug output.

See [`docs/ERROR_FRAMEWORK.md`](docs/ERROR_FRAMEWORK.md) and [`examples/error-framework/main.go`](examples/error-framework/main.go).

---

## Compliance-first runtime additions

`fh` now includes a built-in Business/Professional/Enterprise/Security compliance layer on top of the existing router, middleware, reliability, OpenAPI, queue and security primitives.

### Compliance profiles

```go
app := fh.NewWithConfig(fh.Config{
    Mode: fh.ModeProduction,
    Compliance: fh.ComplianceConfig{
        Enabled: true,
        Profile: fh.ComplianceEnterprise,
        Strict: true,
        ExposeEndpoints: true,
    },
    Reliability: fh.ReliabilityConfig{
        Enabled: true,
        DataDir: ".fh-data",
        JournalEnabled: true,
        IdempotencyEnabled: true,
        QueueEnabled: true,
    },
    Audit: fh.AuditConfig{Enabled: true, FilePath: ".fh-data/audit.jsonl", Redact: true},
    Redaction: fh.DefaultRedactionConfig(),
})
```

Profiles include:

```text
business
professional
enterprise
security_strict
financial
healthcare
government
internal_service
public_api
webhook_receiver
```

Compliance mode enables safe defaults for request limits, timeouts, reliability, idempotency, request journaling, audit records, redaction and evidence endpoints.

### Evidence endpoints

When `Compliance.ExposeEndpoints` is enabled, these routes are registered:

```text
GET /_fh/compliance
GET /_fh/compliance/controls
GET /_fh/compliance/findings
GET /_fh/config/safe
GET /_fh/runtime
GET /_fh/routes
GET /_fh/health
GET /_fh/live
GET /_fh/ready
GET /_fh/queue/stats      # when queue is enabled
```

### Route security metadata

Annotate routes so security/compliance tooling can prove which controls apply.

```go
app.Post("/payments",
    fh.RequireAuth(),
    fh.RequireScope("payments:create"),
    createPayment,
).WithRouteSecurity(fh.RouteSecurityConfig{
    AuthRequired: true,
    Scopes: []string{"payments:create"},
    IdempotencyRequired: true,
    AuditRequired: true,
    DataClass: "regulated",
})
```

The route inventory and OpenAPI export include security metadata such as scopes, idempotency requirements and data classification.

### Principal, tenant and authorization primitives

```go
principal := fh.PrincipalExtractor(fh.PrincipalExtractors{
    ID:       fh.HeaderString("X-Subject-ID"),
    TenantID: fh.HeaderString("X-Tenant-ID"),
    Roles:    fh.HeaderCSV("X-Roles"),
    Scopes:   fh.HeaderCSV("X-Scopes"),
})

app.Use(fh.UsePrincipal(principal, true))
app.Use(fh.TenantResolverWith(fh.TenantExtractor(
    fh.PrincipalTenantExtractor(),
    fh.HeaderString("X-Tenant-ID"),
), true))
app.Post("/orders", fh.RequireAuth(), fh.RequireScope("orders:create"), handler)
```

Extractors are ordinary functions over `*fh.Ctx`, so identity and policy inputs
can come from headers, query strings, route params, JSON body fields, locals,
sessions, JWT claims, or application stores.

For policy authorization, use the optional `contrib/mw/authz` middleware. It is
backed by `github.com/oarkflow/authz` and accepts extractors for subject,
action, resource and environment:

```go
engine, _ := authzmw.LoadEngineFromAuthzFile("config.authz")

app.Use(authzmw.FHWithConfig(authzmw.FHConfig{
    Engine:      engine,
    Subject:     authzmw.SubjectFromPrincipal(),
    Action:      authzmw.StaticAction("read"),
    Resource:    authzmw.ResourceFromRoute("order", "id"),
    Environment: authzmw.EnvironmentFromRequest(),
}))
```

### Audit and operation ledger

Audit records are append-only JSONL by default:

```go
_ = c.Audit().Record("user.disabled", "user", c.Param("id"))
_ = c.Ledger("order.created", "order", orderID, before, after)
```

Default file:

```text
.fh-reliability/audit.jsonl
```

Audit records include request ID, tenant ID, actor ID, action, resource, result, route, IP and data classification. Sensitive metadata is redacted when `Audit.Redact` or `Redaction.Enabled` is set.

### New middleware packages

```text
mw/auth        attach an fh.Principal from an application resolver
mw/tenant      resolve and require tenant context
mw/audit       record request/business audit events
mw/compliance  enforce route security metadata and attach data policy
```

### Enterprise example

See:

```text
examples/compliance-enterprise
```

It demonstrates production compliance defaults, idempotent order creation, durable queued email, request journal, audit ledger, principal/tenant auth, scope checks, compliance evidence endpoints and admin status.


## Latest middleware fixes

- `mw/maintenance` now uses `Renderer fh.Handler` for custom maintenance pages. When `Path` and `Renderer` are configured, normal requests redirect to the maintenance page while the maintenance path renders through the handler.
- `mw/basicauth` now supports multiple users through `NewFromUsers`, `NewFromPlainUsers`, `UsersProvider`, and `PlainUsersProvider`, while preserving the existing `New(username, password)` API.

## Performance

See [docs/performance.md](docs/performance.md) for hot-path configuration, benchmark mode, and zero-allocation response APIs.

## Production reliability and resilience additions

This version includes additional non-duplicative production packages:

- `storage/memory`: process-local journal, idempotency and queue adapters for tests, benchmarks and embedded deployments.
- `storage/sql`: generic `database/sql` reliability adapters with schema generation/migration support for PostgreSQL, MySQL and SQLite-style deployments.
- `mw/bulkhead`: concurrent request isolation.
- `mw/backpressure`: queue-depth-based ingress protection.
- `mw/loadshed`: in-flight/goroutine/heap based overload shedding.
- `mw/realip`: trusted proxy real IP extraction.
- `mw/hostguard`: host allow/deny protection.
- `mw/maintenance`: runtime maintenance switch with optional bypass, JSON rejection, HTML rendering and redirect-to-maintenance-page support.
- `mw/etag`: ETag and conditional request support.
- `mw/tracing`: W3C traceparent propagation without third-party dependencies.
- `mw/admin`: protected operational endpoints for runtime, routes and queue statistics.

See `docs/PRODUCTION_FEATURES.md` and `examples/production` for usage.

## Additional production modules

The continuation package adds more non-overlapping production features:

- `mw/jwt` for dependency-free HMAC JWT verification and `fh.Principal` population.
- Authorization intentionally stays in the existing core helpers (`fh.RequireRole`, `fh.RequirePermission`, `fh.RequireScope`) and `contrib/mw/authz`; duplicate `mw/rbac`, `mw/abac`, and `mw/permissions` packages were removed.
- `mw/webhook` for HMAC signed webhook verification with timestamp tolerance.
- `mw/adaptiveconcurrency`, `mw/retrybudget`, and `mw/tenantlimit` for overload and retry-storm protection.
- `mw/conditional` for ETag / Last-Modified conditional requests.
- `mw/pprof` for guarded pprof endpoints.
- Expanded `mw/metrics` with Prometheus text output and request duration histograms.

See `docs/PRODUCTION_REMAINING_FEATURES.md` and `examples/production/main.go`.

### JWT generation and authz integration

Generate a development/service token with the built-in signer:

```go
token, err := jwt.Sign(map[string]any{
    "sub":         "u1",
    "tenant_id":   "t1",
    "roles":       []string{"admin"},
    "permissions": []string{"users:read"},
    "scope":       "profile email",
    "exp":         time.Now().Add(time.Hour).Unix(),
}, []byte("dev-secret"), "HS256")
```

Use it on requests:

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/admin-only
```

`mw/jwt` is authentication only. It verifies the token, stores claims in `Ctx.Locals("jwt_claims")`, and sets `fh.Principal` so the existing authorization layers can operate:

```go
app.Get("/admin-only",
    jwt.New(jwt.Config{Secret: []byte("dev-secret")}),
    fh.RequireRole("admin"),
    handler,
)
```

For policy-based authorization, keep using `contrib/mw/authz` with `SubjectFromPrincipal()` or `SubjectFromHeadersOrPrincipal()`.


## Continued production hardening

This build continues the production feature work without adding duplicate authorization middleware. Authorization remains in the existing core helpers and `contrib/mw/authz`; JWT is authentication and principal extraction only.

New production additions:

- Core dependency health checks with `app.AddHealthCheck(...)`.
- Readiness now fails when a dependency check fails.
- Queue job listing via `DurableQueue.ListJobs(ctx, state, limit)`.
- Queue DLQ operations via `RetryFailed` and `DiscardFailed` across file, memory and SQL storage.
- Admin queue endpoints for listing, retrying and discarding failed jobs.
- `mw/requesthash` for request body SHA-256 diagnostics.
- `mw/slowlog` for request latency SLO logging.
- `App.Logger()` accessor for middleware/integration logging.

Protected queue ops endpoints:

```text
GET  /_fh/admin/queue/jobs?state=failed&limit=100
POST /_fh/admin/queue/:id/retry
POST /_fh/admin/queue/:id/discard
```

See `docs/PRODUCTION_GAPS_CONTINUED.md` and `examples/production/main.go`.

## Middleware documentation index

Every core and contrib middleware package now includes a local `README.md` explaining what it does, how to implement it, operational impact, ordering guidance, and production considerations. See [`docs/MIDDLEWARE_READMES.md`](docs/MIDDLEWARE_READMES.md) for the full index.



## Additional production-gap fixes

This build also adds cleanup/janitor support for long-running production services:

- `IdempotencyJanitor` optional interface for expiring replay records.
- `QueueJanitor` optional interface for purging old terminal queue jobs.
- File, memory and SQL reliability adapters implement the cleanup interfaces.
- `Reliability.PurgeExpiredIdempotency(ctx, now)` helper.
- `DurableQueue.PurgeJobs(ctx, state, before, limit)` helper.
- Protected admin queue purge endpoint:

```text
POST /_fh/admin/queue/purge?state=done&before=2026-07-01T00:00:00Z&limit=1000
```

See `docs/PRODUCTION_GAPS_NEXT.md`.

## Production gap continuation

This package adds hardened CORS defaults, hashed API-key records, deny-by-default admin ops, CSP nonce/report-only support, mTLS middleware, SSE helpers, and a small `fh` CLI:

```bash
go run ./cmd/fh jwt:sign --secret dev-secret --sub u1 --roles admin --ttl 1h
go run ./cmd/fh apikey:generate --prefix fh_live
```

Admin endpoints are now deny-by-default unless `Auth` is configured or `AllowInsecure` is explicitly set for local development.

## Production gaps continued

The latest production batch adds asymmetric JWT verification/signing helpers, JWKS parsing, `jti` revocation hooks, required-claim validation, a stored outbox/inbox implementation, tamper-evident audit hash chaining, and baseline smoke tests across all middleware packages. See `docs/PRODUCTION_GAPS_CONTINUED_REST.md`.


### Continued production implementation

This package includes additional production support added after the security/core batch:

- `mw/jwt.JWKSCache` for JWKS-backed JWT verification through the existing authentication middleware.
- `cluster` package for node heartbeat and lease-based leader election.
- `config` package for JSON/env driven `fh.Config` loading.
- `docs/REMAINING_GAPS_AND_CONTINUED_IMPLEMENTATION.md` with the current gap list and next implementation priorities.

## Root-package HTTP client

This archive includes a new production-grade outbound HTTP client directly in the root `fh` package. Use `fh.NewClient`, `fh.ClientConfig`, `fh.ClientMiddleware`, `fh.Request`, and `fh.Response` without creating a separate `client/` package.

Key capabilities:

- high-performance connection pooling with HTTP/1.1, HTTPS and HTTP/2 support through Go's hardened transport
- fluent request builder with headers, query params, path params, cookies, JSON, form, multipart, raw bytes, strings and stream bodies
- typed helpers: `fh.GetJSON[T]`, `fh.PostJSON[Req,Res]`, `fh.PutJSON[Req,Res]`
- `fh` JSON engine integration through `CurrentJSONEngine()`
- lifecycle hooks for request, retry, redirect, DNS, connect, TLS, connection reuse and response stages
- middleware chain: logger, recover, headers, auth, API key, HMAC signing, idempotency, gzip request compression, bulkhead, rate limiting, circuit breaker and body limits
- resilience: retry policies, jitter backoff, Retry-After support, circuit breaker, bulkhead and async/batch execution
- security: strict outbound SSRF policy, allowed/blocked hosts, HTTPS requirement, redirect policy and sensitive URL/header redaction
- response helpers for bytes, string, JSON decode, streaming, safe drain/close and save-to-file

See `docs/http-client.md` and `examples/http_client` for usage.

### Root HTTP Client Continuation

The outbound HTTP client lives directly in the root `fh` package. It now includes additional production features beyond the initial implementation: replayable retry bodies, service clients, request-id/trace middleware, in-memory HTTP cache, status enforcement, token-source auth, round-robin load balancing, streaming/atomic downloads, and stricter dial-path SSRF checks.


## Secure Go-WASM Fetch transport

The repository includes `mw/securetransport`, the shared `pkg/securetransport` protocol, and a TypeScript/JavaScript Go-WASM Fetch client under `wasm/`. It provides device-signed session establishment, pinned static plus ephemeral X25519 key agreement, independent AES-256-GCM directional keys, encrypted request/response bodies and application headers, exact target binding, replay prevention, origin/Fetch-Metadata validation, response metadata hiding, device/session revocation, and pluggable stores.

Build with `make wasm`; see `docs/secure-wasm-transport.md` and `examples/secure_wasm`.


## Benchmarks
