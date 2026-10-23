# fh — Zero-Dependency Go Web Framework

**fh** is a standalone, high-performance HTTP/1.1 + HTTP/2 + WebSocket web framework for Go with **no third-party dependencies beyond `golang.org/x/crypto`** (used only for optional OCSP stapling and ACME certificate automation). It implements HTTP parsing, routing, HTTP/2 framing, HPACK, and WebSocket protocols from scratch — no wrappers around `net/http` or `fasthttp`.

Full reference documentation lives in [`docs/`](docs/README.md).

[Documentation](docs/README.md) · [Changelog](CHANGELOG.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [Versioning & Stability](docs/versioning-and-compatibility.md)

## Features

- **Minimal dependencies** — only the Go standard library plus `golang.org/x/crypto` (used for optional OCSP stapling and ACME/autocert).
- **HTTP/1.1** — full request/response parsing, keep-alive, pipelining, chunked transfer, trailers, incremental streaming uploads.
- **HTTP/2** — TLS ALPN, h2c prior knowledge/upgrade, stream multiplexing, HPACK encoding/decoding, RFC 8441 extended CONNECT.
- **WebSocket (RFC 6455)** — low-level `Conn` and high-level `EventHub` pub/sub layer (rooms, topics, authorization, heartbeats), served over HTTP/1.1 or HTTP/2.
- **Trie-based router** — high-performance radix tree router with named (`:param`) and wildcard (`*wild`) parameters, named routes, and route groups.
- **Typed endpoints & OpenAPI 3.1** — generic `GetTyped`, `PostTyped`, `QueryTyped` handlers with struct validation (`param`, `query`, `header`, `cookie` tags), auto-generated OpenAPI 3.1 schemas, and embedded Swagger UI.
- **HTTP QUERY Method** — draft-compliant safe and idempotent queries carrying request bodies (`app.Query`, `app.QueryTyped`).
- **Server-Sent Events (SSE)** — native `c.SSE` streaming with comment keep-alives, custom event types, and automatic disconnect handling.
- **Runtime Execution Fabric (REF)** — data-dependency DAG execution engine with typed fact keys, decoupled intent contracts, and transport-neutral projections.
- **Shared State & Clustered KV** — application-wide `kv.Provider` and `kv.Store` abstraction with atomic `Mutate`, CAS, background TTL sweeps, and memory/file/distributed implementations.
- **Hierarchical Request Budgets** — per-request deadline, CPU, memory, and outbound call limits with sub-budget delegation (`fh.Budget`).
- **Service Level Objectives (SLO)** — route-level latency and error rate targets with multi-window sliding burn-rate tracking (`SLOTracker`).
- **Live Config & Certificate Hot-Reload** — transactional, zero-downtime reconfiguration of routes, policies, and TLS certificates with generation tracking (`ConfigReloader`).
- **Tamper-Evident Merkle Audit Ledger** — cryptographic audit logging with SHA-256 Merkle tree checkpoints for verifiable integrity.
- **Codec system** — pluggable body parsers for JSON (grow-on-demand buffer), XML, form, multipart, CSV, NDJSON, text, and binary.
- **74 built-in middleware packages** — security, CORS, CSRF, rate limiting, distributed caching, circuit breaking, tracing, metrics, sessions, and compliance.
- **Reliability layer** — request journaling, idempotency keys, durable async queue, outbox/inbox, and dead letter queues (DLQ) without external queue services.
- **Compliance layer** — 10 compliance profiles (Business, Professional, Enterprise, Strict, Financial, Healthcare, Government) with auto-mounted evidence endpoints.
- **Zero-allocation pool design** — `sync.Pool` recycling for request contexts, byte buffers, HPACK decoders, and writers.
- **Hardened TLS/mTLS** — TLS 1.3 config builder, verified peer state extraction, cert pinning, OCSP stapling, and ACME / Let's Encrypt automation.
- **Prefork & zero-downtime restarts** — multi-process `SO_REUSEPORT` supervisor with `SIGHUP` rolling worker handoff and zero dropped connections.
- **Outbound HTTP client** — fluent HTTP/1.1 + HTTP/2 client (`fh.NewClient`) with connection pooling, retries, jitter backoff, circuit breaker, and SSRF protection.
- **Linux kernel-assisted transport** — raw nonblocking sockets, sharded epoll or io_uring, SO_REUSEPORT CPU steering, and optional XDP packet admission.

## Installation

```bash
go get github.com/oarkflow/fh
```

Requires Go 1.27.3 or later.

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

## Runtime Profiles

`fh` provides specialized application profiles tailored to different deployment tiers:

- **`fh.New(opts...)`**: The flexible baseline. You configure explicitly what you need.
- **`fh.NewFast(opts...)`**: Maximum throughput candidate for microbenchmarks and protected internal topologies, stripping unnecessary checks and optimizing hot paths.
- **`fh.NewProduction(opts...)`**: Balanced production defaults with bounded memory buffers, strict protocol parsing, panic recovery, and safe error rendering. Combine with `fh.WithSecureByDefault(true)` to enforce security headers and bounds.
- **`fh.NewEnterprise(opts...)`**: Full enterprise posture with strict compliance enforcement (`ComplianceEnterprise`), structured audit logs, reliability queue, and auto-mounted evidence endpoints (`/_fh/compliance`, `/_fh/runtime`, `/_fh/routes`).

See [Performance Profiles](docs/performance_profiles.md) for detailed configuration differences.

## Production application template

Generate a structured server with secure sessions, strict Host/origin policy,
RBAC, bounded requests, rate limiting, audit logs, protected health/metrics,
SQLite persistence, graceful shutdown, encrypted WASM Fetch with signed
ciphertext responses, and a structured, componentized browser frontend
("FH Control Center", built with [`@oarkflow/lithe`](https://www.npmjs.com/package/@oarkflow/lithe)):

```bash
go run ./cmd/fh-init -module example.com/acme/service -dir ./service
cd service
./run.sh
```

The generator initializes `.env` from `.env.example` and preserves it on
subsequent `-force` regeneration. Add `-verify` to have it build the server,
boot it on a spare loopback port, walk through the app's full HTTP surface
(assets, the WASM transport bundle, and the login/secure-bootstrap/logout
lifecycle), and open it in your browser - see the generated README's
`fh-init -verify` section. [`examples/production-app`](examples/production-app)
is a checked-in, buildable-in-CI copy of this template's current output.

Loopback development uses short-lived process-local cryptographic keys. The
generated server fails closed outside the explicit `development` environment
unless HTTPS and persistent session, login, operations, transport, and response
signing secrets are configured. See the generated README and
[secure WASM transport guide](docs/secure-wasm-transport.md) before deployment.

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

### Typed Endpoints & OpenAPI 3.1

Typed endpoints (`GetTyped`, `PostTyped`, `PutTyped`, `DeleteTyped`, `QueryTyped`, `AllTyped`) provide automatic struct parsing, validation, multi-source binding (`param`, `query`, `header`, `cookie` struct tags), and JSON schema generation:

```go
type CreateUserRequest struct {
    Name  string `json:"name" validate:"required"`
    Email string `json:"email" validate:"required,email"`
}

type UserResponse struct {
    ID    string `json:"id"`
    Name  string `json:"name"`
    Email string `json:"email"`
}

app.PostTyped("/users", func(c fh.Ctx, req CreateUserRequest) (UserResponse, error) {
    return UserResponse{ID: "usr_100", Name: req.Name, Email: req.Email}, nil
})

// Expose auto-generated OpenAPI 3.1 schema and interactive HTML docs
app.EnableOpenAPI("/openapi.json", fh.OpenAPIConfig{
    Title:   "API Documentation",
    Version: "1.0.0",
})
app.EnableDocs("/docs")
```

### HTTP QUERY Method

`fh` natively supports the draft IETF HTTP `QUERY` method — an idempotent, safe request method that carries a request body:

```go
app.Query("/search", func(c fh.Ctx) error {
    return c.SendString("Search query processed: " + string(c.Body()))
})
```

See [Routing](docs/routing.md) and [Native Features](docs/native-features.md) for full references.

## Middleware

```go
app.Use(logger.New(), recover.New(), cors.New(cors.Config{
    AllowOrigins: []string{"https://example.com"},
}))

// Route- or group-level
app.Get("/dashboard", authMiddleware, dashboardHandler)
admin := app.Group("/admin", authMiddleware, adminLogger)
```

`fh` includes **74 modular middleware packages** under `mw/`, each importable independently and documented with its own `README.md`:

| Category | Packages | Description |
|---|---|---|
| **Auth & Access Control** | `basicauth`, `apikey`, `csrf`, `cors`, `hostguard`, `security`, `mtls`, `ipwhitelist`, `ipthrottle`, `ipreputation`, `fetchmetadata`, `slowloris`, `earlydata`, `compliance` | Basic/API-key auth, CSRF, CORS, client-cert mTLS, host guards, IP throttling/reputation, and security headers. |
| **Resilience & Traffic** | `ratelimiter`, `slidingwindow`, `circuitbreaker`, `bulkhead`, `loadshed`, `backpressure`, `adaptiveconcurrency`, `retrybudget`, `timeout`, `bodylimit` | Sliding window rate limits, circuit breakers, bulkheads, load shedding, adaptive concurrency, and timeouts. |
| **Performance & Cache** | `cache`, `smartcache`, `coalesce`, `compress`, `decompress`, `etag`, `static`, `requestdedup` | In-memory/file response caching, request coalescing, deduplication, gzip compression/decompression, and ETags. |
| **Observability & Audit** | `logger`, `slowlog`, `tracing`, `metrics`, `servertiming`, `audit`, `auditlog`, `correlationid`, `requestid`, `pprof` | Structured logging, Prometheus metrics, distributed tracing propagation, audit ledgers, and pprof profiling. |
| **Transport & Cryptography** | `httpsignature`, `securetransport`, `contentdigest`, `signature`, `realip`, `proxy`, `override`, `timestamp` | RFC 9421 Ed25519 signatures, RFC 9530 Content-Digest, device-bound WASM encryption, reverse proxy, and trusted IP normalization. |
| **State & Orchestration** | `session`, `reliability`, `workflow`, `idempotency`, `replay`, `requesthash`, `tenant`, `tenantlimit`, `policy`, `actor`, `scheduler`, `lifecycle`, `acceptquery`, `apiversion`, `maintenance`, `conditional`, `skip`, `recover`, `rewrite`, `contract`, `privacy`, `admin`, `validate` | Multi-tenancy, sessions, idempotent execution, actor queues, API versioning, contract assertions, and panic recovery. |

See [`docs/middleware.md`](docs/middleware.md) for full configuration guides and recommended pipeline ordering, or [`mw/README.md`](mw/README.md) for the index.

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

## Server-Sent Events (SSE)

Native streaming SSE support with comment heartbeats, named event types, and automatic disconnect handling:

```go
app.Get("/events", func(c fh.Ctx) error {
    return c.SSE(func(events *fh.SSE) error {
        for i := 0; i < 5; i++ {
            if err := events.WriteEvent(fh.SSEEvent{
                Event: "notice",
                Data:  fmt.Sprintf("Message #%d", i),
                ID:    fmt.Sprintf("%d", i),
            }); err != nil {
                return err // client disconnected
            }
            if err := events.Ping(); err != nil {
                return err
            }
            time.Sleep(1 * time.Second)
        }
        return nil
    })
})
```

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
    "time"

    "github.com/oarkflow/fh/mw/session"
    "github.com/oarkflow/fh/pkg/storage/kv"
)

manager := session.NewSessionManager(
    kv.NewMemoryStore(kv.WithGCInterval(time.Hour), kv.WithMaxEntries(100000)),
    session.SessionSecret([]byte("at-least-32-bytes-of-random-secret")),
)
app.Use(session.New(manager))

app.Get("/login", func(c fh.Ctx) error {
    sess := session.Get(c)
    sess.Set("user_id", 42) // the middleware saves the session automatically
    return c.SendString("logged in")
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

## Shared State & Key-Value Storage

`fh` provides a clean `kv.Provider` and `kv.Store` abstraction for features requiring persistence or coordination (sessions, rate limits, replay protection, response caching, reputation scores):

```go
import (
    "context"

    "github.com/oarkflow/fh/pkg/storage/kv"
)

// In-memory provider for single-node deployments
provider := kv.NewMemoryProvider()

// File-backed durable provider
fileProvider, _ := kv.NewFileProvider("./.fh-data/state")

app := fh.New(fh.WithSharedState(provider))

// Acquire an isolated namespace store
store, err := app.StateStore(context.Background(), "ratelimit")
```

For distributed deployments, the [`fh-contrib`](https://github.com/oarkflow/fh-contrib) repository provides Redis and PostgreSQL `kv.Provider` adapters with atomic cross-process `Mutate` operations. See [Shared State](docs/shared-state.md).

## Hierarchical Request Budgets

Request budgets ensure no single request consumes unbounded system resources. Limits travel with the request context through handlers, database queries, and outbound client calls:

```go
import (
    "context"
    "time"
)

budget := &fh.Budget{
    Deadline:         time.Now().Add(5 * time.Second),
    MaxCPUTime:       500 * time.Millisecond,
    MaxMemoryBytes:   16 * 1024 * 1024,
    MaxUpstreamCalls: 3,
}
ctx := fh.WithBudget(context.Background(), budget)
```

Sub-budgets can be carved out for child routines or third-party dependencies. See [Budgets](docs/budget.md).

## Service Level Objectives (SLO)

Route-level latency and error budget tracking with real-time sliding-window burn-rate alerts:

```go
import "time"

tracker := fh.NewSLOTracker(10 * time.Second)
defer tracker.Stop()

tracker.Register("/api/v1/orders", fh.SLO{
    Availability:   0.999,
    P99Latency:     200 * time.Millisecond,
    BurnRateWindow: 5 * time.Minute,
})

app.Use(tracker.Handler())
```

See [SLO Tracking](docs/slo.md).

## Live Configuration & Certificate Hot-Reload

Zero-downtime, transactional updates of routing, security policies, and TLS certificates without restarting the server:

```go
reloader := fh.NewConfigReloader(app)

// Validates the new configuration structure before applying
gen, err := reloader.Reload(newConfig)
```

Every request records which `ConfigGeneration` handled it for deterministic operational debugging. See [Config Reload](docs/configreload.md).

## Tamper-Evident Merkle Audit Ledger

Cryptographically verifiable audit log tracking with chained SHA-256 Merkle tree checkpoints to guarantee audit immutability:

```go
import "time"

sink := fh.NewMerkleAuditSink(fh.MerkleConfig{
    BucketSize:         100,
    CheckpointInterval: time.Minute,
})
_ = sink
```

See [Merkle Audit](docs/merkle_audit.md).

## Runtime Execution Fabric (REF)

The Runtime Execution Fabric separates transport concerns from business domain logic by expressing operations as transport-neutral intents connected via a data-dependency DAG:

```go
// Connect fh to an execution engine
app.SetREF(engine)
```

See [Runtime Execution Fabric](docs/runtime-execution-fabric.md) and [`examples/ref_integration`](examples/ref_integration).

## Outbound HTTP Client

A production-grade HTTP/1.1 + HTTP/2 client lives directly in the root `fh` package — no separate `client/` module.

```go
client := fh.NewClient(fh.ClientConfig{})
resp, err := client.Get(ctx, "https://api.example.com/users")

user, err := fh.GetJSON[User](ctx, client, "https://api.example.com/users/1")
```

Includes fluent request building, typed helpers (`GetJSON[T]`, `PostJSON[Req,Res]`), retry policies with jitter backoff, circuit breaker, bulkhead, rate limiting, outbound SSRF protection, and streaming/atomic downloads. See [HTTP Client](docs/http-client.md).

## Secure WASM Transport

`mw/securetransport`, the shared `pkg/securetransport` protocol, and a TypeScript/JavaScript TinyGo/WASM Fetch client under `wasm/` provide device-signed session establishment, X25519 key agreement, AES-256-GCM encrypted bodies/headers, replay prevention, and pluggable stores. The secure WASM example additionally negotiates RFC 9421/RFC 9530 Ed25519 signatures over ciphertext and verifies them before decryption.

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
| `hardened-server` | Pre-configured production security baseline with audit logging, rate limiting, and graceful drain |
| `http-modern` | Modern HTTP helpers and protocol behavior |
| `kernel_server` | Kernel-assisted server configuration and readiness |
| `prefork` | Multi-process prefork serving |
| `production-app` | Checked-in `fh-init` output: sessions, RBAC, SQLite, secure WASM transport, and the `@oarkflow/lithe` frontend |
| `ref_integration` | Runtime Execution Fabric integration: typed fact keys, decoupled intents, and transport adapters |
| `rfc9421` | RFC 9421 signed-response server plus fail-closed Go and WebCrypto clients |
| `secure_wasm` | Session + secure WASM client demo for encrypted API calls |

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
- **Process-local defaults.** Middleware and cluster state use the shared `pkg/storage/kv.Store` interface. Defaults are in-process, so rate limits, caches, sessions, replay markers, and cluster leases are per instance unless you configure a shared provider. `kv.Provider` and `fh.WithSharedState` give isolated application-wide namespaces and lifecycle management; the built-in providers are memory and single-host files. For a distributed deployment, [`fh-contrib`](https://github.com/oarkflow/fh-contrib) provides Redis and PostgreSQL `kv.Provider` adapters with cross-process-atomic `Mutate`. See [Shared State](docs/shared-state.md).
- **No gRPC or GraphQL protocol handlers.** Only MIME-type constants exist for GraphQL; there's no built-in gRPC server. Both are addressable via a reverse-proxy route (`mw/proxy`) to a dedicated service if needed.

## Community & Governance

- **Changelog**: Detailed release notes and version history are recorded in [CHANGELOG.md](CHANGELOG.md).
- **Versioning & Stability Policy**: Pre-v1 stability guidance, deprecation timelines, and our roadmap to v1.0 are documented in [Versioning and API Stability](docs/versioning-and-compatibility.md).
- **Contributing**: Guidelines for bug reports, local development, code conventions, and pull requests live in [CONTRIBUTING.md](CONTRIBUTING.md).
- **Security**: Vulnerability reporting process and supported version windows are detailed in [SECURITY.md](SECURITY.md).

## License

fh is distributed under the [MIT License](LICENSE).
