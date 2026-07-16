# Middleware

fh ships 65+ built-in middleware packages under `mw/`. This page documents the most commonly used ones in depth; every package (including those not detailed below) has its own `README.md` in its `mw/<package>/` directory — see the [full index](#full-package-index). All middleware follows the standard handler signature:

```go
func(c *fh.Ctx) error
```

## Global Middleware

```go
app.Use(mw1, mw2, mw3)
```

## Route-Level Middleware

```go
app.Get("/dashboard", authMiddleware, dashboardHandler)

group := app.Group("/api", middleware1, middleware2)
group.Get("/users", usersHandler)
```

## Skipping Middleware

Use the `mw/skip` package for conditional middleware execution:

```go
import "github.com/oarkflow/fh/mw/skip"

app.Use(skip.When(authMiddleware, skip.Path("/health", "/metrics")))
app.Use(skip.Unless(loggerMiddleware, skip.Method("GET", "POST")))
```

---

## Middleware Reference

### actor

Serializes requests by a computed actor/key, ensuring stateful serial processing per actor.

```go
import "github.com/oarkflow/fh/mw/actor"

app.Use(actor.New(actor.Config{
    Key: func(c *fh.Ctx) string {
        return c.Params("id") // serialize requests per user ID
    },
}))
```

### apikey

Authenticates requests via `X-API-Key` header or `api_key` query parameter.

```go
import "github.com/oarkflow/fh/mw/apikey"

app.Use(apikey.New(apikey.Config{
    Keys: []string{"secret-key-1", "secret-key-2"},
    // Or use a validator function:
    Validate: func(key string) bool {
        return validateKey(key)
    },
}))
```

| Config | Description |
|--------|-------------|
| `Keys` | Static list of valid API keys |
| `Validate` | Custom validation function |
| `Header` | Header name (default: `X-API-Key`) |
| `QueryParam` | Query parameter name (default: `api_key`) |
| `ErrorHandler` | Custom auth error handler |

### apiversion

Enforces API version from headers, emits deprecation warnings.

```go
import "github.com/oarkflow/fh/mw/apiversion"

app.Use(apiversion.New(apiversion.Config{
    Version: "2024-01-01",
    Deprecated: true,
    Sunset: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
}))
```

| Config | Description |
|--------|-------------|
| `Version` | Current API version string |
| `Deprecated` | If true, adds `Sunset` and `Deprecation` headers |
| `Sunset` | Sunset date for deprecated versions |

### basicauth

HTTP Basic Authentication with memory, CSV, or JSON user storage.

```go
import "github.com/oarkflow/fh/mw/basicauth"

app.Use(basicauth.New(basicauth.Config{
    Users: map[string]string{
        "admin": "password123",
    },
}))

// Or load from CSV/JSON file:
app.Use(basicauth.New(basicauth.Config{
    File: "./users.csv", // or ./users.json
}))
```

| Config | Description |
|--------|-------------|
| `Users` | Static user/password map |
| `File` | Path to CSV or JSON user file |
| `Realm` | Auth realm (default: `Restricted`) |
| `ErrorHandler` | Custom error handler |

### bodylimit

Rejects requests whose body exceeds the configured byte size.

```go
import "github.com/oarkflow/fh/mw/bodylimit"

app.Use(bodylimit.New(bodylimit.Config{
    MaxSize: 1 << 20, // 1MB
}))
```

Returns 413 Payload Too Large when exceeded.

### cache

In-memory response caching with configurable TTL.

```go
import "github.com/oarkflow/fh/mw/cache"

app.Use(cache.New(cache.Config{
    TTL:       5 * time.Minute,
    MaxCount:  1000,
    Methods:   []string{"GET"},
    Vary:      []string{"Accept-Encoding"},
}))
```

| Config | Description |
|--------|-------------|
| `TTL` | Cache entry TTL |
| `MaxCount` | Max cache entries |
| `Methods` | Cacheable methods (default: GET) |
| `Vary` | Vary header considerations |
| `Key` | Custom key generator |
| `Store` | Custom cache store |

### circuitbreaker

Circuit breaker pattern that opens after repeated failures to protect downstream services.

```go
import "github.com/oarkflow/fh/mw/circuitbreaker"

app.Use(circuitbreaker.New(circuitbreaker.Config{
    FailureThreshold: 5,
    ResetTimeout:     30 * time.Second,
}))
```

| Config | Description |
|--------|-------------|
| `FailureThreshold` | Failures before opening circuit |
| `ResetTimeout` | Time before half-open attempt |
| `OnStateChange` | State change callback |

States: **Closed** (normal), **Open** (rejecting requests), **Half-Open** (probing).

### compress

Gzip response compression with configurable level and minimum size.

```go
import "github.com/oarkflow/fh/mw/compress"

app.Use(compress.New(compress.Config{
    Level:   gzip.DefaultCompression,
    MinSize: 1024, // bytes
}))
```

| Config | Description |
|--------|-------------|
| `Level` | Compression level (0-9) |
| `MinSize` | Minimum size to compress |
| `Types` | MIME types to compress (default: text/*, +json, +xml) |

### contract

Validates request contracts: allowed methods, required content-type, and accept headers.

```go
import "github.com/oarkflow/fh/mw/contract"

app.Use(contract.New(contract.Config{
    Methods:     []string{"GET", "POST", "PUT"},
    ContentType: "application/json",
    Accept:      "application/json",
}))
```

### correlationid

Generates or propagates `X-Correlation-ID` header.

```go
import "github.com/oarkflow/fh/mw/correlationid"

app.Use(correlationid.New(correlationid.Config{
    Header: "X-Correlation-ID", // default
}))
```

### cors

Cross-Origin Resource Sharing with static/dynamic origin support.

```go
import "github.com/oarkflow/fh/mw/cors"

app.Use(cors.New(cors.Config{
    AllowOrigins:     []string{"https://example.com"},
    AllowMethods:     []string{"GET", "POST"},
    AllowHeaders:     []string{"Content-Type", "Authorization"},
    AllowCredentials: true,
    MaxAge:           3600,
}))
```

| Config | Description |
|--------|-------------|
| `AllowOrigins` | Allowed origins |
| `AllowMethods` | Allowed HTTP methods |
| `AllowHeaders` | Allowed request headers |
| `ExposeHeaders` | Exposed response headers |
| `AllowCredentials` | Allow cookies/credentials |
| `MaxAge` | Preflight cache duration |
| `AllowPrivateNetwork` | Private Network Access support |
| `OriginValidator` | Dynamic origin validation |

### csrf

CSRF token validation using double-submit cookie pattern.

```go
import "github.com/oarkflow/fh/mw/csrf"

app.Use(csrf.New(csrf.Config{
	CookieName: "csrf_token",
	HeaderName: "X-CSRF-Token",
	TrustedOrigins: []string{"https://app.example.com"},
}))
```

### earlydata

TLS 1.3 Early Data (0-RTT) protection. Rejects unsafe replay requests.

```go
import "github.com/oarkflow/fh/mw/earlydata"

app.Use(earlydata.New(earlydata.Config{}))
```

### idempotency

Extracts `Idempotency-Key` from headers and stores a deterministic hash in request locals.

```go
import "github.com/oarkflow/fh/mw/idempotency"

app.Use(idempotency.New(idempotency.Config{
    Header: "Idempotency-Key",
}))
```

### ipwhitelist

IP/CIDR allowlist and blocklist enforcement.

```go
import "github.com/oarkflow/fh/mw/ipwhitelist"

app.Use(ipwhitelist.New(ipwhitelist.Config{
    Allowlist: []string{"10.0.0.0/8", "192.168.1.1"},
    Blocklist: []string{"203.0.113.0/24"},
}))
```

### lifecycle

Request lifecycle hooks around handler execution.

```go
import "github.com/oarkflow/fh/mw/lifecycle"

app.Use(lifecycle.New(lifecycle.Config{
    Before: func(c *fh.Ctx) error {
        c.Locals("start", time.Now())
        return c.Next()
    },
    After: func(c *fh.Ctx) error {
        elapsed := time.Since(c.Locals("start").(time.Time))
        log.Printf("Request took: %v", elapsed)
        return nil
    },
}))
```

### logger

Async access logging with multiple output formats.

```go
import "github.com/oarkflow/fh/mw/logger"

app.Use(logger.New(logger.Config{
    Format: "combined", // common, combined, tiny, json, custom
    Output: os.Stdout,
}))
```

| Config | Description |
|--------|-------------|
| `Format` | Log format (common, combined, tiny, json, custom) |
| `CustomFormat` | Custom log format string |
| `Output` | io.Writer (default: os.Stdout) |
| `Skip` | Paths to skip |
| `Slog` | Use slog for structured logging |
| `DropPolicy` | Backpressure drop policy |
| `BufferSize` | Async buffer size |

### metrics

Request counters by method/path/status with JSON and Prometheus endpoints.

```go
import "github.com/oarkflow/fh/mw/metrics"

app.Use(metrics.New(metrics.Config{
    Path: "/_metrics", // default
}))

// Access metrics:
// GET /_metrics (JSON)
// GET /_metrics?format=prometheus (Prometheus text format)
```

### policy

Route data-policy metadata combined with API versioning.

```go
import "github.com/oarkflow/fh/mw/policy"

app.Use(policy.New(policy.Config{
    Sensitivity: "high",
    Retention:   "7d",
    Audit:       true,
}))
```

### proxy

Reverse proxy with configurable transport, prefix stripping, and header propagation.

```go
import "github.com/oarkflow/fh/mw/proxy"

app.Use(proxy.New(proxy.Config{
    Upstream: "http://localhost:3001",
    StripPrefix: "/api",
}))
```

| Config | Description |
|--------|-------------|
| `Upstream` | Target URL |
| `Transport` | Custom http.RoundTripper |
| `StripPrefix` | Prefix to strip from path |
| `Headers` | Headers to propagate |

### ratelimiter

Fixed-window rate limiting with sharded in-memory store.

```go
import "github.com/oarkflow/fh/mw/ratelimiter"

app.Use(ratelimiter.New(ratelimiter.Config{
    Max:     100,
    Window:  1 * time.Minute,
}))
```

| Config | Description |
|--------|-------------|
| `Max` | Max requests per window |
| `Window` | Time window duration |
| `Key` | Custom key function (default: IP) |
| `Skip` | Paths to skip |
| `ErrorHandler` | Custom rate limit error handler |

### recover

Panic recovery with stack trace logging.

```go
import "github.com/oarkflow/fh/mw/recover"

app.Use(recover.New(recover.Config{
    LogStack: true,
    ErrorHandler: func(c *fh.Ctx, err error) error {
        return c.Status(500).SendString("Internal Server Error")
    },
}))
```

### reliability

Per-route reliability policy and typed endpoint wrapper. See [Reliability Layer](reliability.md).

```go
import "github.com/oarkflow/fh/mw/reliability"

app.Use(reliability.New(reliability.Config{}))
```

### replay

Nonce/replay protection to prevent request replay attacks.

```go
import "github.com/oarkflow/fh/mw/replay"

app.Use(replay.New(replay.Config{
    Store: replay.NewMemoryStore(),
    MaxAge: 5 * time.Minute,
}))
```

### requestid

Generates or propagates `X-Request-ID` header.

```go
import "github.com/oarkflow/fh/mw/requestid"

app.Use(requestid.New(requestid.Config{
    Header: "X-Request-ID",
    Length: 32,
}))
```

| Config | Description |
|--------|-------------|
| `Header` | Header name |
| `Length` | Generated ID length |
| `Generator` | Custom ID generator |
| `Validator` | Custom ID validator |

### rewrite

URL path rewriting with parameter extraction and constraints.

```go
import "github.com/oarkflow/fh/mw/rewrite"

app.Use(rewrite.New(rewrite.Config{
    Rules: map[string]string{
        "/old-path": "/new-path",
        "/users/:id": "/api/users/:id",
    },
}))
```

### security

Hardened security response headers.

```go
import "github.com/oarkflow/fh/mw/security"

app.Use(security.New(security.Config{
    CSP:           "default-src 'self'",
    HSTS:          "max-age=31536000; includeSubDomains",
    XFrameOptions: "DENY",
    ContentTypeNoSniff: true,
    ReferrerPolicy: "strict-origin-when-cross-origin",
    PermissionsPolicy: "camera=(), microphone=()",
}))
```

| Config | Header |
|--------|--------|
| `CSP` | Content-Security-Policy |
| `HSTS` | Strict-Transport-Security |
| `XFrameOptions` | X-Frame-Options |
| `ContentTypeNoSniff` | X-Content-Type-Options |
| `XSSProtection` | X-XSS-Protection |
| `ReferrerPolicy` | Referrer-Policy |
| `PermissionsPolicy` | Permissions-Policy |
| `COOP` | Cross-Origin-Opener-Policy |
| `COEP` | Cross-Origin-Embedder-Policy |
| `CORP` | Cross-Origin-Resource-Policy |

### session

Signed cookie sessions with `MemoryStore` and `FileStore`.

```go
import "github.com/oarkflow/fh/mw/session"

manager := session.NewSessionManager(
    session.NewMemoryStore(5*time.Minute), // GC interval; bounded at 100k sessions by default
    session.SessionSecret([]byte("at-least-32-bytes-of-random-secret")),
)
app.Use(session.New(manager))

// In a handler:
sess := session.Get(c)
sess.Get("user_id")     // get value
sess.Set("user_id", 42) // set value
sess.Delete("user_id")  // delete value
sess.Flash("message")   // flash message
manager.Destroy(c, sess)    // destroy session (server-side + cookie)
manager.Regenerate(c, sess) // rotate session ID; call after login to prevent fixation
```

| SessionOption | Description |
|--------|-------------|
| `SessionSecret` / `SessionSecrets` | Cookie signing secret(s) (HMAC-signed, rotatable) |
| `SessionCookieName` | Cookie name (default: `session`) |
| `SessionMaxAge` | Session lifetime |
| `SessionSecure` / `SessionHTTPOnly` / `SessionSameSite` | Cookie attributes (secure defaults) |
| `MaxAge` | Session TTL |
| `HTTPOnly` | HTTPOnly flag (default: true) |
| `Secure` | Secure flag |

### signature

HMAC-SHA256 request/webhook signature verification.

```go
import "github.com/oarkflow/fh/mw/signature"

app.Use(signature.New(signature.Config{
    Secret: "webhook-secret",
    Header: "X-Signature-256",
}))
```

### skip

Predicate toolkit for conditional middleware execution.

```go
import "github.com/oarkflow/fh/mw/skip"

// Skip middleware when path matches
app.Use(skip.When(rateLimiter, skip.Path("/health", "/metrics")))

// Skip middleware unless method matches
app.Use(skip.Unless(authMiddleware, skip.Method("POST", "PUT", "DELETE")))

// Combine predicates
app.Use(skip.When(loggerMiddleware,
    skip.Any(skip.Path("/health"), skip.Method("OPTIONS")),
))

// Available predicates:
skip.Path("/health")            // matches path
skip.Method("GET")              // matches method
skip.Host("admin.example.com") // matches host
skip.Header("X-Internal")      // has header
skip.Query("skip_log")         // has query param
skip.Any(pred1, pred2)         // OR
skip.All(pred1, pred2)         // AND
skip.Not(pred)                 // NOT
```

### static

Enhanced static file serving with safety controls.

```go
import "github.com/oarkflow/fh/mw/static"

app.Use(static.New(static.Config{
    Root:     "./public",
    Prefix:   "/static",
    Browse:   true,
    MaxAge:   3600,
    Compress: true,
    Download: false, // Content-Disposition: attachment
}))
```

### timeout

Adds a context deadline with a configurable timeout response.

```go
import "github.com/oarkflow/fh/mw/timeout"

app.Use(timeout.New(timeout.Config{
    Timeout: 5 * time.Second,
    ErrorHandler: func(c *fh.Ctx) error {
        return c.Status(503).SendString("Service timeout")
    },
}))
```

### workflow

Composes steps into a sequential, conditional, branched, parallel, or job-oriented request workflow, with retry, per-step timeout, compensation, and observability hooks.

```go
import "github.com/oarkflow/fh/mw/workflow"

wf := workflow.New("checkout").
    UseWithOptions("charge-payment", chargePayment,
        workflow.WithRetry(2, 50*time.Millisecond),
        workflow.WithTimeout(3*time.Second)).
    Parallel("fan-out", reserveInventory, sendConfirmation).
    Job("schedule-shipment", "shipment.schedule")

app.Post("/orders", wf.Handler())
```

See [`mw/workflow/README.md`](../mw/workflow/README.md) and [`examples/workflow`](../examples/workflow) for the full API and a runnable example.

---

## Recommended Middleware Order (Production Baseline)

```go
app.Use(
    recover.New(),           // 1. Panic recovery (safety net)
    requestid.New(),         // 2. Request tracking
    correlationid.New(),     // 3. Correlation propagation
    security.New(),          // 4. Security headers
    cors.New(),              // 5. CORS (if needed)
    bodylimit.New(),         // 6. Body size limits
    timeout.New(),           // 7. Request timeout
    ratelimiter.New(),       // 8. Rate limiting
    ipwhitelist.New(),       // 9. IP access control
    apikey.New(),            // 10. Authentication
    logger.New(),            // 11. Access logging
    metrics.New(),           // 12. Metrics
    cache.New(),             // 13. Response caching
    compress.New(),          // 14. Compression
)
```

---

## Full Package Index

Packages detailed above: `actor`, `apikey`, `apiversion`, `basicauth`, `bodylimit`, `cache`, `circuitbreaker`, `compress`, `contract`, `correlationid`, `cors`, `csrf`, `earlydata`, `idempotency`, `ipwhitelist`, `lifecycle`, `logger`, `metrics`, `policy`, `proxy`, `ratelimiter`, `recover`, `reliability`, `replay`, `requestid`, `rewrite`, `security`, `session`, `signature`, `skip`, `static`, `timeout`, `workflow`.

Remaining packages — see `mw/<package>/README.md` for full usage:

| Package | Description |
|---|---|
| `mw/acceptquery` | Advertises/enforces RFC 10008 Accept-Query formats for HTTP QUERY |
| `mw/adaptiveconcurrency` | Auto-adjusts in-flight request limit based on latency/errors |
| `mw/admin` | Protected ops endpoints: runtime info, routes, queue stats/retry |
| `mw/audit` | Records structured audit events for compliance/security review |
| `mw/backpressure` | Rejects/slows admission under queue or worker saturation |
| `mw/bulkhead` | Limits concurrent execution globally or per key |
| `mw/coalesce` | Collapses concurrent identical requests into a single upstream call |
| `mw/compliance` | Enforces route security metadata and attaches data policy |
| `mw/conditional` | Handles If-None-Match / If-Match / If-Modified-Since preconditions |
| `mw/contentdigest` | Verifies/adds RFC 9530 Content-Digest header |
| `mw/decompress` | Bounded gzip request decompression with expansion-ratio limits |
| `mw/etag` | Adds/validates ETag headers |
| `mw/hostguard` | Rejects requests with unexpected Host headers |
| `mw/httpsignature` | Nonce-bound RFC 9421 Ed25519 response signatures |
| `mw/jwt` | Verifies signed JWTs, stores claims, sets `fh.Principal` |
| `mw/maintenance` | Runtime maintenance-mode switch for controlled downtime |
| `mw/mtls` | Validates verified client cert chains for high-trust routes |
| `mw/pprof` | Protected Go profiling endpoints |
| `mw/privacy` | Privacy-aware telemetry filtering for logs/traces/metrics/audit |
| `mw/realip` | Normalizes client IP from trusted proxy headers |
| `mw/requestdedup` | Prevents duplicate processing of identical requests |
| `mw/requesthash` | Computes/exposes request body hash for idempotency/audit |
| `mw/retrybudget` | Limits retry traffic per key to prevent outage amplification |
| `mw/scheduler` | Priority-based weighted admission with concurrency limits |
| `mw/securetransport` | Application-layer encrypted Fetch transport for WASM clients |
| `mw/servertiming` | Emits `Server-Timing` response headers |
| `mw/slidingwindow` | Sliding-window rate limiting |
| `mw/slowlog` | Logs requests exceeding a latency threshold |
| `mw/smartcache` | Adaptive response caching |
| `mw/tenant` | Extracts tenant identity from header/host/path/JWT |
| `mw/tenantlimit` | Limits concurrent requests per tenant |
| `mw/tracing` | Creates/propagates trace IDs and span metadata |
| `mw/validate` | Request validation |
| `mw/webhook` | Verifies webhook signatures with replay protection |

See [`mw/README.md`](../mw/README.md) for the package overview.
