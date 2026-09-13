# fh middleware

This directory contains first-class middleware packages for `github.com/oarkflow/fh`. Each package is intentionally small, composable, and importable on its own.

## Usage pattern

```go
package main

import (
    "log"
    "time"

    "github.com/oarkflow/fh"
    "github.com/oarkflow/fh/mw/recover"
    "github.com/oarkflow/fh/mw/requestid"
    "github.com/oarkflow/fh/mw/security"
    "github.com/oarkflow/fh/mw/logger"
    "github.com/oarkflow/fh/mw/ratelimiter"
)

func main() {
    app := fh.New(fh.WithSecureByDefault(true))

    app.Use(recover.New())
    app.Use(requestid.New())
    app.Use(logger.New())
    app.Use(security.New())
    app.Use(ratelimiter.New(ratelimiter.Config{Max: 300, Window: time.Minute}))

    app.Get("/", func(c fh.Ctx) error {
        return c.SendString("ok")
    })

    log.Fatal(app.ListenWithGracefulShutdown(":3000"))
}
```

## Recommended production baseline

Use the following middleware order for most public APIs:

1. `recover` — panic recovery.
2. `requestid` and `correlationid` — request tracking.
3. `logger` and `metrics` — observe downstream failures and denials.
4. `security` — secure response headers.
5. `cors` — browser cross-origin policy, only when required.
6. `bodylimit` and `timeout` — resource protection.
7. trusted `realip`, then rate limits and route-specific authentication/authorization.
8. `session` followed by `csrf` for cookie-authenticated browser routes.
9. `cache`, `compress`, `static`, `proxy`, `rewrite` — response/routing features.

## Middleware packages

| Package | Purpose |
|---|---|
| `actor` | Serializes requests by a computed actor/key. |
| `apikey` | API key authentication from header or query. |
| `apiversion` | Header-based API version enforcement and deprecation headers. |
| `basicauth` | Production-ready Basic Auth with memory/CSV/JSON storage and PBKDF2 helpers. |
| `bodylimit` | Rejects requests whose already-buffered body exceeds a configured size. |
| `cache` | Bounded in-memory response cache for safe cacheable responses. |
| `circuitbreaker` | Opens a circuit after repeated failures and protects downstreams. |
| `compress` | Gzip response compression using body transforms. |
| `contract` | Request contract checks for method, content type, and accept headers. |
| `correlationid` | Propagates or generates a correlation ID. |
| `cors` | CORS headers, preflight handling, static and dynamic origin allow rules. |
| `csrf` | CSRF token validation using header, form field, and cookie token. |
| `earlydata` | Rejects unsafe TLS early-data requests. |
| `idempotency` | Stores a deterministic idempotency key in request locals. |
| `httpsignature` | Negotiates and signs nonce-bound RFC 9421 responses with Ed25519. |
| `ipwhitelist` | IP/CIDR allowlist and blocklist enforcement. |
| `lifecycle` | Request lifecycle hooks around handler execution. |
| `logger` | Async access logging with formats, slog, skip rules, and backpressure controls. |
| `metrics` | Request counters and JSON/Prometheus metrics endpoint. |
| `policy` | Combines route data-policy metadata with API versioning. |
| `proxy` | Reverse proxy and simple prefix gateway. |
| `ratelimiter` | Fixed-window rate limiting with sharded in-memory store. |
| `recover` | Panic recovery with stack logging and custom error handling. |
| `reliability` | Route reliability policy and typed reliable endpoint wrapper. |
| `replay` | Nonce/replay protection. |
| `requestid` | Validated request ID propagation/generation. |
| `rewrite` | Path rewrite middleware with params, methods, and host constraints. |
| `security` | Common hardened security response headers. |
| `session` | Signed cookie sessions with memory/file stores. |
| `signature` | HMAC request/webhook signature verification. |
| `skip` | Predicate toolkit to skip middleware safely. |
| `static` | Static file serving with safe paths and cache/download controls. |
| `timeout` | Adds a context deadline and timeout response. |
| `workflow` | Sequential handler/job workflow composition. |
| `acceptquery` | Advertises and enforces RFC 10008 request formats for QUERY. |
| `adaptiveconcurrency` | Adjusts concurrency admission using observed latency and errors. |
| `admin` | Mounts protected operational endpoints for runtime, routes and queues. |
| `audit` | Records structured core `fh.AuditEvent` values. |
| `auditlog` | Emits request-level security/audit summaries to configurable sinks. |
| `backpressure` | Applies admission pressure when work or queues saturate. |
| `bulkhead` | Limits concurrent work globally or per key. |
| `coalesce` | Collapses concurrent identical requests to prevent a thundering herd. |
| `compliance` | Enforces route-security metadata and data policy. |
| `conditional` | Evaluates HTTP validators and precondition headers. |
| `contentdigest` | Verifies and emits RFC 9530 Content-Digest fields. |
| `decompress` | Performs bounded gzip request decompression. |
| `etag` | Generates and validates entity tags. |
| `fetchmetadata` | Enforces browser Fetch Metadata isolation policy. |
| `hostguard` | Restricts accepted Host/authority values. |
| `ipreputation` | Scores, decays and blocks risky client IPs. |
| `ipthrottle` | Applies bounded per-IP and global fixed-window limits. |
| `loadshed` | Rejects work based on process resource pressure. |
| `maintenance` | Provides an atomic maintenance-mode switch. |
| `mtls` | Authorizes verified client-certificate identities. |
| `override` | Supports allowlisted HTTP method overrides on POST. |
| `pprof` | Exposes protected Go profiling endpoints. |
| `privacy` | Filters sensitive telemetry fields. |
| `realip` | Resolves client identity through explicitly trusted proxies. |
| `requestdedup` | Rejects or coalesces duplicate request bodies/keys. |
| `requesthash` | Computes a request digest for downstream policy and audit. |
| `retrybudget` | Bounds retry amplification per key. |
| `scheduler` | Performs priority-weighted concurrency admission. |
| `securetransport` | Terminates the encrypted browser/WASM application protocol. |
| `servertiming` | Emits RFC 8638 Server-Timing metrics. |
| `slidingwindow` | Applies sliding-window request limits. |
| `slowlog` | Logs requests exceeding a latency threshold. |
| `slowloris` | Adds goroutine and heap admission checks. |
| `smartcache` | Provides bounded, variant-aware HTTP response caching. |
| `tenant` | Resolves and stores tenant identity. |
| `tenantlimit` | Limits concurrent work per tenant. |
| `timestamp` | Enforces timestamp freshness and nonce replay protection. |
| `tracing` | Parses, creates and propagates trace context. |
| `validate` | Applies core validation to untyped routes. |
| `webhook` | Verifies, replay-protects and deduplicates webhook delivery. |

Each subdirectory contains its own `README.md` with focused examples.
