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
    app := fh.New()

    app.Use(recover.New())
    app.Use(requestid.New())
    app.Use(security.New())
    app.Use(logger.New())
    app.Use(ratelimiter.New(ratelimiter.Config{Max: 300, Window: time.Minute}))

    app.Get("/", func(c *fh.Ctx) error {
        return c.SendString("ok")
    })

    log.Fatal(app.Listen(":3000"))
}
```

## Recommended production baseline

Use the following middleware order for most public APIs:

1. `recover` — panic recovery.
2. `requestid` and `correlationid` — request tracking.
3. `security` — secure response headers.
4. `cors` — browser cross-origin policy, only when required.
5. `bodylimit` and `timeout` — resource protection.
6. `ratelimiter`, `ipwhitelist`, `apikey`, `basicauth`, `signature`, `csrf`, or `session` — route-specific access control.
7. `logger` and `metrics` — observability.
8. `cache`, `compress`, `static`, `proxy`, `rewrite` — response/routing features.

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

Each subdirectory contains its own `README.md` with focused examples.
