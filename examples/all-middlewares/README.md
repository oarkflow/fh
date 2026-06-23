# All Middlewares & Features Example

A single Go server demonstrating every built-in middleware and core framework feature, with curl commands and expected output.

## Quick start

```bash
cd examples/all-middlewares
go run .
# listening on :3000
```

## Global Middleware Stack

The app applies these middleware globally (in this order):

1. **recover** – Panic recovery with stack trace
2. **requestid** – Request ID generation and propagation
3. **correlationid** – End-to-end correlation ID
4. **metrics** – Request counters (Prometheus)
5. **security** – Hardened response headers (CSP, HSTS, XFO, etc.)
6. **earlydata** – TLS 1.3 0-RTT replay protection
7. **bodylimit** – 1 MiB ceiling, skipped for health + static
8. **timeout** – 10s global deadline
9. **logger** – Structured JSON access logging
10. **cors** – Cross-Origin Resource Sharing
11. **compress** – Gzip compression for text responses
12. **rewrite** – Legacy path rewriting
13. **ratelimiter** – 120 req/min, exempt health + static
14. **session** – Signed-cookie session management

Plus the **global reliability middleware** (idempotency/journal) when `ReliabilityConfig.Enabled` is true.

---

## Per-Route Demos

### 1. Health Check

```go
app.Get("/health", func(c *fh.Ctx) error {
    return c.JSON(fh.Map{"status": "ok"})
})
```

```bash
curl http://localhost:3000/health
```

```json
{"status":"ok"}
```

---

### 2. Request ID & Correlation ID

Demonstrates `requestid` and `correlationid` middleware.

```go
app.Get("/demo/request-id", func(c *fh.Ctx) error {
    return c.JSON(fh.Map{
        "request_id":     c.Locals("request_id"),
        "correlation_id": c.Locals("correlationID"),
    })
})
```

```bash
curl http://localhost:3000/demo/request-id
```

```json
{"correlation_id":"abc123...","request_id":"req_..."}
```

Response also includes `X-Request-ID` and `X-Correlation-ID` headers.

---

### 3. Security Headers

Set globally. Every response includes headers like `X-Frame-Options: DENY`,
`X-Content-Type-Options: nosniff`, `X-XSS-Protection: 0`, and more.

```bash
curl -I http://localhost:3000/demo/security-headers
```

```
X-Frame-Options: DENY
X-Content-Type-Options: nosniff
X-XSS-Protection: 0
Content-Security-Policy: default-src 'self'
Permissions-Policy: geolocation=(), microphone=(), camera=(), ...
```

---

### 4. Basic Auth

Protects a route with HTTP Basic Authentication (PBKDF2-SHA256 hashing).

```go
app.Get("/demo/basic-auth",
    basicauth.New("admin", "password"),
    func(c *fh.Ctx) error {
        return c.JSON(fh.Map{"message": "authenticated"})
    },
)
```

**Without credentials (401):**

```bash
curl -i http://localhost:3000/demo/basic-auth
```

```
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Basic realm="Restricted"
```

**With credentials (200):**

```bash
curl -u admin:password http://localhost:3000/demo/basic-auth
```

```json
{"message":"authenticated","user":"admin"}
```

---

### 5. API Key Auth

Authenticates via `X-API-Key` header with constant-time comparison.

```go
app.Get("/demo/api-key",
    apikey.New(apikey.Config{Header: "X-API-Key", Keys: []string{"demo-key-123"}}),
    func(c *fh.Ctx) error { return c.JSON(fh.Map{"message": "api key accepted"}) },
)
```

```bash
curl -H 'X-API-Key: demo-key-123' http://localhost:3000/demo/api-key
```

```json
{"message":"api key accepted"}
```

```bash
curl -i -H 'X-API-Key: wrong-key' http://localhost:3000/demo/api-key
```

```
HTTP/1.1 401 Unauthorized
```

---

### 6. API Versioning

Reads version from `Accept-Version` header, sets `c.Locals("api_version")`,
rejects unsupported versions, emits deprecation headers.

```go
app.Get("/demo/api-version",
    apiversion.New(apiversion.Config{Default: "2026-01", Supported: []string{"2025-06", "2026-01"}}),
    func(c *fh.Ctx) error { return c.JSON(fh.Map{"api_version": c.Locals("api_version")}) },
)
```

```bash
curl -H 'Accept-Version: 2026-01' http://localhost:3000/demo/api-version
```

```json
{"api_version":"2026-01"}
```

```bash
curl -i -H 'Accept-Version: 2099-99' http://localhost:3000/demo/api-version
```

```
HTTP/1.1 400 Bad Request
```

---

### 7. Route Policy (Data Sensitivity + API Version)

Groups data-handling policy with API version enforcement.

```go
app.Post("/demo/policy",
    policy.New(policy.Config{
        Data:    fh.DataPolicy{Sensitivity: "pii", RedactLogs: true},
        Version: apiversion.Config{Default: "v1", Supported: []string{"v1"}},
    }),
    func(c *fh.Ctx) error { return c.JSON(fh.Map{"message": "policy applied"}) },
)
```

```bash
curl -X POST http://localhost:3000/demo/policy
```

```json
{"message":"policy applied","api_version":"v1"}
```

---

### 8. Body Limit (Route-Specific)

Rejects requests over 100 bytes with 413 Payload Too Large.

```go
app.Post("/demo/body-limit",
    bodylimit.New(100),
    func(c *fh.Ctx) error { return c.JSON(fh.Map{"size": len(c.Body())}) },
)
```

**Under limit (200):**

```bash
curl -X POST -d '{"short":"ok"}' -H 'Content-Type: application/json' http://localhost:3000/demo/body-limit
```

```json
{"size":13}
```

**Over limit (413):**

```bash
curl -X POST -d '{"data":"this exceeds the 100 byte ceiling for this route and will be rejected"}' -H 'Content-Type: application/json' http://localhost:3000/demo/body-limit
```

```
HTTP/1.1 413 Payload Too Large
```

---

### 9. Request Timeout (Route-Specific)

Returns a timeout error if the handler exceeds 100ms.

```go
app.Get("/demo/timeout",
    timeout.New(100*time.Millisecond),
    func(c *fh.Ctx) error {
        time.Sleep(200 * time.Millisecond)
        return c.SendString("too slow")
    },
)
```

```bash
curl -i http://localhost:3000/demo/timeout
```

```
HTTP/1.1 408 Request Timeout
```

---

### 10. Rate Limiter (Route-Specific)

3 requests per minute. Sends `X-RateLimit-*` headers. 4th request is rejected.

```go
app.Get("/demo/rate-limit",
    ratelimiter.New(ratelimiter.Config{Max: 3, Window: time.Minute, SendHeaders: true}),
    func(c *fh.Ctx) error { return c.JSON(fh.Map{"message": "request allowed"}) },
)
```

```bash
for i in 1 2 3 4; do
  echo "Request $i: $(curl -s -o /dev/null -w '%{http_code} - X-RateLimit-Remaining: %{header{x-ratelimit-remaining}}' http://localhost:3000/demo/rate-limit)"
done
```

```
Request 1: 200 - X-RateLimit-Remaining: 2
Request 2: 200 - X-RateLimit-Remaining: 1
Request 3: 200 - X-RateLimit-Remaining: 0
Request 4: 429 - X-RateLimit-Remaining: 0
```

---

### 11. Response Caching

In-memory cache with 30s TTL. First request is `MISS`, subsequent requests within
30s are `HIT`.

```go
app.Get("/demo/cache",
    cachemw.New(cachemw.Config{TTL: 30 * time.Second, MaxEntries: 64}),
    func(c *fh.Ctx) error {
        return c.JSON(fh.Map{"generated_at": time.Now().UTC()})
    },
)
```

```bash
curl -i http://localhost:3000/demo/cache 2>&1 | grep -i x-cache
# X-Cache: MISS

curl -i http://localhost:3000/demo/cache 2>&1 | grep -i x-cache
# X-Cache: HIT
```

---

### 12. Gzip Compression

Compresses responses when client sends `Accept-Encoding: gzip`.

```go
app.Get("/demo/compress", func(c *fh.Ctx) error {
    return c.SendString(strings.Repeat("hello world! ", 100))
})
```

```bash
# Without compression
curl -s http://localhost:3000/demo/compress | wc -c
# 1200

# With compression
curl -s -H 'Accept-Encoding: gzip' http://localhost:3000/demo/compress -o /tmp/compressed
gunzip < /tmp/compressed | wc -c
# 1200
```

---

### 13. CSRF Protection

Double-submit cookie pattern. GET returns a token; POST requires matching
`X-CSRF-Token` header.

```go
csrfProtection := csrf.New(csrf.Config{TrustedOrigins: []string{"http://localhost:3000"}})
app.Get("/demo/csrf-token", csrfProtection, func(c *fh.Ctx) error {
    return c.JSON(fh.Map{"csrf_token": c.Locals("csrf_token")})
})
app.Post("/demo/csrf-submit", csrfProtection, func(c *fh.Ctx) error {
    return c.JSON(fh.Map{"message": "CSRF token accepted"})
})
```

```bash
# Get a CSRF token (also sets cookie)
CSRF=$(curl -s -c /tmp/cookies http://localhost:3000/demo/csrf-token | python3 -c 'import sys,json; print(json.load(sys.stdin)["csrf_token"])')
echo "Token: $CSRF"

# Submit with valid token
curl -s -b /tmp/cookies -H "X-CSRF-Token: $CSRF" -X POST http://localhost:3000/demo/csrf-submit
# {"message":"CSRF token accepted"}

# Submit without token (403)
curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:3000/demo/csrf-submit
# 403
```

---

### 14. Request Contract

Enforces allowed method, content type, required headers, and max body size.

```go
app.Post("/demo/contract",
    contract.New(contract.Config{
        Methods:        []string{"POST"},
        ContentTypes:   []string{"application/json"},
        RequireHeaders: []string{"X-Client-ID"},
        MaxBodyBytes:   4096,
    }),
    func(c *fh.Ctx) error { return c.JSON(fh.Map{"message": "contract satisfied"}) },
)
```

```bash
# Valid request
curl -X POST -H 'Content-Type: application/json' -H 'X-Client-ID: abc' -d '{}' http://localhost:3000/demo/contract
# {"message":"contract satisfied"}

# Missing X-Client-ID
curl -i -X POST -H 'Content-Type: application/json' -d '{}' http://localhost:3000/demo/contract
# HTTP/1.1 400 Bad Request

# Wrong content type
curl -i -X POST -H 'Content-Type: text/plain' http://localhost:3000/demo/contract
# HTTP/1.1 415 Unsupported Media Type
```

---

### 15. HMAC Signature Verification

Verifies `X-Signature` header with `sha256=HMAC(body, secret)`.

```go
app.Post("/demo/signature",
    signature.New(signature.Config{
        Secret:          []byte("hmac-demo-secret"),
        SignatureHeader: "X-Signature",
        Tolerance:       5 * time.Minute,
    }),
    func(c *fh.Ctx) error { return c.JSON(fh.Map{"message": "signature verified"}) },
)
```

```bash
# Compute HMAC-SHA256 signature
BODY='{"test":true}'
TIMESTAMP=$(date +%s)
SIG=$(echo -n "$TIMESTAMP.$BODY" | openssl dgst -sha256 -hmac 'hmac-demo-secret' | awk '{print $2}')

# Valid request
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-Signature: t=$TIMESTAMP,sig=$SIG" \
  -d "$BODY" \
  http://localhost:3000/demo/signature
# {"message":"signature verified"}

# Tampered body
curl -i -X POST \
  -H "Content-Type: application/json" \
  -H "X-Signature: t=$TIMESTAMP,sig=invalid" \
  -d '{"test":false}' \
  http://localhost:3000/demo/signature
# HTTP/1.1 401 Unauthorized
```

---

### 16. Nonce / Replay Protection

Rejects duplicate `X-Nonce` values within a 5-minute window.

```go
app.Post("/demo/replay",
    replay.New(replay.Config{Header: "X-Nonce", TTL: 5 * time.Minute}),
    func(c *fh.Ctx) error { return c.JSON(fh.Map{"message": "nonce accepted"}) },
)
```

```bash
# First use — accepted
curl -X POST -H 'X-Nonce: unique-nonce-1' http://localhost:3000/demo/replay
# {"message":"nonce accepted"}

# Same nonce again — rejected (409)
curl -i -X POST -H 'X-Nonce: unique-nonce-1' http://localhost:3000/demo/replay
# HTTP/1.1 409 Conflict
```

---

### 17. IP Whitelist

Only allows requests from `127.0.0.1` or `::1`.

```go
app.Get("/demo/ip-whitelist",
    ipwhitelist.New("127.0.0.1/32", "::1/128"),
    func(c *fh.Ctx) error { return c.JSON(fh.Map{"message": "ip allowed"}) },
)
```

```bash
# From localhost — allowed
curl http://localhost:3000/demo/ip-whitelist
# {"message":"ip allowed"}

# From another IP — forbidden
curl -i -H 'Host: example.com' --interface 0.0.0.0 http://localhost:3000/demo/ip-whitelist 2>/dev/null
# HTTP/1.1 403 Forbidden
```

---

### 18. Actor (Per-Key Serialization)

Serializes concurrent requests by a computed key (mutex per key).

```go
app.Post("/demo/actor",
    actor.New(actor.Config{
        Key: func(c *fh.Ctx) string { return "user:" + c.Get("X-User-ID") },
    }),
    func(c *fh.Ctx) error { return c.JSON(fh.Map{"message": "serialized by user", "user_id": c.Get("X-User-ID")}) },
)
```

```bash
curl -X POST -H 'X-User-ID: user-42' http://localhost:3000/demo/actor
# {"message":"serialized by user","user_id":"user-42"}

# Concurrent requests with same X-User-ID execute serially
# (demonstrated by sending two requests in parallel and observing sequential processing)
```

---

### 19. URL Rewriting

Rewrites `/old-api/*` to `/api/*` globally.

```go
app.Use(rewrite.New(
    rewrite.Rule{From: "/old-api/*path", To: "/api/*path"},
))
app.Get("/api/hello", func(c *fh.Ctx) error {
    return c.SendString("reached via rewrite")
})
```

```bash
curl http://localhost:3000/old-api/hello
# reached via rewrite
```

---

### 20. Idempotency + Reliability

Idempotent POST with response replay and conflict detection.

```go
app.Post("/demo/idempotency",
    idempotency.New(func(c *fh.Ctx) string { return c.Get(fh.HeaderIdempotencyKey) }),
    reliability.New(fh.ReliabilityPolicy{
        Enabled: true, RequireIdempotency: true, Journal: true,
        ReplayResponse: true, MaxReplayAge: 24 * time.Hour,
    }),
    func(c *fh.Ctx) error {
        return c.Status(fh.StatusCreated).JSON(fh.Map{
            "order_id": "ord_" + time.Now().Format("20060102150405"),
            "request_id": c.Locals("request_id"),
        })
    },
)
```

```bash
# First request — 201 Created
curl -i -X POST -H 'Idempotency-Key: order-001' -H 'Content-Type: application/json' -d '{}' http://localhost:3000/demo/idempotency
```

```
HTTP/1.1 201 Created
X-Request-ID: req_abc123...
```

```json
{"order_id":"ord_20260623010000","request_id":"req_abc123..."}
```

```bash
# Duplicate request with same key — replayed (same response, same request_id)
curl -i -X POST -H 'Idempotency-Key: order-001' -H 'Content-Type: application/json' -d '{}' http://localhost:3000/demo/idempotency
```

```
HTTP/1.1 201 Created
X-Request-ID: req_abc123...   (same as before!)
X-Idempotency-Replayed: true
```

```bash
# Same key, different body — conflict (409)
curl -i -X POST -H 'Idempotency-Key: order-001' -H 'Content-Type: application/json' -d '{"different":"payload"}' http://localhost:3000/demo/idempotency
```

```
HTTP/1.1 409 Conflict
```

---

### 21. Lifecycle Hooks

Runs hooks around request processing. Logs start/error/end events.

```go
app.Post("/demo/lifecycle",
    lifecycle.New(lifecycle.Hooks{
        OnRequestStart: func(c *fh.Ctx) { log.Printf("start %v", c.Locals("request_id")) },
        OnError:        func(c *fh.Ctx, err error) { log.Printf("error %v: %v", c.Locals("request_id"), err) },
        OnRequestEnd:   func(c *fh.Ctx) { log.Printf("end %v status=%d", c.Locals("request_id"), c.StatusCode()) },
    }),
    func(c *fh.Ctx) error { return c.JSON(fh.Map{"message": "lifecycle hooks executed"}) },
)
```

```bash
curl -X POST http://localhost:3000/demo/lifecycle
```

```json
{"message":"lifecycle hooks executed"}
```

Server logs:
```
lifecycle start request=req_abc...
lifecycle end request=req_abc... status=200
```

---

### 22. Workflow

Sequential pipeline: step_one → step_two → async job (step_three) → respond.

```go
wf := workflow.New("demo-workflow").
    Use("step_one", func(c *fh.Ctx) error { c.Locals("step1", "done"); return nil }).
    Use("step_two", func(c *fh.Ctx) error { c.Locals("step2", "done"); return nil }).
    Job("step_three", "demo.process").
    Use("respond", func(c *fh.Ctx) error {
        return c.JSON(fh.Map{"message": "workflow completed", "step1": c.Locals("step1"), "step2": c.Locals("step2"), "job_id": c.Locals("job_id")})
    })

app.Post("/demo/workflow", wf.Handler())
```

```bash
curl -X POST http://localhost:3000/demo/workflow
```

```json
{"job_id":"job_abc...","message":"workflow completed","step1":"done","step2":"done"}
```

Server also logs async job processing:
```
processing workflow job=job_abc... payload={"workflow":"demo-workflow","step":"step_three",...}
```

---

### 23. Circuit Breaker

Opens after 2 consecutive failures, resets after 10s.

```go
breaker := circuitbreaker.New(circuitbreaker.Config{
    FailureThreshold: 2, ResetAfter: 10 * time.Second,
    OnOpen: func(c *fh.Ctx) error {
        return fh.NewHTTPError(fh.StatusServiceUnavailable, "CIRCUIT_OPEN", "service temporarily unavailable")
    },
})
app.Get("/demo/circuit-breaker", breaker.Handler(), func(c *fh.Ctx) error {
    if c.Query("fail") == "true" {
        return fh.NewHTTPError(fh.StatusInternalServerError, "UPSTREAM_ERROR", "simulated failure")
    }
    return c.JSON(fh.Map{"message": "circuit closed"})
})
```

```bash
# Request 1: success
curl http://localhost:3000/demo/circuit-breaker
# {"message":"circuit closed"}

# Request 2: failure
curl 'http://localhost:3000/demo/circuit-breaker?fail=true'
# {"code":"UPSTREAM_ERROR","detail":"simulated failure","status":500,...}

# Request 3: failure (threshold reached — circuit opens)
curl 'http://localhost:3000/demo/circuit-breaker?fail=true'
# {"code":"UPSTREAM_ERROR","detail":"simulated failure","status":500,...}

# Request 4: circuit open — rejected immediately
curl -i http://localhost:3000/demo/circuit-breaker
# HTTP/1.1 503 Service Unavailable
# {"code":"CIRCUIT_OPEN","detail":"service temporarily unavailable",...}
```

---

### 24. Reverse Proxy

Proxies `/demo/proxy/*` to a configurable upstream.

```go
app.All("/demo/proxy/*", proxy.New(proxy.Config{
    Target: *upstream, StripPrefix: "/demo/proxy", Timeout: 5 * time.Second,
}))
```

```bash
# Requires an upstream server running on :4000 (use -upstream flag to change)
curl http://localhost:3000/demo/proxy/some/path
# Proxied to http://127.0.0.1:4000/some/path

# Without upstream — 502
curl -i http://localhost:3000/demo/proxy/test
# HTTP/1.1 502 Bad Gateway
```

---

### 25. Metrics (Prometheus)

Request counters exposed at `/metrics` (IP-whitelisted to loopback).

```go
requests := metrics.New()
app.Use(requests.Middleware())
app.Get("/metrics", ipwhitelist.New("127.0.0.1/32", "::1/128"), requests.Handler())
```

```bash
curl http://localhost:3000/metrics
```

```
# HELP fh_requests_total Total request count
# TYPE fh_requests_total counter
fh_requests_total{method="GET",path="/health",status="200"} 1
fh_requests_total{method="GET",path="/demo/request-id",status="200"} 2
...
```

---

### 26. Static File Serving

Serves files with ETag and caching.

```go
app.Get("/static/*", staticmw.New("./public", staticmw.Config{
    Root: "./public", Prefix: "/static/",
    ETag: true, LastModified: true, MaxAge: time.Hour,
}))
```

```bash
curl -i http://localhost:3000/static/test.json
```

```
HTTP/1.1 200 OK
ETag: "abc123..."
Last-Modified: Tue, 23 Jun 2026 00:00:00 GMT
Cache-Control: public, max-age=3600
Content-Type: application/json
```

```json
{"name":"hello"}
```

```bash
# Conditional request with ETag
curl -i -H 'If-None-Match: "abc123..."' http://localhost:3000/static/test.json
# HTTP/1.1 304 Not Modified
```

---

### 27. Named Routes & URL Generation

Routes can be named and URLs generated programmatically.

```go
app.Get("/hello/:name", func(c *fh.Ctx) error {
    return c.SendString("hello " + c.Param("name"))
}).Name("demo.hello")

app.Get("/named-route-example", func(c *fh.Ctx) error {
    url, _ := app.URL("demo.hello", map[string]string{"name": "world"})
    return c.JSON(fh.Map{"named_url": url})
})
```

```bash
curl http://localhost:3000/hello/world
# hello world

curl http://localhost:3000/named-route-example
# {"named_url":"/hello/world"}
```

---

### 28. Redirect

301/302 redirects and named-route redirects.

```go
app.Get("/old-home", func(c *fh.Ctx) error {
    return c.Redirect("/health")
})
app.Get("/go-hello", func(c *fh.Ctx) error {
    return c.RedirectTo("demo.hello", map[string]string{"name": "redirected"})
})
```

```bash
curl -i http://localhost:3000/old-home
# HTTP/1.1 302 Found
# Location: /health

curl -i http://localhost:3000/go-hello
# HTTP/1.1 302 Found
# Location: /hello/redirected
```

---

### 29. Path & Query Parameters

```go
app.Get("/demo/params/:id", func(c *fh.Ctx) error {
    return c.JSON(fh.Map{"id": c.Param("id"), "query": c.Query("filter")})
})
```

```bash
curl 'http://localhost:3000/demo/params/42?filter=active'
```

```json
{"id":"42","query":"active"}
```

---

### 30. Content Negotiation (Codecs)

`c.BodyParser` automatically selects the parser based on `Content-Type`.
Supports JSON, XML, form, multipart, CSV, NDJSON, text, binary.

```go
app.Post("/demo/codecs", func(c *fh.Ctx) error {
    var p map[string]any
    if err := c.BodyParser(&p); err != nil {
        return c.Status(400).SendString("BodyParser error: " + err.Error())
    }
    return c.JSON(fh.Map{"echo": p, "content_type": c.Get("Content-Type")})
})
```

```bash
# JSON
curl -X POST -d '{"name":"Alice","age":30}' -H 'Content-Type: application/json' http://localhost:3000/demo/codecs
```

```json
{"echo":{"age":30,"name":"Alice"},"content_type":"application/json"}
```

```bash
# Form URL-encoded
curl -X POST -d 'name=Bob&role=admin' -H 'Content-Type: application/x-www-form-urlencoded' http://localhost:3000/demo/codecs
```

```json
{"echo":{"name":"Bob","role":"admin"},"content_type":"application/x-www-form-urlencoded"}
```

```bash
# XML
curl -X POST -d '<root><name>Carol</name></root>' -H 'Content-Type: application/xml' http://localhost:3000/demo/codecs
```

```json
{"echo":{"name":"Carol"},"content_type":"application/xml"}
```

```bash
# Multipart form
curl -X POST -F 'name=Dave' -F 'age=28' http://localhost:3000/demo/codecs
```

```json
{"echo":{"age":"28","name":"Dave"},"content_type":"multipart/form-data; boundary=..."}
```

---

### 31. Error Handling

Return structured HTTP errors with `fh.NewHTTPError`.

```go
app.Get("/demo/error", func(c *fh.Ctx) error {
    return fh.NewHTTPError(fh.StatusUnprocessableEntity, "DEMO_ERROR", "this is a demonstration error")
})
```

```bash
curl -i http://localhost:3000/demo/error
```

```
HTTP/1.1 422 Unprocessable Entity
Content-Type: application/json
```

```json
{"code":"DEMO_ERROR","detail":"this is a demonstration error","status":422,"title":"Unprocessable Entity","type":"about:blank"}
```

---

### 32. Panic Recovery

Catches panics and returns a controlled 500 response instead of crashing.

```go
app.Get("/demo/panic", func(c *fh.Ctx) error {
    panic("simulated panic for recover middleware demo")
})
```

```bash
curl -i http://localhost:3000/demo/panic
```

```
HTTP/1.1 500 Internal Server Error
```

```json
{"code":"INTERNAL_ERROR","detail":"An internal server error occurred","status":500}
```

Server logs the full stack trace.

---

### 33. Durable Queue

Enqueue async jobs, workers process them with retries and backoff.

```go
app.Queue().Register("demo.process", func(ctx context.Context, job *fh.QueueJob) error {
    log.Printf("processing job=%s payload=%s", job.ID, job.Payload)
    return nil
})

app.Post("/demo/queue", func(c *fh.Ctx) error {
    id, err := app.Queue().Enqueue("demo.process", fh.Map{"source": "queue_demo"})
    if err != nil { return err }
    return c.Status(fh.StatusAccepted).JSON(fh.Map{"job_id": id, "status": "queued"})
})

app.Get("/demo/queue/stats", func(c *fh.Ctx) error {
    st, _ := app.Queue().Stats()
    return c.JSON(st)
})
```

```bash
# Enqueue a job
curl -X POST http://localhost:3000/demo/queue
```

```json
{"job_id":"job_abc...","status":"queued"}
```

```
# Worker processes it (server log):
processing workflow job=job_abc... payload={"source":"queue_demo"}
```

```bash
# Queue statistics
curl http://localhost:3000/demo/queue/stats
```

```json
{"done":1,"failed":0,"pending":0,"processing":0}
```

---

### 34. Request Journal

Every request is journaled to `.fh-data/request-journal.jsonl` with
received/completed events, body hashes, and timing.

```go
app.Get("/demo/journal", func(c *fh.Ctx) error {
    return c.JSON(fh.Map{"message": "every request is journaled", "request_id": c.Locals("request_id")})
})
```

```bash
curl http://localhost:3000/demo/journal
# {"message":"every request is journaled","request_id":"req_abc..."}

# View the journal
cat .fh-data/request-journal.jsonl | head -2
# {"request_id":"req_abc...","event":"received","method":"GET","path":"/demo/journal","body_hash":"...","time":"..."}
# {"request_id":"req_abc...","event":"completed","method":"GET","path":"/demo/journal","status":200, ...}
```

---

### 35. CORS

Global CORS middleware allows cross-origin requests from configured origins.

```bash
# Preflight
curl -i -X OPTIONS \
  -H 'Origin: http://localhost:5173' \
  -H 'Access-Control-Request-Method: POST' \
  http://localhost:3000/demo/cors
```

```
HTTP/1.1 204 No Content
Access-Control-Allow-Origin: http://localhost:5173
Access-Control-Allow-Methods: GET, POST, PUT, PATCH, DELETE, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization, X-API-Key, ...
Access-Control-Max-Age: 600
```

```bash
# Actual request
curl -i -H 'Origin: http://localhost:5173' http://localhost:3000/demo/cors
```

```
HTTP/1.1 200 OK
Access-Control-Allow-Origin: http://localhost:5173
```

---

### 36. Outbox Pattern

Publish events reliably after business logic completes.

```go
app.Post("/demo/outbox", func(c *fh.Ctx) error {
    id, _ := app.Outbox().Publish(context.Background(), fh.OutboxEvent{
        Topic: "order.placed", Key: "order-42",
        Payload: []byte(`{"order_id":"order-42"}`),
    })
    return c.Status(fh.StatusAccepted).JSON(fh.Map{"event_id": id})
})
```

```bash
curl -X POST http://localhost:3000/demo/outbox
```

```json
{"event_id":"job_abc..."}
```

---

### 37. Inbox Pattern

Deduplicate external webhook events using idempotency key.

```go
app.Post("/demo/inbox", func(c *fh.Ctx) error {
    id, _ := app.Inbox().Accept(context.Background(), fh.InboxEvent{
        Source: "stripe", EventID: c.Get("X-Webhook-ID"), Payload: c.Body(),
    }, "")
    return c.Status(fh.StatusAccepted).JSON(fh.Map{"event_id": id})
})
```

```bash
# First delivery — accepted
curl -X POST -H 'X-Webhook-ID: evt_123' -H 'Content-Type: application/json' -d '{}' http://localhost:3000/demo/inbox
# {"event_id":"job_abc..."}

# Duplicate delivery (same event ID) — silently deduplicated
curl -i -X POST -H 'X-Webhook-ID: evt_123' -H 'Content-Type: application/json' -d '{}' http://localhost:3000/demo/inbox
# HTTP/1.1 202 Accepted (no job enqueued)
```

---

## Middleware / Feature Matrix

| # | Middleware / Feature | Type | Route |
|---|---------------------|------|-------|
| 1 | Health Check | Core | `GET /health` |
| 2 | Request ID | Global | `GET /demo/request-id` |
| 3 | Correlation ID | Global | `GET /demo/request-id` |
| 4 | Security Headers | Global | `GET /demo/security-headers` |
| 5 | Basic Auth | Per-route | `GET /demo/basic-auth` |
| 6 | API Key Auth | Per-route | `GET /demo/api-key` |
| 7 | API Versioning | Per-route | `GET /demo/api-version` |
| 8 | Route Policy | Per-route | `POST /demo/policy` |
| 9 | Body Limit | Per-route | `POST /demo/body-limit` |
| 10 | Request Timeout | Per-route | `GET /demo/timeout` |
| 11 | Rate Limiter | Per-route | `GET /demo/rate-limit` |
| 12 | Response Cache | Per-route | `GET /demo/cache` |
| 13 | Gzip Compression | Global | `GET /demo/compress` |
| 14 | CSRF Protection | Per-route | `GET /demo/csrf-token`, `POST /demo/csrf-submit` |
| 15 | Request Contract | Per-route | `POST /demo/contract` |
| 16 | HMAC Signature | Per-route | `POST /demo/signature` |
| 17 | Nonce Replay | Per-route | `POST /demo/replay` |
| 18 | IP Whitelist | Per-route | `GET /demo/ip-whitelist` |
| 19 | Actor Serialization | Per-route | `POST /demo/actor` |
| 20 | URL Rewriting | Global | `GET /old-api/hello` |
| 21 | Idempotency | Per-route | `POST /demo/idempotency` |
| 22 | Lifecycle Hooks | Per-route | `POST /demo/lifecycle` |
| 23 | Workflow | Per-route | `POST /demo/workflow` |
| 24 | Circuit Breaker | Per-route | `GET /demo/circuit-breaker` |
| 25 | Reverse Proxy | Per-route | `/demo/proxy/*` |
| 26 | Metrics | Global | `GET /metrics` |
| 27 | Static Files | Per-route | `GET /static/*` |
| 28 | Named Routes | Core | `GET /hello/:name` |
| 29 | Redirect | Core | `GET /old-home`, `GET /go-hello` |
| 30 | Path/Query Params | Core | `GET /demo/params/:id` |
| 31 | Codecs | Core | `POST /demo/codecs` |
| 32 | Error Handling | Core | `GET /demo/error` |
| 33 | Panic Recovery | Global | `GET /demo/panic` |
| 34 | Durable Queue | Core | `POST /demo/queue` |
| 35 | Request Journal | Global | `GET /demo/journal` |
| 36 | CORS | Global | `GET /demo/cors` |
| 37 | Outbox Pattern | Core | `POST /demo/outbox` |
| 38 | Inbox Pattern | Core | `POST /demo/inbox` |
