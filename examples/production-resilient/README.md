# Production-Resilient Showcase Service

This example provides an end-to-end, battle-tested demonstration of **`fh` running in tough, unforgiving production environments**.

It strictly adheres to **zero third-party dependencies** (Go standard library + core `github.com/oarkflow/fh` only) and implements an enterprise-grade defense-in-depth pipeline capable of surviving hostile ingress, downstream outages, burst traffic, and unhandled application panics.

---

## Architecture & Defense-in-Depth Pipeline

```text
 Client Traffic
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│ fh.NewProduction() (Hardened Deadlines, Socket Guard, Bounds)  │
└─────────────────────────────────────────────────────────────────┘
      │
      ├─► [1] Panic Recovery (mw/recover) ── Catch panics, return JSON 500
      ├─► [2] Security Headers (mw/security) ── HSTS, X-Frame-Options: DENY, nosniff
      ├─► [3] High-Entropy Request ID (mw/requestid) ── X-Request-ID propagation
      ├─► [4] In-Memory Favicon (mw/favicon) ── ETag caching & 304 fast path
      ├─► [5] Trailing Slash (mw/trailingslash) ── Zero-roundtrip URL normalization
      ├─► [6] Body Limit Guard (mw/bodylimit) ── Rejects >1 MiB with HTTP 413
      ├─► [7] Sliding Window Rate Limiter (mw/ratelimiter) ── Throttles with HTTP 429
      ├─► [8] RED Observability (mw/metrics) ── Latency distribution, Prometheus
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│ Route Handlers                                                  │
├─────────────────────────────────────────────────────────────────┤
│ • /health/live        ── K8s Liveness Probe                     │
│ • /health/ready       ── K8s Readiness (checks breaker & drain) │
│ • /metrics            ── Prometheus Metrics Scraper             │
│ • /api/v1/catalog     ── SingleFlight Coalescing (mw/coalesce) │
│ • /api/v1/orders      ── Transactional JSON Order Submission    │
│ • /api/v1/external    ── Guarded by Circuit Breaker & Timeout  │
│ • /api/v1/panic       ── Demonstrates Zero-Downtime Recovery    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Production Capabilities Demonstrated

1. **Hostile Ingress Protection**:
   - **Ingress Body Limit (`mw/bodylimit`)**: Enforces hard payload bounds to prevent memory exhaustion (OOM) attacks.
   - **Sliding-Window Rate Limiting (`mw/ratelimiter`)**: Throttles abusive callers per client IP, attaching standard `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `Retry-After` headers.
   - **Security Headers (`mw/security`)**: Sets `Strict-Transport-Security`, `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, and secure `Permissions-Policy`.
   - **URL Normalization (`mw/trailingslash`)**: Normalizes routes via internal in-flight rewrites (`c.Rewrite`) without extra network round-trips.

2. **Downstream Fault Tolerance & Self-Healing**:
   - **Circuit Breaker (`mw/circuitbreaker`)**: Wraps unreliable external dependencies. Automatically trips to `StateOpen` on consecutive failures, shedding load with HTTP `503 Service Unavailable` (`CIRCUIT_OPEN`) to prevent cascading failures.
   - **Canary Probe Recovery**: Transitions to `StateHalfOpen` after the reset window, testing the upstream dependency before safely re-closing the circuit.
   - **Cooperative Timeout (`mw/timeout`)**: Derives per-request deadline contexts to prevent hanging goroutines from consuming worker capacity.

3. **Thundering Herd Shield (SingleFlight)**:
   - **Request Coalescing (`mw/coalesce`)**: When 100 concurrent requests hit an expensive read endpoint (`/api/v1/catalog`), only **1** request executes the backend query. All other 99 callers receive the identical response simultaneously.

4. **Zero-Crash Resilience**:
   - **Panic Recovery (`mw/recover`)**: Any unhandled panic in business logic is intercepted, logged with a stack trace, and returned as a safe HTTP 500 without crashing the worker process or terminating active TCP connections.

5. **Kubernetes Readiness & Observability**:
   - **`/health/live`**: Kubernetes liveness probe detecting process deadlock.
   - **`/health/ready`**: Kubernetes readiness probe that returns HTTP `503 Service Unavailable` if the application is draining connections or if critical dependencies/breakers are degraded.
   - **`/metrics`**: OpenMetrics / Prometheus exporter exposing real-time request rates, status codes, in-flight gauges, and latency histogram buckets.
   - **Zero-Downtime Graceful Shutdown**: Traps `SIGINT`/`SIGTERM` and executes `app.ShutdownWithContext()` allowing in-flight transactions to drain cleanly.

---

## Quickstart

### 1. Run the Server

From the repository root:

```bash
go run ./examples/production-resilient
```

### 2. Verify Operational Endpoints

```bash
# 1. Check Liveness & Readiness
curl -i http://localhost:8080/health/live
curl -i http://localhost:8080/health/ready

# 2. Inspect Prometheus Metrics
curl -s http://localhost:8080/metrics?format=prometheus

# 3. Test In-Memory Favicon (returns 200 with ETag)
curl -i http://localhost:8080/favicon.ico

# 4. Create an Order (POST with JSON schema validation)
curl -i -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: req-tx-88124" \
  -d '{"item":"enterprise-sku","quantity":2,"amount":499.00}'
```

### 3. Test Defense & Chaos Scenarios

```bash
# Test A: Trigger Panic Recovery (returns 500, server stays alive)
curl -i http://localhost:8080/api/v1/panic

# Verify server immediately handles subsequent requests:
curl -i http://localhost:8080/health/live

# Test B: Ingress Body Limit (exceeding 1MB returns 413)
curl -i -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  --data-binary @/dev/urandom

# Test C: Test Circuit Breaker Tripping
# Inject upstream fault:
curl -X POST http://localhost:8080/api/v1/fault-injection -d '{"fault":true}'

# Trigger consecutive failures:
curl -i http://localhost:8080/api/v1/external-service
curl -i http://localhost:8080/api/v1/external-service
curl -i http://localhost:8080/api/v1/external-service

# Next request is instantly rejected with 503 CIRCUIT_OPEN:
curl -i http://localhost:8080/api/v1/external-service

# Readiness probe automatically reflects degraded state:
curl -i http://localhost:8080/health/ready

# Restore upstream:
curl -X POST http://localhost:8080/api/v1/fault-injection -d '{"fault":false}'

# After reset window (1s), canary probe heals circuit back to 200 OK:
curl -i http://localhost:8080/api/v1/external-service
```

---

## Automated Verification & Benchmarks

Run the all-in-one live production verification shell script:

```bash
# Compiles, boots, tests all chaos/defense scenarios, runs tests and benchmarks
./examples/production-resilient/check.sh
```

Or run the Go integration tests and benchmarks individually:

```bash
go test -v ./examples/production-resilient
go test -bench=BenchmarkResilient -benchtime=3s ./examples/production-resilient
```
