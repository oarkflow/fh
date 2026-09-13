# Production Readiness

fh is production-oriented but pre-v1. Production readiness is a property of a
specific release, application configuration and deployment environment—not of
the framework name alone. Complete the gates below before serving public or
sensitive traffic.

## Supported deployment shape

The best-supported shape is HTTP/1.1 or HTTP/2 behind a trusted CDN, WAF or
load balancer, with TLS either at fh or at that trusted edge. HTTP/3, gRPC and
GraphQL servers are not built in. Default rate-limit, cache, session, replay,
cluster and EventHub state is process-local unless a shared store is supplied.

Custom kernel transports, XDP and secure browser/WASM transport require the
additional platform and security gates described below.

## Application baseline

```go
app := fh.New(
    fh.WithSecureByDefault(true),
    fh.WithAllowedHosts("api.example.com"),
)
```

Then explicitly install the policies appropriate to the application:

- authentication and authorization;
- CSRF for cookie-authenticated browser endpoints;
- exact CORS origins;
- trusted proxy CIDRs before consuming forwarded client identity;
- global and identity/tenant/route-specific rate limits;
- tighter body, timeout and concurrency limits for expensive endpoints;
- shared stores for all state that must be consistent across replicas.

Use TLS 1.3 at fh or a trusted edge. `SecureByDefault` disables h2c, adds
strict parsing/resource ceilings and hardened response headers, but cannot
infer application policy or provision certificates.

## Startup validation

Annotate sensitive routes with `WithRouteSecurity` and fail deployment when
the framework detects a critical configuration finding:

```go
findings := app.ValidateSecurity()
for _, finding := range findings {
    log.Printf("security finding: severity=%s code=%s route=%s: %s",
        finding.Severity, finding.Code, finding.Route, finding.Message)
}
```

Enterprise/compliance endpoints reveal route and configuration details. Set
`WithComplianceEndpointAuth(...)`; without authentication they remain
unmounted and validation reports a critical finding. Compliance profiles help
collect evidence but do not certify an application.

## Release gates

Run these with the exact Go version declared by `go.mod`:

```bash
go test ./...
go test -race ./...
go build ./...
go vet ./...
staticcheck ./...
govulncheck ./...
```

Use versions of staticcheck and govulncheck that support the selected Go
toolchain. CI should run at least on Linux, macOS and Windows. Native runtime
testing is required; successful cross-compilation is not runtime validation.

Also require:

1. compile checks for every documentation example;
2. short continuous fuzz runs for HTTP/1 framing, chunked encoding, HPACK,
   HTTP/2 frames, WebSocket frames and security-envelope parsers;
3. dependency and reachable-vulnerability review;
4. API compatibility checks against the previous release;
5. signed, reproducible release artifacts and an SBOM where policy requires it.

## Load and failure testing

Before rollout, test with the real handlers, payloads and TLS configuration:

- p50/p95/p99/p99.9 latency, throughput, CPU, RSS and allocation behavior;
- 24–72 hour keep-alive and connection-churn soak;
- slow headers/bodies, half-closed sockets and aborted uploads;
- descriptor exhaustion, memory pressure and TLS handshake floods;
- overload rejection and recovery at all configured resource limits;
- graceful shutdown/restart while HTTP/1, HTTP/2, SSE and WebSocket traffic is
  active;
- shared-store outage, latency, retry, failover and split-brain behavior;
- queue recovery, idempotency collisions, replay-store saturation and DLQ
  operation.

Canary every release and define rollback triggers before broad deployment.

## Kernel transport gates

`ProductionKernelConfig` is a balanced candidate, not a universal performance
guarantee. Validate the backend reported by `KernelRuntimeInfo` and
`KernelReadiness` on the actual kernel and workload. Test epoll/kqueue/runtime
native behavior on each target OS. Canary io_uring. Build, verifier-load,
attach and roll back XDP on the actual kernel, NIC and driver.

## Secure browser/WASM transport gates

The custom secure transport has bounded parsers and tests but has not received
an independent cryptographic audit. Do not use it for high-value financial,
healthcare or regulated data until that review is complete. Production builds
must embed trusted origin/key pins, enforce asset integrity, publish hashes
through an independently authenticated channel and use durable shared replay
and session stores. See [Secure WASM Transport](secure-wasm-transport.md).

## Known product limitations

- no HTTP/3/QUIC, gRPC server or GraphQL server;
- no built-in OTLP exporter;
- process-local defaults for distributed state;
- pre-v1 API compatibility;
- secure transport buffers request/response bodies and supports one active
  transport key per instance;
- browser Fetch cannot provide certificate pinning beyond normal PKI/TLS.

These are architectural constraints, not settings that `SecureByDefault` can
remove.
