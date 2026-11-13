# Production Readiness

fh is production-oriented but pre-v1. Production readiness is a property of a
specific release, application configuration and deployment environment—not of
the framework name alone. Complete the gates below before serving public or
sensitive traffic.

## `NewProduction()` and `SecureByDefault` are narrower than they sound

This is the single most common misconfiguration risk in fh, so it gets its
own section instead of a passing mention: **neither `fh.NewProduction()` nor
`fh.WithSecureByDefault(true)` — together or separately — adds
authentication, authorization, CSRF protection, rate limiting, or a Host
allow-list.** Both are real, but they only cover protocol/transport hygiene:

| | `NewProduction()` (`ModeProduction`) | `WithSecureByDefault(true)` |
|---|---|---|
| Stricter protocol validation | ✅ | ✅ |
| Read/write/body/HTTP-2-idle timeout floors | ✅ (flagged by `ValidateSecurity` if unset) | ✅ (sets bounds) |
| Connection/body-size ceilings | partial | ✅ (sets bounds) |
| Hardened response headers (HSTS, COOP/CORP, `Referrer-Policy`) | ✗ | ✅ |
| h2c (cleartext HTTP/2) disabled | ✗ (flagged if left on) | ✅ |
| Authentication / authorization | ✗ — always your middleware | ✗ — always your middleware |
| CSRF protection | ✗ — mount `mw/csrf` | ✗ — mount `mw/csrf` |
| Rate limiting | ✗ — mount `mw/ratelimiter` | ✗ — mount `mw/ratelimiter` |
| Host allow-list | ✗ — set `Config.AllowedHosts` | ✗ — set `Config.AllowedHosts` |

Call `app.ValidateSecurity()` (or `GET /_fh/compliance/findings` once
compliance endpoints are mounted with auth) on any app running in a
production-like mode or with `SecureByDefault` — it always returns a
`SECURE_DEFAULTS_SCOPE` finding restating this, plus a `HOST_POLICY_MISSING`
finding if `Config.AllowedHosts` is empty. The startup banner prints the same
reminder at every process boot (`StartupBannerConfig.HideSecureDefaultsNotice`
silences it once your team doesn't need reminding). `ValidateSecurity` cannot
detect whether auth/CSRF/rate-limiting middleware is actually mounted — a
`HandlerFunc` gives it nothing to introspect — which is exactly why the
reminder is unconditional rather than a "detected" claim.

## Supported deployment shape

### Linux TCP vulnerability CVE-2026-98432

CVE-2026-98432 is a use-after-free in the Linux kernel's IPv4 TCP handshake.
The fix is to install a kernel package containing the fix from your Linux
vendor and boot into that kernel. Updating Go or fh alone does not fix it.

Go 1.24 and later enable Multipath TCP (MPTCP) on Linux listeners by default.
fh now explicitly disables MPTCP on TCP listeners created by its `Listen*`
methods, including the standard kernel fallback. The raw Linux kernel
transport already creates ordinary TCP sockets. This reduces exposure to the
published MPTCP-based exploit while the host kernel is being patched; the
underlying bug may also be exploitable on ordinary TCP listeners.

Applications passing their own listener to `Serve`, `ServeTLS`, or
`ServeContext` must configure that listener separately. For Go-created
listeners, call `net.ListenConfig.SetMultipathTCP(false)` before `Listen`, or
set `GODEBUG=multipathtcp=0` for the application process. A `godebug` directive
in this repository's `go.mod` affects only binaries built from this module;
it does not propagate to applications importing fh. Explicit per-listener
MPTCP settings can override `GODEBUG`. Check every listener in the deployed
process and follow your Linux vendor's advisory for kernel patch status.

References: [Linux kernel CVE record](https://www.cve.org/CVERecord?id=CVE-2026-98432),
[Go security team advisory, mirrored on oss-security](https://seclists.org/oss-sec/2026/q4/437),
[Go MPTCP defaults](https://go.dev/doc/godebug#go-124).

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

## Opt-in deployment gate and request-body budget

The [hardened server example](../examples/hardened-server/README.md) is a
runnable direct-origin and trusted-edge setup, with tested startup validation
and public probes that omit internal health-check errors.

For a public direct origin, configure `WithDeploymentSecurityGate` with
`DeploymentDirectOrigin`; use `DeploymentTrustedEdge` and explicit
`TrustedProxyCIDRs` behind an edge. Set `ExpectedReplicas` to the deployed
replica count. When it exceeds one, confirm shared queue, idempotency, replay,
and session stores with the corresponding `Shared*` fields. Declare replay
and session usage with `UsesReplay` and `UsesSessions`, since middleware is
opaque to the app. A failed gate
returns a startup error listing the missing controls. Direct mode accepts the
fh TLS serving entrypoints (`ServeTLS`, `ListenTLS*`, and `ListenAutoTLS*`); an arbitrary
caller-wrapped TLS listener is not identifiable to the gate.
For multiple replicas the file queue and file idempotency store are rejected.
A custom queue must also implement `MultiReplicaQueueStorage`, attest to atomic
claims and safe recovery, and implement `QueueLeaseRenewer` with a positive TTL.
When keyed concurrency is enabled, it must also implement
`MultiReplicaQueueKeyConcurrencyStorage`, `QueueConcurrencyLeaseStorage`, and
`QueueJobDeferrer`. The SQL queue advertises these capabilities for PostgreSQL
and MySQL after `Migrate` has created its lease tables and columns. Configure
`sqlstore.Config.QueueLeaseTTL` longer than the worst expected scheduler or
database interruption, with margin. MySQL queue tables must use InnoDB; the
adapter creates them with InnoDB and rejects existing queue tables using a
non-transactional engine. SQLite intentionally does not pass
the multi-replica gate. On a rolling upgrade from the older SQL queue, stop all
old queue workers before migrating and restarting them: old binaries do not
renew or check fences or concurrency-key leases. The database fence prevents
stale workers from changing queue state, but applications must still make
handler side effects idempotent or enforce the fence in their own transactional
writes.
`QueueConcurrencyLimitByKey` is process-local unless the storage adapter
implements `MultiReplicaQueueKeyConcurrencyStorage` and `QueueJobDeferrer`; the
deployment gate rejects multi-replica use of that option without both
capabilities.
Run `contracttest.RunQueue` and `contracttest.RunIdempotency` from the
adapter's integration tests against its real shared backend. The fixtures
exercise independent handles, concurrent claims, lease expiry, restart, and
backend outages; a passing in-memory mock is not sufficient evidence for a
multi-replica deployment.

```go
app := fh.New(
    fh.WithSecureByDefault(true),
    fh.WithAllowedHosts("api.example.com"),
    fh.WithMaxBufferedRequestBytes(64<<20),
    fh.WithDeploymentSecurityGate(fh.DeploymentSecurityConfig{
        Mode: fh.DeploymentDirectOrigin,
        ExpectedReplicas: 1,
    }),
)
```

The gate verifies local configuration and operator declarations. It cannot
verify external firewall rules, actual proxy placement, or authentication
inside arbitrary application middleware. In trusted-edge mode, fh rejects
connections whose raw socket peer is outside `TrustedProxyCIDRs`; configure
the same CIDRs in `mw/realip` or `ProxyProtocolConfig.TrustedPeers` before
trusting forwarded client identity.
PROXY negotiation admits up to 256 pending handshakes by default, bounds
prefaces to 4096 bytes, and drops bad peers individually. Tune
`MaxPendingHandshakes` and `MaxHeaderBytes` for the trusted edge's connection
pool and TLV usage.

`WithMaxBufferedRequestBytes` configures a process-wide budget for request
bodies buffered in memory by HTTP/1 and HTTP/2. `SecureByDefault` uses a 64 MiB
budget unless a stricter value is supplied. Exceeding the budget rejects the
request or resets its stream. Unread streaming frames use transport flow
control; bytes retained after a stream is consumed and middleware-produced
replacement bodies, including decompressed payloads, count against this
budget. Application allocations outside the request-body pipeline remain
application-owned. Size the budget for expected concurrent uploads.

`WithMaxBufferedResponseBytes` configures the process-wide budget for
intermediate response bodies retained by body transforms and buffered response
streams. `SecureByDefault` uses 64 MiB unless a stricter value is supplied.
Direct response values created and retained by handlers are application-owned
allocations and are not covered by this budget.

Build and run the framework with Go 1.27.4 or later. The module pins this
minimum because earlier Go 1.27 patches predate the November 2026 standard
library and toolchain security fixes.

Run the example and focused hardening checks with:

```sh
go test ./examples/hardened-server ./mw/proxy
go test -race . -run 'Test(HealthProbe|ShutdownHook|StartupReady|DeploymentSecurityGate|ProxyProtocol|.*BufferedBodyBudget|ChunkedBodyBudget)'
```

Context-aware cleanup can be registered with `OnShutdownContext`. The
`ShutdownWithContext` deadline covers draining and hooks, returning the
deadline error if cleanup is still running. Go cannot forcibly terminate a
callback that ignores context cancellation; such callbacks may continue after
the call returns.

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

[`.gitlab-ci.yml`](../.gitlab-ci.yml) automates this: a `lint` stage
(`gofmt`, `go vet`, `staticcheck`, `govulncheck`), a `test` stage matrixed
across the declared Go version and current Go on Linux/macOS/Windows, a
`fuzz` stage running every `Fuzz*` target with a short bounded fuzztime, and
a tag-triggered `release-gate` stage that reruns the full set (including a
longer fuzz budget) before a release. It assumes self-hosted runners tagged
`linux`, `macos` and `windows`; adjust the `tags:` on each job if your
runner fleet differs — it has not been validated against a specific fleet.

Also require:

1. compile checks for every documentation example — see
   [`doc_examples_test.go`](../doc_examples_test.go), run as part of
   `go test ./...` and therefore covered by the `test` stage above;
2. short continuous fuzz runs for HTTP/1 framing, chunked encoding, HPACK,
   HTTP/2 frames, WebSocket frames and security-envelope parsers — covered by
   the `fuzz`/`release-gate` stages, which discover and run every `Fuzz*`
   target in the module;
3. dependency and reachable-vulnerability review — `govulncheck`, in both the
   `lint` and `release-gate` stages;
4. API compatibility checks against the previous stable release — the Linux
   release gate runs `cmd/apicheck` when an earlier `v1+` tag exists. The first
   stable tag establishes the baseline. Run
   `make api-check BASELINE_REF=v1.0.0` locally for a specific baseline;
   review any reported changes because the source-level check is conservative;
5. signed, reproducible release artifacts and an SBOM where policy
   requires it — after the GitLab release gate passes, manually dispatch
   `.github/workflows/release.yml` from the default branch with the release
   tag. Manually dispatch `.github/workflows/mira-release-v7.yml` from the
   default branch with the MIRA release tag when publishing MIRA materials.
   Both workflows validate the tag before building, keep
   source compilation and packaging in a read-only job, and grant release and
   attestation permissions only to a separate publishing job. The publisher
   verifies provenance and the SBOM attestation, then confirms the remote tag
   still resolves to the validated commit before publication. It packages the
   tagged source as a reproducible archive, creates an SPDX SBOM and SHA-256
   manifest, and publishes the attested files. Neither workflow has yet been
   executed in this repository; verify its output and attestation results
   before relying on it. The workflow does not establish that the tag itself
   was signed by an authorized key; enforce tag protection and release
   authorization in repository policy.

The external production gates remain open: independent MIRA cryptographic
review and penetration test, real WAN/path-MTU and long-soak evidence, live
multi-node PostgreSQL failover validation, and operational failover/rollback
drills. The evidence and acceptance criteria are in
[`operations-drills.md`](operations-drills.md). Do not mark these complete
from source inspection or simulated tests.

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
