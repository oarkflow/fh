# fh Documentation

**fh** is a standalone Go web framework that implements HTTP/1.1, HTTP/2, and WebSocket protocols entirely from scratch using only the Go standard library plus `golang.org/x/crypto` (OCSP stapling and ACME/`autocert`, both optional). It is not a wrapper around `net/http` or `fasthttp` — it has its own TCP server, HTTP parser, trie-based router, HTTP/2 framing engine, HPACK encoder/decoder, and WebSocket implementation.

**Module:** `github.com/oarkflow/fh` · **Go:** 1.26.5+

See the [root README](../README.md) for a quick tour. This folder is the full reference.

---

## Core

| Document | Description |
|---|---|
| [Getting Started](getting-started.md) | Installation, quick start, basic concepts |
| [Configuration](configuration.md) | Full `fh.Config` reference (timeouts, buffers, HTTP/2, codec, reliability, static) |
| [Routing](routing.md) | Router, route groups, parameters, named routes, route listing |
| [Request & Response](response.md) | All request/response methods and types |
| [Codecs](codecs.md) | Body parsing and codec system (JSON, XML, Form, Multipart, CSV, NDJSON, Text, Binary) |
| [Static Files](static-files.md) | Static file serving configuration |
| [Middleware](middleware.md) | Built-in middleware packages, ordering guidance, full package index |
| [Startup Banner](STARTUP_BANNER.md) | The ASCII startup banner shown on `Listen` |

## Protocols

| Document | Description |
|---|---|
| [HTTP/2](http2.md) | TLS ALPN, h2c prior knowledge, h2c upgrade, framing, flow control, HPACK |
| [WebSocket](websocket.md) | RFC 6455 WebSocket, low-level `Conn`, `EventHub` pub/sub |
| [HTTP Client](http-client.md) | Outbound `fh.NewClient` HTTP/1.1 + HTTP/2 client with resilience and security |

## Platform features

| Document | Description |
|---|---|
| [Native Features](native-features.md) | Typed endpoints, OpenAPI 3.1, SSE, security helpers, gateway/proxy, API versioning |
| [Error Framework](ERROR_FRAMEWORK.md) | RFC 9457 problem details, typed errors, panic recovery, redaction |
| [Reliability Layer](reliability.md) | Request journaling, idempotency, durable queue, outbox/inbox, DLQ, custom storage |
| [Security](security.md) | TLS/mTLS, read budgets, trusted-proxy identity, message integrity, HTTP QUERY |
| [Secure WASM Transport](secure-wasm-transport.md) | Device-bound encrypted Fetch transport for browser/WASM clients |
| [RFC 9421 Response Signatures](rfc9421-response-signatures.md) | Nonce-bound Ed25519 response integrity for Go and browser clients |
| [Merkle Audit](merkle_audit.md) | Tamper-evident audit checkpoints via Merkle tree |
| [SLO Tracking](slo.md) | Route-level availability/latency SLOs and burn-rate alerts |
| [Budgets](budget.md) | Hierarchical per-request execution budgets (time, memory, upstream, retries) |
| [Config Reload](configreload.md) | Atomic config/route/policy/certificate reload with generation tracking |
| [ACME / Let's Encrypt](acme.md) | Automatic TLS certificate issuance and renewal via TLS-ALPN-01 |

## Operations

| Document | Description |
|---|---|
| [Performance](performance.md) | Hot-path configuration and zero-allocation response APIs |
| [Linux Kernel Transport](kernel-transport.md) | epoll/io_uring reactors, SO_REUSEPORT steering, XDP operations and deployment |
| [Performance Profiles](performance_profiles.md) | Fast / Production / Enterprise runtime profiles |
| [Prefork & Zero-Downtime Restarts](prefork.md) | Multi-process `SO_REUSEPORT` prefork and `SIGHUP`-triggered rolling restarts |

---

## Key Features

- **Zero external dependencies** — only the Go standard library
- **HTTP/1.1** — keep-alive, pipelining, chunked transfer, trailers, `Expect: 100-continue`
- **HTTP/2** — TLS ALPN, h2c prior knowledge, h2c upgrade, full framing, flow control, HPACK
- **WebSocket (RFC 6455)** — low-level `Conn` + high-level `EventHub` pub/sub with rooms, topics, auth
- **Radix tree router** — compressed trie with named params (`:param`), wildcards (`*wild`), named routes
- **Route groups** — shared prefix and middleware inheritance, nested groups
- **Pluggable codec system** — JSON, XML, Form, Multipart, CSV, NDJSON, Text, Binary + custom codecs
- **65+ built-in middleware packages** — security, CORS, rate limiting, caching, logging, metrics, sessions, CSRF, and more (see [mw/](../mw/README.md))
- **Typed endpoints** — generic `PostTyped[T, U]`-style handlers with automatic validation, struct binding, schema generation
- **OpenAPI 3.1** — auto-generated spec from routes and typed endpoints
- **Reliability layer** — request journaling, idempotency keys, durable async queue, outbox/inbox, DLQ
- **Compliance layer** — Business/Professional/Enterprise/Security profiles, audit ledger, route security metadata
- **Static file serving** — directory listing, compression, cache control, range requests, ETag, conditional requests
- **Streaming** — chunked response streaming, Server-Sent Events
- **Graceful shutdown** — drain mode, connection tracking, configurable timeout
- **TLS / mTLS** — `ListenTLS` with automatic ALPN negotiation, verified peer state, atomic certificate reload
- **ACME / Let's Encrypt** — automatic certificate issuance and renewal via TLS-ALPN-01, no `net/http` dependency
- **Prefork & zero-downtime restarts** — multi-process `SO_REUSEPORT` supervisor with `SIGHUP`-triggered rolling restarts
- **Pluggable storage** — custom backends for reliability (journal, idempotency, queue) and clustering
- **Pluggable template engine** — `TemplateEngine` interface for any template library
