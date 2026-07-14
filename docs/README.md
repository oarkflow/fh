# fh

**fh** is a standalone, zero-dependency Go web framework that implements HTTP/1.1, HTTP/2, and WebSocket protocols entirely from scratch using only the Go standard library. It is not a wrapper around `net/http` or `fasthttp` — it has its own TCP server, HTTP parser, trie-based router, HTTP/2 framing engine, HPACK encoder/decoder, and WebSocket implementation.

**Module:** `github.com/oarkflow/fh`

---

## Documentation Index

| Document | Description |
|----------|-------------|
| [Getting Started](getting-started.md) | Installation, quick start, basic concepts |
| [Configuration](configuration.md) | Full configuration reference (App, Codec, Reliability, Static) |
| [Routing](routing.md) | Router, route groups, parameters, named routes, route listing |
| [Request & Response](response.md) | All request/response methods and types |
| [Codecs](codecs.md) | Body parsing and codec system (JSON, XML, Form, Multipart, CSV, NDJSON, Text, Binary) |
| [Static Files](static-files.md) | Static file serving configuration |
| [Middleware](middleware.md) | All 34 built-in middleware packages with descriptions and recommended order |
| [Reliability Layer](reliability.md) | Request journaling, idempotency, durable queue, outbox/inbox, DLQ |
| [HTTP/2](http2.md) | HTTP/2 ALPN, h2c, flow control, HPACK |
| [WebSocket](websocket.md) | RFC 6455 WebSocket, EventHub pub/sub |
| [Native Features](native-features.md) | Typed endpoints, OpenAPI 3.1, SSE, security helpers |
| [Secure WASM Transport](secure-wasm-transport.md) | Device-bound encrypted Fetch transport for FH |

---

## Key Features

- **Zero external dependencies** — only Go standard library
- **HTTP/1.1** — Keep-alive, pipelining, chunked transfer, trailers, Expect: 100-continue
- **HTTP/2** — TLS ALPN, h2c prior knowledge, h2c upgrade, full framing, flow control, HPACK
- **WebSocket (RFC 6455)** — Low-level Conn + high-level EventHub pub/sub with rooms, topics, auth
- **Radix Tree Router** — Compressed trie with named params (`:param`), wildcards (`*wild`), named routes
- **Route Groups** — Shared prefix and middleware inheritance, nested groups
- **Pluggable Codec System** — JSON, XML, Form, Multipart, CSV, NDJSON, Text, Binary + custom codecs
- **34 Built-in Middleware** — Security, CORS, rate limiting, caching, logging, metrics, sessions, CSRF, etc.
- **Typed Endpoints** — Generic `PostTyped[T, U]` with automatic validation, struct binding, schema generation
- **OpenAPI 3.1** — Auto-generated spec from routes and typed endpoints
- **Reliability Layer** — Request journaling, idempotency keys, durable async queue, outbox/inbox, DLQ
- **Static File Serving** — Directory listing, compression, cache, range requests, ETag, conditional requests
- **Streaming** — Chunked response streaming, SSE (Server-Sent Events)
- **Graceful Shutdown** — Drain mode, connection tracking, configurable timeout
- **TLS** — `ListenTLS` with automatic ALPN negotiation
- **Server-Sent Events** — Native SSE with `Event()` and `Comment()` methods
- **Pluggable Storage** — Custom backends for reliability (journal, idempotency, queue)
- **Pluggable Template Engine** — `TemplateEngine` interface for any template library
