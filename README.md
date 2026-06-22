# fh — Zero-Dependency Go Web Framework

**fh** is a standalone, high-performance HTTP/1.1 + HTTP/2 + WebSocket web framework for Go with **zero external dependencies**. It implements HTTP parsing, routing, HTTP/2 framing, HPACK, and WebSocket protocols from scratch — no wrappers around `net/http` or `fasthttp`.

## Features

- **Zero dependencies** — only the Go standard library
- **HTTP/1.1** — full request/response parsing, chunked transfer, trailers
- **HTTP/2** — TLS ALPN, h2c prior knowledge, h2c upgrade, stream multiplexing, flow control
- **WebSocket** — RFC 6455 server implementation with `EventHub` pub/sub layer
- **Trie-based router** — radix tree with named (`:param`) and wildcard (`*wild`) parameters
- **Codec system** — pluggable body parsers for JSON, XML, form, multipart, CSV, NDJSON, text, binary
- **Middleware** — built-in: CORS, CSRF, rate limiter, session, cache, compress, logger, recover, security headers, basic auth, body limit, rewrite, timeout, IP whitelist, request ID, early data, skip
- **Template engine** — agnostic interface, any engine implementing `Render(w, name, data, layout...)`
- **Static file serving** — directory listings, compression, cache control, range requests
- **Graceful shutdown** — `app.Shutdown(ctx)` with context deadline
- **Pool-based zero-allocation** — sync.Pool for contexts, byte buffers, HPACK decoders

## Installation

```bash
go get github.com/oarkflow/fh
```

## Quick Start

```go
package main

import "github.com/oarkflow/fh"

func main() {
    app := fh.New()

    app.Get("/", func(c *fh.Ctx) error {
        return c.SendString("Hello, World!")
    })

    app.Listen(":8080")
}
```

## Routing

### HTTP Methods

```go
app.Get("/path", handler)
app.Post("/path", handler)
app.Put("/path", handler)
app.Delete("/path", handler)
app.Patch("/path", handler)
app.Head("/path", handler)
app.Options("/path", handler)

// Register all methods at once
app.All("/path", handler)

// Register with explicit method string
app.Add("GET", "/path", handler)
```

### Route Parameters

```go
app.Get("/users/:id", func(c *fh.Ctx) error {
    id := c.Params("id")
    return c.SendString("User: " + id)
})

app.Get("/files/*path", func(c *fh.Ctx) error {
    path := c.Params("path")
    return c.SendString("File: " + path)
})
```

### Named Routes

```go
app.Get("/users/:id", handler).Name("user.show")
// Redirect by route name
c.RedirectTo("user.show", fh.Map{"id": "42"})
```

### Route Groups

```go
api := app.Group("/api")
api.Get("/users", listUsers)
api.Post("/users", createUser)
api.Get("/users/:id", getUser)

// Nested groups with middleware
admin := api.Group("/admin", adminMiddleware)
admin.Get("/dashboard", dashboardHandler)
```

## Middleware

### Global Middleware

```go
app.Use(logger.New())
app.Use(recover.New())
app.Use(cors.New(cors.Config{
    AllowOrigins: []string{"https://example.com"},
}))
```

### Route-level Middleware

```go
app.Get("/dashboard", authMiddleware, dashboardHandler)

// Group middleware
admin := app.Group("/admin", authMiddleware, adminLogger)
admin.Get("/", adminHandler)
```

### Built-in Middleware

| Package | Description |
|---|---|
| `mw/basicauth` | HTTP Basic Authentication |
| `mw/bodylimit` | Request body size limit |
| `mw/cache` | Response caching with TTL |
| `mw/compress` | Gzip response compression |
| `mw/cors` | Cross-Origin Resource Sharing |
| `mw/csrf` | CSRF protection |
| `mw/earlydata` | TLS 1.3 Early Data (0-RTT) protection |
| `mw/ipwhitelist` | IP allowlisting |
| `mw/logger` | Request logging (common, combined, tiny, json, custom) |
| `mw/ratelimiter` | Rate limiting |
| `mw/recover` | Panic recovery |
| `mw/requestid` | Request ID injection |
| `mw/rewrite` | URL path rewriting |
| `mw/security` | Security headers (CSP, HSTS, XFO, etc.) |
| `mw/session` | Cookie-based sessions with HMAC signing |
| `mw/skip` | Conditional middleware skipping by path |
| `mw/timeout` | Request timeout |

## Body Parsing & Codecs

fh uses an interface-based codec system. `BodyParser` automatically selects the right codec based on `Content-Type`:

```go
var user User
if err := c.BodyParser(&user); err != nil {
    return err
}
```

### Built-in Codecs

| Content-Type | Codec |
|---|---|
| `application/json` | JSON |
| `application/xml`, `text/xml` | XML |
| `application/x-www-form-urlencoded` | Form |
| `multipart/form-data` | Multipart |
| `text/csv` | CSV |
| `application/x-ndjson` | NDJSON (newline-delimited JSON) |
| `text/plain` | Plain text |
| `application/octet-stream` | Binary |

### Custom Codecs

```go
type MsgPackCodec struct{}

func (c *MsgPackCodec) ContentTypes() []string {
    return []string{"application/msgpack"}
}

func (c *MsgPackCodec) Decode(data []byte, v interface{}) error {
    // decode msgpack into v
}

fh.RegisterCodec(&MsgPackCodec{})
```

## Responses

```go
c.SendString("text")
c.SendBytes([]byte("data"))
c.SendStream(reader)
c.JSON(data)
c.JSONPretty(data, "  ")
c.XML(data)
c.XMLPretty(data, "  ")
c.HTML(html)
c.SendFile("path/to/file.pdf")
c.Redirect("/new-path")
c.RedirectTo("route.name", fh.Map{"id": "42"})
c.RedirectBack()

// Set status code
c.Status(201).JSON(createdResource)
```

## Static File Serving

```go
// Serve a single directory
app.Static("/static", "./public")

// With configuration
app.StaticFS("/", fh.StaticConfig{
    Root:         "./public",
    Compress:     true,
    Browse:       true,
    IndexFiles:   []string{"index.html", "index.htm"},
    CacheControl: "public, max-age=3600",
})
```

## HTTP/2

fh supports three HTTP/2 modes:

### TLS + ALPN (standard)

```go
app.ListenTLS(":443", "cert.pem", "key.pem")
```

### h2c Prior Knowledge (cleartext)

```go
app.Listen(":8080") // implicit if client sends PRI * HTTP/2.0
```

### h2c Upgrade (from HTTP/1.1)

```go
// Client sends Upgrade: h2c, server responds with 101 Switching Protocols
// fh handles this automatically
```

## WebSocket

### Low-level Upgrade

```go
app.Get("/ws", func(c *fh.Ctx) error {
    conn, err := c.Upgrade()
    if err != nil {
        return err
    }
    // conn is a *websocket.Conn
    msg, err := conn.ReadMessage()
    conn.WriteMessage(msg)
})
```

### EventHub (pub/sub)

```go
import "github.com/oarkflow/fh/pkg/websocket"

hub := websocket.NewEventHub()

app.Get("/ws", func(c *fh.Ctx) error {
    conn, err := c.Upgrade()
    if err != nil {
        return err
    }
    hub.Serve(conn)
})

// Broadcast to a topic
hub.Publish("chat:general", "Hello everyone!")

// With auth and rooms
hub.SetAuth(func(ctx *websocket.Context) error {
    if ctx.Token != "valid-token" {
        return errors.New("unauthorized")
    }
    return nil
})

hub.OnConnect(func(ctx *websocket.Context) {
    ctx.Join("room:golang")
})
```

## Template Rendering

Any engine implementing the `TemplateEngine` interface:

```go
type TemplateEngine interface {
    Render(w io.Writer, name string, data interface{}, layout ...string) error
}
```

```go
app.SetTemplateEngine(myEngine)

app.Get("/page", func(c *fh.Ctx) error {
    return c.Render("index", fh.Map{
        "title": "Hello",
        "body":  "World",
    })
})

// With layout
c.Render("content", data, "layouts/main")
```

## Error Handling

```go
// Return typed HTTP errors
return fh.NotFound("User not found")
return fh.Unauthorized("Sign in required")
return fh.NewHTTPError(fh.StatusConflict, "USER_EXISTS", "User already exists")

// Custom server fallbacks and error responses
app := fh.New(fh.Config{
    ErrorHandler: func(c *fh.Ctx, err error) {
        _ = c.ErrorResponse(err)
    },
    NotFoundHandler: func(c *fh.Ctx) error {
        return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "missing"})
    },
    MethodNotAllowed: func(c *fh.Ctx, allowed []string) error {
        return c.Status(fh.StatusMethodNotAllowed).JSON(fh.Map{"allowed": allowed})
    },
    OptionsHandler: func(c *fh.Ctx, allowed []string) error {
        return c.SendStatus(fh.StatusNoContent)
    },
})
```

## Configuration

```go
app := fh.New(fh.Config{
    ReadTimeout:          10 * time.Second,
    WriteTimeout:         10 * time.Second,
    IdleTimeout:          120 * time.Second,
    ReadBufferSize:       4096,
    MaxConnections:       256 * 1000,
    MaxRequestBodySize:   4 * 1024 * 1024, // 4MB
    MaxHeaderListSize:    64 << 10,
    MaxConcurrentStreams: 128,
    DisableKeepAlive:     false,
    DisableHTTP2:         false,
    ErrorHandler:         customErrorHandler,
    NotFoundHandler:      customNotFoundHandler,
    MethodNotAllowed:     customMethodNotAllowedHandler,
    OptionsHandler:       customOptionsHandler,
    TemplateEngine:       myEngine,
    Logger:               logger,
    Debug:                false,
})
```

## HTTP/1.1 Features

- Request/response header parsing (zero-alloc byte comparisons)
- Chunked transfer encoding (RFC 9112)
- Trailers
- Keep-alive connection reuse
- `Expect: 100-continue`
- `Host` header validation
- `Content-Length` enforcement

## Sessions

```go
import "github.com/oarkflow/fh/mw/session"

store := session.NewMemoryStore()
smw := session.New(session.Config{
    Store: store,
    Secret: "your-256-bit-secret",
})

app.Use(smw.Middleware)

app.Get("/login", func(c *fh.Ctx) error {
    sess := session.Get(c)
    sess.Set("user_id", 42)
    sess.Set("role", "admin")
    return sess.Save()
})

app.Get("/profile", func(c *fh.Ctx) error {
    sess := session.Get(c)
    uid := sess.Get("user_id")
    return c.JSON(fh.Map{"user_id": uid})
})
```

Session stores: `MemoryStore` (default), `FileStore`. Custom stores implement `session.Store`.

## Graceful Shutdown

```go
app := fh.New()
go app.Listen(":8080")

quit := make(chan os.Signal, 1)
signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
<-quit

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
app.Shutdown(ctx)
```

---

# Technical Architecture

## Core Types

```
App — application container, route registry, lifecycle
Ctx — per-request context (request/response, params, locals)
Router — radix tree with parameter extraction
Codec — pluggable body codec interface
RequestHeader — zero-alloc HTTP/1.1 header parser
```

## Router Design

The router is a **compressed radix tree** (trie) where each node represents a path segment. Parameter nodes are prefixed with `:` and wildcard nodes with `*`.

- **Insert**: `O(n)` where n is path segment count. Paths are split on `/` and merged into existing nodes when possible.
- **Lookup**: `O(n)` with backtracking for parameter extraction. No reflection or regex.
- **Named routes**: Routes can be named via `.Name("name")` and looked up at runtime for redirect generation.

## Connection Lifecycle

1. `Accept()` — accept TCP connection
2. `serveConn()` — goroutine per connection
3. `readRequest()` — parse HTTP/1.1 or detect HTTP/2
4. Route dispatch — find handler via trie
5. Middleware chain — sequential handler execution
6. Response write — headers + body
7. Recycle — release `Ctx` to pool, keep-alive or close

## Codec System

`Codec` interface:

```go
type Codec interface {
    ContentTypes() []string
    Decode(data []byte, v interface{}) error
}
```

Optional interfaces: `ContentTypeAwareCodec` (response encoding), `EncoderCodec` (separate encode/decode), `ResettableCodec` (pool recycling).

`BodyParser` iterates registered codecs, matches by `Content-Type`, and delegates decoding.

## HTTP/2 Implementation

Self-contained in `http2.go` with `pkg/hpack`:

- **Framing**: DATA, HEADERS, PRIORITY, RST_STREAM, SETTINGS, PUSH_PROMISE, PING, GOAWAY, WINDOW_UPDATE, CONTINUATION
- **HPACK**: Custom implementation (~36% faster static-table decode vs `golang.org/x/net/http2/hpack`)
- **Stream multiplexing**: `sync.Map`-backed stream table with flow control
- **H2C**: Prior knowledge (direct `PRI * HTTP/2.0`) and upgrade (`Upgrade: h2c`)
- **TLS ALPN**: Automatic via `ListenTLS` with `h2` protocol negotiation

## WebSocket Implementation

- **Low-level**: `pkg/websocket.Conn` — read/write with masking, control frames (ping/pong/close), rate limiting
- **High-level**: `pkg/websocket.EventHub` — pub/sub with auth, authorize, rooms, topics, channels, acknowledgements, heartbeat, reconnect

## Testing

```bash
go test ./...
go test -bench=. ./...
go test -v -run TestRouter ./...
```

### Test Structure

| File | Scope |
|---|---|
| `fasthttp_test.go` | Integration tests with real `net.Conn` |
| `codec_test.go` | Form/codec unit tests |
| `fs_test.go` | Static file serving tests |
| `protocol_test.go` | HTTP/1.1 + HTTP/2 protocol (chunked, trailers, h2c, TLS) |
| `hardening_test.go` | Security/fuzzing/hardening |
| `router_features_test.go` | Router (params, wildcards, named routes) |
| `middleware_features_test.go` | Middleware integration |
| `template_test.go` | Template engine |

## Performance Considerations

- **Pool-based Ctx allocation** avoids per-request heap allocation
- **Byte buffer pools** at 4 sizes (512B, 4K, 16K, 64K) match common I/O sizes
- **Zero-alloc header parsing** uses `[]byte` comparisons from the raw buffer
- **Unsafe string/byte conversions** (`b2s`, `s2b`) avoid copying where safe
- **HPACK decoder pool** reuses decoder state across HTTP/2 streams

---

# Examples

Full working examples in `examples/`:

| Example | Description |
|---|---|
| `basic` | Minimal "Hello World" |
| `codecs` | All built-in codecs + custom codec + QueryParser |
| `files` | File upload/download with range/conditional requests |
| `http2` | TLS ALPN, h2c prior knowledge, h2c upgrade |
| `middleware` | Body limits, rewrite, skip, CSRF, cache, Early-Data, CORS |
| `redirect` | 301/302 redirects, named route redirect, redirect back |
| `sink` | Comprehensive: sessions, all methods, groups, codecs, streaming, WebSocket |
| `websocket` | Event-driven WebSocket with EventHub (auth, topics, channels, heartbeat) |
| `fiber` | Comparative example using `gofiber/fiber/v3` |
