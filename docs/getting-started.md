# Getting Started

## Installation

```bash
go get github.com/oarkflow/fh
```

**Requirements:** Go 1.26.5 or later. The module is currently pre-v1, so pin a
specific release and review release notes before upgrading.

## Quick Start

```go
package main

import (
    "log"

    "github.com/oarkflow/fh"
)

func main() {
    app := fh.New(fh.WithSecureByDefault(true))

    app.Get("/", func(c fh.Ctx) error {
        return c.JSON(map[string]string{"hello": "world"})
    })

    log.Fatal(app.ListenWithGracefulShutdown(":8080"))
}
```

This example enables the strict, fail-closed protocol baseline.
Authentication, authorization, CORS, CSRF, trusted-host policy and
application rate limits remain application-specific and must be configured
explicitly — this applies equally to `NewProduction()` below, despite the
name. See [Production Readiness](production-readiness.md#newproduction-and-securebydefault-are-narrower-than-they-sound)
for exactly what each constructor does and does not cover, and call
`app.ValidateSecurity()` for a live reminder plus concrete findings (e.g. a
missing `AllowedHosts` policy).

## Basic Concepts

### Creating an App

```go
app := fh.New()                           // production mode and bounded I/O defaults
app := fh.NewProduction()                 // explicit spelling of production mode
app := fh.NewWithConfig(fh.Config{        // config-struct form
    ReadTimeout: 5 * time.Second,
    WriteTimeout: 10 * time.Second,
})
app := fh.New(                            // functional-option form
    fh.WithSecureByDefault(true),
    fh.WithAllowedHosts("api.example.com"),
)
```

`New()` defaults to `ModeProduction`, but `SecureByDefault` is deliberately
opt-in. Use `NewFast()` only for trusted benchmarks or behind an edge that
supplies the omitted protections. `NewEnterprise()` enables compliance,
reliability and audit-oriented defaults and must be supplied authentication for
its evidence endpoints.

### Defining Routes

```go
app.Get("/users", listUsers)
app.Get("/users/:id", getUser)
app.Post("/users", createUser)
app.Put("/users/:id", updateUser)
app.Delete("/users/:id", deleteUser)
app.Patch("/users/:id", patchUser)
app.Head("/users/:id", headUser)
app.Options("/users", optionsUsers)
app.All("/webhook", webhookHandler) // matches any HTTP method
app.Add("PURGE", "/cache", purgeCache) // custom method

// QUERY (RFC 10008) — safe/idempotent method with a request body
app.Query("/search", func(c fh.Ctx) error {
    var q SearchRequest
    if err := c.BodyParser(&q); err != nil {
        return fh.NewHTTPError(fh.StatusBadRequest, "INVALID_QUERY", err.Error())
    }
    return c.JSON(search(q))
})
```

### Handler Signature

```go
func handler(c fh.Ctx) error {
    // ... handle request
    return c.SendString("ok")
}
```

### Starting the Server

```go
// HTTP
app.Listen(":8080")

// HTTPS with automatic HTTP/2 via ALPN
app.ListenTLS(":443", "cert.pem", "key.pem")

// Custom listener
ln, _ := net.Listen("tcp", ":8080")
app.Serve(ln)
```

### Graceful Shutdown

```go
// Listen with automatic signal handling (SIGINT/SIGTERM)
app.ListenWithGracefulShutdown(":8080")

// Blocking shutdown (waits indefinitely or until ShutdownTimeout)
app.Shutdown()

// Shutdown with context (deadline, cancellation)
app.ShutdownWithContext(ctx)

// Shutdown with explicit timeout
app.ShutdownWithTimeout(30 * time.Second)
```

### Lifecycle Hooks

```go
app.OnListen(func() error {
    log.Println("Server started")
    return nil
})
app.OnShutdown(func() error {
    log.Println("Server shutting down")
    return nil
})
app.OnConnect(func(conn net.Conn) {
    log.Printf("New connection from %s", conn.RemoteAddr())
})
app.OnClose(func(conn net.Conn) {
    log.Printf("Connection closed: %s", conn.RemoteAddr())
})
app.OnError(func(err error) {
    log.Printf("Error: %v", err)
})
```

## Request Context (Ctx)

The `Ctx` is the per-request context, acquired from `sync.Pool`. It provides all request/response accessors.

### Request Information

```go
c.Method()            // GET, POST, etc.
c.Path()              // /users/123
c.OriginalURL()       // /users/123?page=1
c.Hostname()          // example.com
c.IP()                // 192.168.1.1
c.Protocol()          // "http" or "https"
c.Secure()            // true if TLS
c.ConnectProtocol()   // RFC 8441 extended CONNECT protocol, when present
```

### Route Parameters

```go
c.Params("id")        // "123"
c.Params("name")      // with defaults: c.Params("name", "default")
c.Params("wild")      // wildcard: /files/*wild
```

### Query Parameters

```go
c.Query("page")       // "1"
c.Query("sort", "asc") // with default
```

### Headers

```go
c.Get("Content-Type")     // single request header
c.GetReqHeaders()         // all request headers
```

### Request Body

```go
body := c.Body()          // raw body bytes
body := c.BodyCopy()      // copied body (safe after handler returns)
c.BodyParser(&myStruct)   // auto-detect content-type and decode
```

### Response

```go
c.SendString("hello")     // plain text
c.SendBytes([]byte("binary payload"))  // binary
c.JSON(map[string]any{})  // JSON
c.XML(doc)                // XML
c.HTML("<h1>Title</h1>") // HTML
c.SendStatus(204)         // status only
c.Redirect("/login", 302) // redirect
c.SendFile("doc.pdf")     // file download
```

### Locals (request-scoped storage)

```go
c.Locals("user", userObj)
user := c.Locals("user").(*User)
```

### Middleware / Next

```go
func myMiddleware(c fh.Ctx) error {
    // before
    err := c.Next()
    // after
    return err
}
```

## Error Handling

```go
// Typed HTTP errors
return fh.ErrNotFound
return fh.ErrBadRequest
return fh.NewHTTPError(429, "RATE_LIMITED", "Rate limit exceeded")

// Problem Details (RFC 9457)
return c.Problem(fh.Problem{
    Type:   "https://example.com/errors/rate-limited",
    Title:  "Rate Limited",
    Detail: "Too many requests",
})

// Custom error handler
app := fh.NewWithConfig(fh.Config{
    ErrorHandler: func(c fh.Ctx, err error) {
        _ = c.SafeErrorResponse(err)
    },
})
```

`ErrorHandler` does not return an error. Prefer `SafeErrorResponse` in
production so private causes are not disclosed.

## Full Example

```go
package main

import (
    "log"
    "time"

    "github.com/oarkflow/fh"
    "github.com/oarkflow/fh/mw/compress"
    "github.com/oarkflow/fh/mw/logger"
    "github.com/oarkflow/fh/mw/recover"
)

func main() {
    app := fh.NewWithConfig(fh.Config{
        ReadTimeout:  10 * time.Second,
        WriteTimeout: 10 * time.Second,
        SecureByDefault: true,
    })

    // Global middleware
    app.Use(recover.New())
    app.Use(logger.New())
    app.Use(compress.New())

    // Routes
    app.Get("/", func(c fh.Ctx) error {
        return c.SendString("Hello, World!")
    })

    api := app.Group("/api")
    api.Get("/users", func(c fh.Ctx) error {
        return c.JSON([]string{"Ada", "Linus"})
    })
    api.Post("/users", func(c fh.Ctx) error {
        var input map[string]any
        if err := c.BodyParser(&input); err != nil {
            return fh.NewHTTPError(fh.StatusBadRequest, "INVALID_BODY", err.Error())
        }
        return c.Status(fh.StatusCreated).JSON(input)
    })

    log.Fatal(app.ListenWithGracefulShutdown(":8080"))
}
```
