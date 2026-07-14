# Getting Started

## Installation

```bash
go get github.com/oarkflow/fh
```

**Requirements:** Go 1.23.2 or later

## Quick Start

```go
package main

import (
    "github.com/oarkflow/fh"
)

func main() {
    app := fh.New()

    app.Get("/", func(c *fh.Ctx) error {
        return c.JSON(map[string]string{"hello": "world"})
    })

    app.Listen(":8080")
}
```

## Basic Concepts

### Creating an App

```go
app := fh.New()                           // default config
app := fh.New(fh.Config{                  // custom config
    ReadTimeout: 5 * time.Second,
    WriteTimeout: 10 * time.Second,
})
```

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

// QUERY (RFC 9485) — safe/idempotent method with request body for search/query
app.Query("/search", func(c *fh.Ctx) error {
    var q SearchRequest
    c.BodyParser(&q)
    return c.JSON(search(q))
})
```

### Handler Signature

```go
func handler(c *fh.Ctx) error {
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
app.OnListen(func() {
    log.Println("Server started")
})
app.OnShutdown(func() {
    log.Println("Server shutting down")
})
app.OnConnect(func(c *fh.Ctx) {
    log.Printf("New connection from %s", c.IP())
})
app.OnClose(func(c *fh.Ctx) {
    log.Printf("Connection closed: %s", c.IP())
})
app.OnError(func(c *fh.Ctx, err error) {
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
c.Port()              // 8080
c.Scheme()            // http or https
c.Protocol()          // HTTP/1.1 or HTTP/2
c.IsTLS()             // true if TLS
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
c.Get("Content-Type")     // single header
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
c.SendBytes([]byte{...})  // binary
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
func myMiddleware(c *fh.Ctx) error {
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
return fh.NewHTTPError(429, "Rate limit exceeded")

// Problem Details (RFC 9457)
return c.Problem(fh.Problem{
    Type:   "https://example.com/errors/rate-limited",
    Title:  "Rate Limited",
    Detail: "Too many requests",
})

// Custom error handler
app := fh.New(fh.Config{
    ErrorHandler: func(c *fh.Ctx, err error) error {
        return c.JSON(map[string]string{"error": err.Error()})
    },
})
```

## Full Example

```go
package main

import (
    "log"
    "time"
    "github.com/oarkflow/fh"
    "github.com/oarkflow/fh/mw/recover"
    "github.com/oarkflow/fh/mw/logger"
    "github.com/oarkflow/fh/mw/compress"
)

func main() {
    app := fh.New(fh.Config{
        ReadTimeout:  10 * time.Second,
        WriteTimeout: 10 * time.Second,
    })

    // Global middleware
    app.Use(recover.New())
    app.Use(logger.New())
    app.Use(compress.New())

    // Routes
    app.Get("/", func(c *fh.Ctx) error {
        return c.SendString("Hello, World!")
    })

    api := app.Group("/api")
    api.Get("/users", listUsers)
    api.Post("/users", createUser)

    log.Fatal(app.Listen(":8080"))
}
```
