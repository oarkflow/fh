# Routing

## Adaptive Trie Router

fh uses an adaptive segment trie for routing with O(n) insert/lookup where n is the number of path segments. Tiny route tables use a bounded linear fast path; larger tables automatically use hash/trie lookup so matching cost does not grow linearly with the total route count.

The request hot path uses byte slices, supports zero-allocation matching in fast mode, and becomes lock-free after the router is frozen. Static segments take precedence over named parameters, and named parameters take precedence over wildcards.

### Basic Routes

```go
app.Get("/", home)
app.Get("/users", listUsers)
app.Post("/users", createUser)
app.Put("/users/:id", updateUser)
app.Delete("/users/:id", deleteUser)
app.Patch("/users/:id", patchUser)
app.Head("/users/:id", headUser)
app.Options("/users", optionsUsers)
app.Connect("/proxy", proxyConnect)
app.Trace("/debug", traceHandler)
app.Query("/search", searchHandler)
```

### Named Parameters (`:param`)

```go
app.Get("/users/:id", func(c fh.Ctx) error {
    id := c.Params("id")
    return c.SendString("User: " + id)
})

app.Get("/users/:id/posts/:postId", func(c fh.Ctx) error {
    id := c.Params("id")
    postId := c.Params("postId")
    return c.JSON(map[string]string{"user": id, "post": postId})
})
```

Query strings are not part of route matching. For example, `/users/42?expand=team` matches `/users/:id` and captures `id=42`.

### Wildcard Parameters (`*wild`)

```go
app.Get("/files/*path", func(c fh.Ctx) error {
    path := c.Params("path")
    return c.SendFile(path)
})
```

### Default Values

```go
c.Params("name")           // returns "" if not found
c.Params("name", "guest")  // returns "guest" if not found
```

### Method Routing

```go
// Register all 10 HTTP methods at once
app.All("/webhook", webhookHandler)

// Register custom method
app.Add("PURGE", "/cache", purgeHandler)

// QUERY method (RFC 9485) — safe, idempotent, body-bearing method for search
app.Query("/search", func(c fh.Ctx) error {
    var q SearchRequest
    c.BodyParser(&q)
    return c.JSON(search(q))
})

// HEAD falls back to GET handler automatically if no HEAD route is registered
```

---

## Named Routes

```go
app.Get("/users/:id", getUser).Name("user.profile")
app.Post("/users", createUser).Name("user.create")
```

### Reverse URL Generation

```go
url, err := app.URL("user.profile", map[string]string{"id": "42"})
// url = "/users/42"

// Redirect to named route
c.RedirectTo("user.profile", map[string]string{"id": "42"})
```

---

## Route Groups

Groups allow you to organize routes under a common prefix with shared middleware.

```go
api := app.Group("/api")

// All routes under /api
api.Get("/users", listUsers)
api.Post("/users", createUser)
api.Get("/users/:id", getUser)
```

### Group with Middleware

```go
api := app.Group("/api", authMiddleware, loggerMiddleware)
api.Get("/users", listUsers)
```

### Nested Groups

```go
v1 := app.Group("/v1")
v1.Get("/status", status)

admin := v1.Group("/admin", adminAuth)
admin.Get("/dashboard", dashboard)
// Route: /v1/admin/dashboard
```

### Group Method Chaining

Groups provide all the same HTTP method registration methods as the App: `Get`, `Post`, `Put`, `Delete`, `Patch`, `Head`, `Options`, `Connect`, `Trace`, `All`, `Add`.

---

## HEAD Fallback to GET

If no handler is explicitly registered for HEAD on a path, the framework automatically falls back to the GET handler registered for that same path and strips the response body. Other explicit HEAD routes do not disable this per-path fallback. This ensures HEAD requests always return the correct headers.

---

## Route Information

### Route Metadata

```go
type RouteInfo struct {
    Method      string
    Path        string
    Name        string
    Middlewares int
    Typed       any
    Schema      any
}
```

### Listing Routes

```go
for _, route := range app.Routes() {
    fmt.Printf("%s %s (%s)\n", route.Method, route.Path, route.Name)
}
```

### Enable Route List Endpoint

```go
app.EnableRouteList("/_fh/routes")
// GET /_fh/routes returns JSON list of all registered routes
```

---

## Route Freezing

After `app.Listen()` or `app.Serve()` is called, the router is frozen (read-only) for lock-free concurrent reads. Routes cannot be modified after freezing.

To manually freeze:

```go
app.Router().Freeze()
```

---

## Handler Chain

Routes can have multiple handlers. They execute in order using `ctx.Next()`.

```go
func middleware(c fh.Ctx) error {
    c.Locals("start", time.Now())
    err := c.Next() // call next handler
    elapsed := time.Since(c.Locals("start").(time.Time))
    log.Printf("Request took %v", elapsed)
    return err
}

func handler(c fh.Ctx) error {
    return c.JSON(map[string]string{"status": "ok"})
}

app.Get("/", middleware, handler)
```

### Global Middleware

Middleware registered with `app.Use()` applies to all routes:

```go
app.Use(recover.New())
app.Use(logger.New())
```

### Group Middleware

Middleware on a group applies only within that group (including nested groups):

```go
admin := app.Group("/admin", authMiddleware, auditMiddleware)
```

### Skip Middleware

Use the `mw/skip` package for conditional skipping:

```go
import "github.com/oarkflow/fh/mw/skip"

app.Use(skip.When(recover.New(), skip.Path("/health")))
```

---

## Rewrite Loop Protection

If a rewrite middleware changes the path and the new path matches a different route, the router re-dispatches. A maximum of 8 rewrite loops is enforced to prevent infinite cycles.
