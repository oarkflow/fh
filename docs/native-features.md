# Native Features

## Typed Endpoints

Typed endpoints provide type-safe request handling with automatic JSON parsing, validation, struct binding, and response serialization.

```go
type CreateUserRequest struct {
    Name  string `json:"name" validate:"required"`
    Email string `json:"email" validate:"required,email"`
    Age   int    `json:"age" validate:"min=18"`
}

type CreateUserResponse struct {
    ID    string `json:"id"`
    Name  string `json:"name"`
    Email string `json:"email"`
}

app.PostTyped("/users", func(c fh.Ctx, req CreateUserRequest) (CreateUserResponse, error) {
    // req is automatically validated (if it implements Validator)
    user := createUser(req)
    return CreateUserResponse{
        ID:    user.ID,
        Name:  user.Name,
        Email: user.Email,
    }, nil
})
```

### Available Typed Methods

```go
app.GetTyped(path, handler)
app.HeadTyped(path, handler)
app.PostTyped(path, handler)
app.PutTyped(path, handler)
app.PatchTyped(path, handler)
app.DeleteTyped(path, handler)
app.OptionsTyped(path, handler)
app.ConnectTyped(path, handler)
app.TraceTyped(path, handler)
app.QueryTyped(path, handler)
app.AllTyped(path, handler)  // registers all 10 methods
```

QUERY typed endpoints work like POST (accept a body) but are safe and idempotent:

```go
type SearchRequest struct {
    Query string `json:"query" validate:"required"`
    Page  int    `json:"page"`
}

type SearchResponse struct {
    Results []SearchResult `json:"results"`
    Total   int            `json:"total"`
}

app.QueryTyped("/search", func(c fh.Ctx, req SearchRequest) (SearchResponse, error) {
    results, total := search(req.Query, req.Page)
    return SearchResponse{Results: results, Total: total}, nil
})
```

Typed methods are also available on route groups:

```go
api := app.Group("/api")
api.GetTyped("/users/:id", handler)
api.PostTyped("/users", handler)
api.PutTyped("/users/:id", handler)
api.DeleteTyped("/users/:id", handler)
```

### Validation

If the request type implements the `Validator` interface, it is automatically validated:

```go
func (r CreateUserRequest) Validate() error {
    if r.Name == "" {
        return fh.NewHTTPError(422, "INVALID_NAME", "name is required")
    }
    return nil
}
```

### Struct Binding Tags

Typed endpoints support automatic parameter binding from multiple sources via struct tags:

```go
type GetUserRequest struct {
    ID    string `param:"id"`                // from route parameter :id
    Page  int    `query:"page"`              // from query string
    Token string `header:"Authorization"`    // from header
    Role  string `cookie:"role"`             // from cookie
    // All can be mixed with JSON body fields
}
```

Tag order priority for body fields: `param` > `query` > `header` > `cookie` > JSON body.

### Schema Generation

Typed endpoints automatically generate JSON Schema for OpenAPI:

```go
// app.EnableOpenAPI("/openapi.json") exposes the schema
// Each typed endpoint's request/response types are included
```

---

## OpenAPI 3.1

fh can auto-generate an OpenAPI 3.1 specification from registered routes and typed endpoints.

```go
app.EnableOpenAPI("/openapi.json")
// Serves OpenAPI spec at GET /openapi.json
```

### OpenAPI with Docs UI

```go
app.EnableOpenAPI("/openapi.json")
app.EnableDocs("/docs")
// Serves a simple HTML documentation page at GET /docs
// that fetches the spec from /openapi.json
```

### What's Included

- **Paths** — All registered routes with methods
- **Parameters** — Route params, query params from typed endpoint struct tags
- **Request Bodies** — JSON Schema for typed endpoint request types
- **Responses** — JSON Schema for typed endpoint response types
- **Schemas** — All referenced types as reusable schema components
- **Info** — Framework name and version

---

## Server-Sent Events (SSE)

Native SSE support with bounded connection lifetime inherited from the request
context and streaming writes through `fh.SSE`.

```go
app.Get("/events", func(c fh.Ctx) error {
    return c.SSE(func(events *fh.SSE) error {
        for i := 0; i < 10; i++ {
            if err := events.WriteEvent(fh.SSEEvent{
                Event: "update",
                Data:  fmt.Sprintf("Message %d", i),
                ID:    fmt.Sprintf("%d", i),
            }); err != nil {
                return err
            }

            if err := events.Comment("keepalive"); err != nil {
                return err
            }

            time.Sleep(1 * time.Second)
        }
        return nil
    })
})
```

### SSEEvent

```go
type SSEEvent struct {
    ID      string
    Event   string
    Data    any
    Retry   time.Duration
    Comment string
}
```

### SSE methods

```go
events.WriteEvent(event)         // send a complete event
events.Send(data)                // ordinary message event
events.SendEvent(name, data)     // named event
events.Comment(text)             // comment/heartbeat
events.Ping()                    // standard ping comment
events.SetRetry(duration)        // client reconnect delay
events.Done()                    // closed when request disconnects
```

---

## Security Helpers

### Constant-Time Comparison

```go
if fh.ConstantTimeEqual(signature, expectedSignature) {
    // signatures match
}
```

### Redact Secrets

```go
redacted := fh.RedactSecret("sk_live_abc123def456")
// "sk_live_...456"
```

### Cookie Signing

```go
secret := []byte("replace-with-at-least-32-random-bytes")
signed := fh.SignCookie("session_value", secret)
value, valid := fh.VerifySignedCookie(signed, secret)
```

### Data Sensitivity

```go
redactor := fh.NewRedactor(fh.RedactionConfig{
    Enabled: true,
    Fields: []string{"password", "secret", "token"},
})
safeMetadata := redactor.RedactMap(metadata)
```

## Route Information

### RouteInfo

```go
type RouteInfo struct {
    Method      string
    Path        string
    Name        string
    Typed          bool
    RequestType    string
    ResponseType   string
    RequestSchema  fh.JSONSchema
    ResponseSchema fh.JSONSchema
    Deprecated     bool
    Tags           []string
    Security       fh.RouteSecurityConfig
    Data           fh.DataPolicy
    Meta           map[string]any
}
```

### Route Listing

```go
// Programmatic access
routes := app.Routes()
for _, r := range routes {
    fmt.Printf("%-6s %-30s %s\n", r.Method, r.Path, r.Name)
}

// HTTP endpoint
app.EnableRouteList("/_fh/routes", authenticationMiddleware)
// GET /_fh/routes -> JSON array of route info
```

---

## Gateway & Reverse Proxy

```go
app.Get("/api/*", proxy.New(proxy.Config{
    Target: "http://backend:9000",
}))

app.Use(proxy.Gateway(map[string]proxy.Config{
    "/users": {Target: "http://users-svc:9001"},
    "/orders": {Target: "http://orders-svc:9002"},
}))
```

`mw/proxy` supports path strip/add rewrite, header/director rewrite, and per-upstream timeouts. Combine with `mw/circuitbreaker` for upstream fault isolation.

## API Versioning

```go
app.Use(apiversion.New(apiversion.Config{
    Header:     "Accept-Version",
    Default:    "2026-06-01",
    Supported:  []string{"2026-01-01", "2026-06-01"},
    Deprecated: map[string]string{"2026-01-01": "2026-12-31"},
}))
```

`mw/apiversion` sets `api_version` in locals and emits `Deprecation` and `Sunset` headers for deprecated versions.

## Pluggable JSON Engine

```go
// Implement fh.JSONEngine, then install it once during startup.
fh.MustSetJSONEngine(myJSONEngine)
// Body codecs and JSON response helpers now use the installed engine.
```

## Pluggable Template Engine

```go
type TemplateEngine interface {
    Render(w io.Writer, name string, data any, layout ...string) error
}

// Usage:
c.Render("index", data)
c.Render("index", data, "main") // with layout
```
