# Maintenance Mode Middleware

## What it does

Provides a runtime maintenance switch for controlled downtime, brownouts, migrations, deployments, and dependency outages.

It supports two production modes:

- API mode: return a JSON maintenance response with `503 Service Unavailable`.
- Render/redirect mode: when `Renderer` and `Path` are configured, normal traffic redirects to the public maintenance path and that path is rendered by your custom handler.

## How to implement

### Basic JSON maintenance response

```go
sw := maintenance.NewSwitch()
app.Use(maintenance.New(maintenance.Config{Switch: sw}))

sw.Enable("Database migration in progress")
```

During maintenance, requests receive:

```text
HTTP/1.1 503 Service Unavailable
Retry-After: 60
Content-Type: application/json
```

### Custom maintenance renderer

`Renderer` is a normal `fh.Handler`, so it can render HTML, JSON, templates, or static content using your existing response helpers.

```go
sw := maintenance.NewSwitch()

app.Use(maintenance.New(maintenance.Config{
    Switch: sw,
    Renderer: func(c fh.Ctx) error {
        return c.Status(fh.StatusServiceUnavailable).
            Type("html").
            SendString(`<!doctype html><h1>Maintenance</h1><p>We will be back shortly.</p>`)
    },
}))
```

### Redirect browser traffic to a maintenance page

When both `Path` and `Renderer` are configured, the middleware redirects non-bypassed requests to `Path`. Requests already targeting `Path` call the renderer directly, so redirect loops are avoided.

```go
sw := maintenance.NewSwitch()

app.Use(maintenance.New(maintenance.Config{
    Switch: sw,
    Path:   "/maintenance",
    Renderer: func(c fh.Ctx) error {
        return c.Status(fh.StatusServiceUnavailable).
            Type("html").
            SendString(`<!doctype html><h1>We will be back soon</h1>`)
    },
    RedirectCode: fh.StatusFound,
}))
```

### Bypass trusted internal traffic

```go
app.Use(maintenance.New(maintenance.Config{
    Switch:       sw,
    BypassHeader: "X-Maintenance-Bypass",
    BypassToken:  "internal-secret",
}))
```

## Configuration

```go
type Config struct {
    Switch       *maintenance.Switch
    BypassHeader string
    BypassToken  string
    RetryAfter   time.Duration
    StatusCode   int

    Renderer     fh.Handler
    Path         string
    RedirectCode int

    JSONBody func(c fh.Ctx, data maintenance.ViewData) any
}
```

## Impact

- JSON mode returns `503 Service Unavailable` by default.
- Renderer mode uses your handler, so your handler controls status, content type, and body.
- Redirect mode returns `302 Found` by default for normal routes and renders the maintenance page at `Path`.
- `Retry-After` is set before either JSON or rendered responses.

## Ordering guidance

Run early so expensive application handlers are skipped during maintenance. Place after trusted proxy/real-IP middleware if bypass decisions depend on proxy-derived headers. Decide whether health/admin endpoints should be bypassed based on your deployment model.

## Production considerations

- Protect switch control with admin auth, IP allowlists, mTLS, or internal-only ops APIs.
- Keep the renderer fast and dependency-light so the maintenance page still works during outages.
- Prefer JSON mode for APIs and redirect/render mode for browser-facing apps.
- Use bypass headers only between trusted internal systems.
