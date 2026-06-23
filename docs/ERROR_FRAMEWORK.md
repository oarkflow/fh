# FH Error Framework

FH now includes an environment-aware error framework for professional HTTP APIs and fault-tolerant services.

## Features implemented

- RFC 9457 `application/problem+json` responses.
- Stable machine-readable error codes.
- Error kinds for policy, dashboards, alerts, and retry logic.
- Error severity values for operations and logging.
- Retryable flag and automatic `Retry-After` support for transient failures.
- Typed `HTTPError` with safe public message and private wrapped cause.
- `ValidationError` and `FieldError` support with field-level response details.
- `PanicError` with stack capture, safe client masking, and optional debug stack exposure.
- Environment-aware rendering: production masks internals, development/test can expose redacted diagnostics.
- Built-in secret redaction for passwords, tokens, authorization headers, cookies, API keys, and common credentials.
- Request ID propagation through `X-Request-ID` and `Locals("request_id")`.
- Cache-safe error responses with `Cache-Control: no-store`.
- Problem `type`, `instance`, `timestamp`, `kind`, `severity`, and `retryable` extensions.
- Safe fallback response path if error rendering itself fails.
- Error counting by stable code through `app.ErrorCount(code)`.
- Default 404 and 405 handlers now route through the same typed error framework.
- Panic recovery in core dispatch and `mw/recover` middleware.
- Middleware packages restored for the tests and real-world usage: bodylimit, cache, compress, cors, csrf, earlydata, recover, requestid, rewrite, security, session, skip, timeout.
- Minimal bundled `pkg/hpack` package restored so the repository keeps its internal import tree intact.

## Production configuration

```go
app := fh.New(fh.Config{
    Environment: fh.EnvProduction,
    ErrorOptions: fh.ErrorOptions{
        Environment:      fh.EnvProduction,
        ProblemTypeBase:  "https://api.example.com/problems",
        IncludeRequestID: true,
        IncludeTimestamp: true,
        IncludeInstance:  true,
    },
})
```

Production responses never expose private causes by default:

```json
{
  "type": "https://api.example.com/problems/internal-error",
  "title": "Internal Server Error",
  "status": 500,
  "detail": "An internal server error occurred",
  "code": "INTERNAL_ERROR",
  "kind": "internal",
  "severity": "error",
  "retryable": false,
  "request_id": "req_...",
  "timestamp": "2026-06-23T00:00:00Z"
}
```

## Development configuration

```go
app := fh.New(fh.Config{
    Environment: fh.EnvDevelopment,
    ErrorOptions: fh.ErrorOptions{
        Environment:  fh.EnvDevelopment,
        ExposeCauses: true,
        // Enable only locally. Stack traces can include sensitive source paths.
        ExposeStackTrace: true,
    },
})
```

Debug output is still passed through `RedactSecrets`, so values such as `password=...`, tokens, cookies, and API keys are masked.

## Returning errors from handlers

```go
app.Get("/users/:id", func(c *fh.Ctx) error {
    user, err := loadUser(c.Param("id"))
    if errors.Is(err, fs.ErrNotExist) {
        return fh.NotFound("User not found")
    }
    if err != nil {
        return fh.InternalError(err)
    }
    return c.JSON(user)
})
```

## Validation errors

```go
return &fh.ValidationError{Fields: []fh.FieldError{
    {Field: "email", Code: "required", Message: "email is required"},
}}
```

## Panic handling

Core dispatch catches panics. Use `mw/recover` when you want middleware-level recovery and custom panic logging:

```go
app.Use(recover.New(recover.Config{EnableStackTrace: true}))
```

## Fault-tolerant behavior

- Error rendering is guarded by a fallback path.
- Panics are converted into `PanicError` and classified as critical internal errors.
- Known protocol/body/timeout errors are mapped to proper HTTP statuses.
- Transient overload, timeout, dependency, and rate-limit failures are marked retryable.
- Responses include correlation metadata without leaking internals.
