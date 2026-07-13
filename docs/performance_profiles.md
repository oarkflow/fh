# fh runtime profiles

fh now separates benchmark-oriented speed from strict production and enterprise defaults.

## Fast

Use `fh.NewFast()` or `fh.New(fh.WithMode(fh.ModeFast))` for trusted benchmark paths. This keeps benchmark-sensitive defaults such as no Date header and omits per-request graceful-shutdown activity tracking. Fast-mode shutdown closes HTTP/1 connections immediately; use `NewProduction` when in-flight requests must drain. Prefer explicit byte APIs in handlers:

```go
app := fh.NewFast()
app.Get("/ping", func(c fh.Ctx) error {
    dc := c.(*fh.DefaultCtx)
    _ = dc.PathBytes()
    return c.SendString("ok")
})
```

String helpers such as `Path()`, `Method()`, `Query()` and map helpers such as `GetHeaders()` remain compatibility APIs and may allocate.

## Production

Use `fh.NewProduction()` or default `fh.New()`. Production mode enables Date headers and read/write/idle timeouts while preserving the zero-copy parser, pooled contexts and pooled buffers.

## Enterprise

Use `fh.NewEnterprise()` for strict protocol validation, compliance evidence endpoints, redaction, audit and reliability defaults.

Enterprise mode is intentionally not a zero-allocation path: audit records, compliance evidence, JSON reports, reliability journals and redaction all perform durable or structured work. Use this mode for correctness, auditability and business controls.

## HTTP/1.1 hardening implemented

- Absolute-form request targets are normalized for routing while preserving the original target.
- Configured header-count limits are enforced instead of silently truncating.
- Unsupported or unsafe transfer-coding now fails with bad request semantics instead of treating dangerous framing as an ordinary unsupported feature.

## HTTP/2 hardening implemented

- Client SETTINGS no longer mutate global app config.
- Peer send limits and local receive limits are separated.
- Inbound frame-size validation uses the server's local receive limit.
- Connection-level receive flow-control accounting is enforced.
- Header-count overflow is rejected instead of silently truncating.

## Remaining engineering boundary

Zero allocation is a hot-path target, not a promise for every feature. Streaming transforms, JSON reflection, map conversion, audit/compliance, reliability storage, cookies and middleware stacks can allocate. Use byte APIs, append-style JSON, `NewFast`, and benchmark gates for strict performance-critical endpoints.
