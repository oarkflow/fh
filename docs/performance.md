# fh Performance Notes

fh's default runtime now keeps the secure parser and reliability hooks available while removing avoidable work from the HTTP/1 hot path.

## What changed

- The normal request path now includes the zero-copy and low-allocation optimizations directly; there is no separate mode or duplicate API surface.
- Requests with no write timeout reuse the connection context instead of allocating a new `context.WithCancel` object per request.
- `OriginalURL()` is preserved without copying the URI on every request. The slice is valid for the request lifetime, which is the same lifetime expected by route params in zero-copy mode.
- Common route methods (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS`, `CONNECT`, `TRACE`) use cached router pointers instead of a method map lookup per request.
- `Expect`, `Upgrade`, and `HTTP2-Settings` are captured while parsing headers, so ordinary HTTP/1 requests avoid repeated header scans for 100-continue and h2c upgrade checks.
- `Ctx.JSON(map[string]string)` and `Ctx.JSON(map[string]any)` use a direct append encoder for common small objects. `JSONAppend`, `JSONString`, `JSONBytes`, and `EchoJSON` remain the lowest-allocation APIs for known payloads and echo/proxy endpoints.

## Benchmark-mode configuration

For HTTP/1-only throughput comparisons, disable h2c detection explicitly:

```go
app := fh.New(
        fh.WithDisableHTTP2(true),
)
```

For production HTTP/2 or h2c deployments, leave HTTP/2 enabled and use normal production timeouts:

```go
app := fh.New(
    fh.WithReadTimeout(5*time.Second),
    fh.WithWriteTimeout(10*time.Second),
    fh.WithIdleTimeout(60*time.Second),
    fh.WithMaxRequestBodySize(4<<20),
)
```

## Hot response APIs

Use these when the response is already known or can be appended safely:

```go
app.Get("/json", func(c *fh.Ctx) error {
    return c.JSONString(`{"message":"Hello, World!"}`)
})

app.Get("/search", func(c *fh.Ctx) error {
    q := c.Query("q")
    return c.JSONAppend(func(dst []byte) ([]byte, error) {
        dst = append(dst, `{"query":"`...)
        dst = append(dst, q...)
        dst = append(dst, `"}`...)
        return dst, nil
    })
})

app.Post("/echo", func(c *fh.Ctx) error {
    return c.EchoJSON()
})
```

`JSON(v)` still supports the configured JSON engine for compatibility. For stable DTOs that need the highest throughput, implement `AppendJSON(dst []byte) ([]byte, error)` on the response type.

## Benchmarking

```bash
go test -bench=. -benchmem ./...
```

See [`benchmarks/`](../benchmarks/README.md) for cross-framework comparisons against Fiber and fasthttp.
