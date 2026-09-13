# API Conventions

This page records conventions that apply throughout fh and prevents common
mistakes when reading older examples.

## Context and handlers

`fh.Ctx` is an interface. Do not use a pointer to it.

```go
type HandlerFunc func(fh.Ctx) error

func handler(c fh.Ctx) error {
    return c.SendString("ok")
}
```

The built-in implementation is `*fh.DefaultCtx`. Avoid type assertions unless
an optimization explicitly needs implementation-only behavior. The context and
most byte slices returned from it are pooled and valid only during the request.
Use `BodyCopy`, copy values, or enable `SafeParams` before retaining data after
the handler returns.

## App construction

`fh.New` accepts functional options, not a `Config` value:

```go
app := fh.New(fh.WithReadTimeout(5 * time.Second))
app := fh.NewWithConfig(fh.Config{ReadTimeout: 5 * time.Second})
```

`New`, `NewProduction`, `NewFast` and `NewEnterprise` return `*fh.App` and may
panic on invalid startup configuration. Register routes and hooks before
serving; the router is frozen after startup.

## Errors

Handlers return errors. The core error handler has this signature and writes a
response rather than returning another error:

```go
type ErrorHandler func(fh.Ctx, error)

return fh.NewHTTPError(422, "INVALID_EMAIL", "email is invalid")
```

Use stable machine-readable codes. Wrap private causes with the typed error
helpers and use production error rendering so secrets and internal causes are
not exposed.

## Headers and cookies

`c.Get`/`GetReqHeaders` read request headers. `c.Set`, `Append`, `Vary` and
`Type` modify response headers. Cookies use `GetCookie`, `SetCookie` and
`DelCookie`.

## Streaming

Streaming callbacks are synchronous and return errors:

```go
return c.Stream(func(w *fh.StreamWriter) error {
    _, err := w.Write([]byte("chunk\n"))
    return err
})
```

Observe `c.Done()` or the callback controller's `Done()` for disconnects.
`WriteTimeout` applies to socket writes and is refreshed by streaming writes;
`HandlerTimeout` is a separate context deadline.

## Lifecycle hooks

```go
app.OnListen(func() error { return nil })
app.OnShutdown(func() error { return nil })
app.OnConnect(func(net.Conn) {})
app.OnClose(func(net.Conn) {})
app.OnError(func(error) {})
app.OnRoute(func(fh.RouteInfo) {})
```

## Process-wide registries

Codec and JSON-engine configuration is process-wide. Set it during startup,
before serving concurrent requests:

```go
fh.SetCodecOptions(fh.CodecOptions{MaxFormPairs: 5_000})
fh.MustSetJSONEngine(engine)
```

## Source of truth

The exported Go declarations and GoDoc are authoritative if prose and code
disagree. Documentation examples are intended to track the current `main`
branch; consumers should use the documentation attached to their pinned tag.
