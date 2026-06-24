# TCPGuard middleware for fh

This package integrates the upstream [`github.com/oarkflow/tcpguard`](https://github.com/oarkflow/tcpguard) engine with `github.com/oarkflow/fh`.

It is intentionally a middleware adapter only. Rule evaluation, BCL loading, audit chains, incidents, approvals, HMAC/replay checks, datasource lookups, risk scoring, response rendering, and management APIs are provided by upstream TCPGuard.

## Install

```bash
go get github.com/oarkflow/tcpguard@v0.0.14
go get github.com/oarkflow/fh/contrib
```

## Minimal usage

```go
package main

import (
    "context"
    "log"

    "github.com/oarkflow/fh"
    tcpguardmw "github.com/oarkflow/fh/contrib/mw/tcpguard"
    "github.com/oarkflow/tcpguard"
    "github.com/oarkflow/tcpguard/bcl"
)

func main() {
    bundle, err := bcl.LoadTCPGuardBundleFile(context.Background(), "tcpguard.bcl")
    if err != nil {
        log.Fatal(err)
    }
    guard, err := tcpguard.New(tcpguard.WithBundle(bundle))
    if err != nil {
        log.Fatal(err)
    }

    app := fh.New()
    app.Use(tcpguardmw.Middleware(guard))
    app.Get("/public", func(c *fh.Ctx) error {
        return c.JSON(map[string]any{"ok": true})
    })
    log.Fatal(app.Listen(":3000"))
}
```

## Full configuration

```go
app.Use(tcpguardmw.MiddlewareWithConfig(tcpguardmw.Config{
    Guard: guard,
    Skip: func(c *fh.Ctx) bool {
        return c.Path() == "/healthz"
    },
    HeaderPrefix: "X-TCPGuard",
    ResponsePolicy: tcpguard.DefaultResponseMessagePolicy(tcpguard.EnvironmentProduction),
    OnDecision: func(c *fh.Ctx, result tcpguard.HTTPRequestResult) {
        // write structured logs, metrics, traces, or SOC events
    },
    OnError: func(c *fh.Ctx, err error) error {
        return c.Status(500).JSON(map[string]any{"error": err.Error()})
    },
}))
```

## Behavior

For every non-skipped request the middleware builds an `*http.Request` from the `fh.Ctx` and calls `Guard.EvaluateHTTPRequest`.

If TCPGuard does not enforce the decision, the middleware calls `c.Next()` and the endpoint runs normally.

If TCPGuard enforces the decision, the middleware writes the upstream TCPGuard rendered response, sets TCPGuard metadata headers, and stops the chain.

## Headers

The adapter sets safe response metadata headers:

- `X-TCPGuard-Decision`
- `X-TCPGuard-Severity`
- `X-TCPGuard-Trace`
- `X-TCPGuard-Message`
- `X-TCPGuard-Risk` when the response policy enables risk score exposure

## Complete example

See `examples/tcpguard-fh-server` for a complete demo using `github.com/oarkflow/tcpguard v0.0.14`, BCL policy files, datasource lookups, HMAC signing, auth events, incidents, approvals, audit verification, management APIs, and endpoint-specific behavior.
