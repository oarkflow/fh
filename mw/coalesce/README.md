# coalesce middleware

Collapses overlapping identical requests so one handler execution supplies the
response to all waiters. Completed responses are removed immediately, so later
requests always run the handler and receive fresh data. The key includes method
and full URL.

```go
app.Use(coalesce.New(coalesce.Config{
    MaxEntries: 4_096,
    MaxBodySize: 1 << 20,
    VaryHeaders: []string{"Accept", "Accept-Language", "X-Tenant-ID"},
}))
```

`TTL` remains accepted for source compatibility but completed entries are no
longer retained. Entries are removed when their handler finishes. If a response
exceeds `MaxBodySize` (1 MiB by default), waiters run their own handler instead
of retaining or replaying that response.

For authenticated personalized GET routes, attach coalescing after authentication
and provide a key that includes the authenticated principal, for example:

```go
app.Group("/api", authenticate, coalesce.New(coalesce.Config{
    KeyFunc: func(c fh.Ctx) string {
        return c.Method() + ":" + authenticatedUserID(c) + ":" + c.OriginalURL()
    },
}))
```

Replace `authenticatedUserID` with the application's principal lookup. The key
must include every value that can change the response. Keep the default
credential exclusions unless the custom key safely partitions identity.
For simpler cases, `VaryHeaders` adds response-varying request headers to the
default method/URL key. The default key already includes the query string.
Custom keys can also read authenticated principals, session data, tenant
context, or other `fh.Ctx` values. A URL fragment is browser-only and is never
sent to FH, so it can only distinguish calls in a client-side key.

Authorization-bearing requests are never coalesced. Cookie-bearing requests
are also excluded unless `AllowRequestCookies` is explicitly enabled. Keep that
unsafe opt-in disabled for personalized responses. Set `IncludeBody` only when
coalescing body-bearing requests is intended and body limits are already active.
