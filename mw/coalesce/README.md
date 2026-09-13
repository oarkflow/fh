# coalesce middleware

Collapses concurrent identical requests so one handler execution supplies the
response to all waiters. The key includes method and full URL.

```go
app.Use(coalesce.New(coalesce.Config{
    TTL: 2 * time.Second,
    MaxEntries: 4_096,
}))
```

Authorization-bearing requests are never coalesced. Cookie-bearing requests
are also excluded unless `AllowRequestCookies` is explicitly enabled. Keep that
unsafe opt-in disabled for personalized responses. Set `IncludeBody` only when
coalescing body-bearing requests is intended and body limits are already active.
