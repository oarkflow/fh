# smartcache middleware

Caches bounded GET responses with conditional-request support and pluggable
`kv.Store` storage.

```go
app.Use(smartcache.New(smartcache.Config{
    MaxSize: 10_000,
    DefaultTTL: 5 * time.Minute,
    MaxBodySize: 1 << 20,
    VaryHeaders: []string{"Accept-Encoding", "Accept-Language"},
}))
```

Requests with Authorization or Cookie are excluded by default, as are responses
with `Set-Cookie` or private/no-store directives. Keep
`AllowRequestCookies=false` unless the cache key safely partitions identity.
