# ipthrottle middleware

Applies per-IP and global fixed-window limits with bounded IP cardinality.

```go
app.Use(ipthrottle.New(ipthrottle.Config{
    MaxPerIP: 100,
    GlobalMax: 10_000,
    MaxIPs: 65_536,
    Window: time.Minute,
    Store: kv.NewMemoryStore(),
}))
```

When `MaxIPs` is exhausted the middleware fails closed. Install trusted
real-IP normalization first when requests arrive through a proxy.
