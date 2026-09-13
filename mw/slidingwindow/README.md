# slidingwindow middleware

Provides per-key sliding-window rate limiting with an optional burst allowance.

```go
app.Use(slidingwindow.New(slidingwindow.Config{
    Rate: 100,
    Burst: 20,
    Window: time.Minute,
    MaxKeys: 65_536,
    KeyFunc: slidingwindow.ByIP,
    Store: kv.NewMemoryStore(),
}))
```

Use a shared bounded store for cross-replica enforcement. Normalize trusted
proxy identity before choosing IP as the key.
