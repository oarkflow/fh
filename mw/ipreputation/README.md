# ipreputation middleware

Scores client IPs, decays scores over time and supports explicit allow/block
lists. `New` returns the middleware and a cleanup function.

```go
handler, closeReputation := ipreputation.New(ipreputation.Config{
    Store: kv.NewMemoryStore(),
    MaxEntries: 65_536,
    BlockThreshold: 100,
    SuspiciousThreshold: 50,
})
defer closeReputation()
app.Use(handler)
```

Install `mw/realip` first when behind a proxy, and configure only trusted proxy
CIDRs. Use a shared bounded store when reputation must apply across replicas.
