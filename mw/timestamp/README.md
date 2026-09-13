# timestamp middleware

Validates request timestamps and optional nonces within a bounded replay store.
`New` returns middleware plus a cleanup function.

```go
handler, closeTimestamps := timestamp.New(timestamp.Config{
    Header: "X-Timestamp",
    NonceHeader: "X-Nonce",
    MaxSkew: 5 * time.Minute,
    MaxSize: 100_000,
    Required: true,
    RequireNonce: true,
    Store: kv.NewMemoryStore(),
})
defer closeTimestamps()
app.Use(handler)
```

A timestamp is not a signature. Bind method, target, identity, body digest,
timestamp and nonce in authenticated request-signature middleware.
