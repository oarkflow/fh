# servertiming middleware

Emits RFC 8638 `Server-Timing` metrics.

```go
app.Use(servertiming.New(servertiming.Config{MaxMetrics: 16, AddTotal: true}))

app.Get("/items", func(c fh.Ctx) error {
    timings := servertiming.Get(c)
    timings.Start("db")
    items, err := loadItems(c.Context())
    timings.Stop("db")
    if err != nil { return err }
    timings.AddBytes("payload", int64(len(items)))
    return c.SendBytes(items)
})
```

Metric names/descriptions are response data. Do not expose sensitive internal
topology or identifiers; use `Opaque` where duration disclosure is undesirable.
