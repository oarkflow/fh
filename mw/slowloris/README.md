# slowloris middleware

Adds request-time goroutine and heap admission checks. Core absolute header and
body deadlines remain the primary slow-client defense.

```go
app.Use(slowloris.New(slowloris.Config{
    MaxGoroutines: 20_000,
    MaxHeapBytes: 1 << 30,
    SampleInterval: 250 * time.Millisecond,
}))
```

For new applications, `fh.WithSecureByDefault(true)` supplies equivalent core
resource ceilings in addition to protocol-level limits.
