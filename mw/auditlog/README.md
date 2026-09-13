# auditlog middleware

Records a structured `AuditEntry` after downstream handling. Status codes map
to info, warning or critical severity. Configure one or more `Sink`
implementations; the default writes through `log.Printf`.

```go
sink := auditlog.NewBufferSink(10_000)
app.Use(auditlog.New(auditlog.Config{
    Sinks: []auditlog.Sink{sink},
    MinSeverity: auditlog.SeverityWarning,
    CaptureHeaders: []string{"X-Request-ID"},
}))
```

Captured header values are masked, but minimize collection and apply your data
retention policy. This request-summary middleware is distinct from the core
`fh.AuditSink`/ledger used by compliance features.
