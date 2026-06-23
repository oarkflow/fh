# audit middleware

Records compliance-grade business/security audit events through `fh.AuditSink`.

```go
app.Use(audit.New(audit.Config{Action:"http.request", Resource:"api", OnError:true}))
```

Use route-local audit records for sensitive operations:

```go
app.Post("/admin/users/:id/disable", handler,
    audit.New(audit.Config{Action:"user.disabled", Resource:"user", ResourceID: func(c *fh.Ctx) string { return c.Param("id") }}),
)
```
