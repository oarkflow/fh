# compliance middleware

Attaches route security/data metadata and enforces common security requirements.

```go
app.Post("/payments", handler,
    compliance.New(compliance.Config{
        Security: fh.RouteSecurityConfig{AuthRequired:true, IdempotencyRequired:true, Scopes:[]string{"payments:create"}},
        Data: fh.DataPolicy{Sensitivity:"regulated", RedactLogs:true, EncryptAtRest:true},
    }),
).WithRouteSecurity(fh.RouteSecurityConfig{AuthRequired:true, IdempotencyRequired:true})
```
