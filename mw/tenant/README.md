# tenant middleware

Resolves tenant identity from the authenticated principal or a configured header.

```go
app.Use(tenant.New(tenant.Config{Header:"X-Tenant-ID", Required:true}))
```

The tenant is available with `fh.TenantID(c)` and is included in audit events.
