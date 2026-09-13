# fetchmetadata middleware

Enforces browser Fetch Metadata headers as defense in depth against CSRF, XSSI
and cross-site leaks.

```go
app.Use(fetchmetadata.New(fetchmetadata.Config{
    AllowTopLevelNavigations: true,
    AllowedDestinations: []string{"image"},
    ExemptPaths: []string{"/webhooks/"},
}))
```

This does not replace CSRF tokens for cookie-authenticated unsafe requests.
Exempt only endpoints protected by an appropriate non-cookie mechanism.
