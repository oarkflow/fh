# override middleware

Allows POST clients to request PUT, PATCH or DELETE through a validated method
override header, query value or form field.

```go
app.Use(override.New(override.Config{
    Header: "X-HTTP-Method-Override",
    AllowedMethods: []string{"PUT", "PATCH", "DELETE"},
}))
```

Run authentication, CSRF and authorization against the effective method. Do
not widen `AllowedMethods` without reviewing caches, routing and access policy.
