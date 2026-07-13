# Accept-Query middleware

`acceptquery` advertises HTTP QUERY formats using the RFC 10008 Structured
Fields representation and can reject unsupported QUERY content types.

```go
formats := acceptquery.New(acceptquery.Config{
    MediaTypes: []string{"application/jsonpath", "application/sql; charset=UTF-8"},
    Enforce: true,
    RequireContentType: true,
})

app.Query("/search", formats, search)
app.Get("/search", formats, describeSearch)
```

Attach it to every representation of a resource that should advertise QUERY.

