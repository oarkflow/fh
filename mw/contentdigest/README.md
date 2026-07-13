# Content-Digest middleware

`contentdigest` verifies RFC 9530 `Content-Digest` request fields and can add a
digest to responses. Supported algorithms are `sha-256` and `sha-512`.

```go
app.Post("/artifacts",
    contentdigest.New(contentdigest.Config{
        RequireRequest: true,
        Response: contentdigest.ResponseWhenRequested,
    }),
    upload,
)
```

Place response compression before this middleware when the digest must cover
the compressed message content. Verification uses constant-time comparison.

