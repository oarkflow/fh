# Request decompression middleware

`decompress` expands `Content-Encoding: gzip` request bodies with both an
absolute output limit and an expansion-ratio limit.

```go
app.Post("/ingest",
    decompress.New(decompress.Config{
        MaxSize: 8 << 20,
        MaxExpansionRatio: 50,
    }),
    ingest,
)
```

The compressed wire body remains subject to the server's
`MaxRequestBodySize`; `MaxSize` separately bounds the decoded body.
When the framework context buffers the decoded body, its retained capacity is
reserved incrementally against `WithMaxBufferedRequestBytes`; exhausted budget
returns a temporary `503` before the expanded body is retained.
