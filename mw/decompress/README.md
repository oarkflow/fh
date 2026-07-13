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

