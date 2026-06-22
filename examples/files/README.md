# Upload, download, files, and errors

Run from the repository root:

```sh
go run ./examples/files
```

Upload a file and keep the returned `stored` name:

```sh
curl -i -F 'document=@./README.md' -F 'title=Framework README' \
  http://localhost:3000/upload
```

Serve it inline, download it, or request a range:

```sh
curl -i http://localhost:3000/files/STORED_NAME
curl -i -OJ http://localhost:3000/download/STORED_NAME
curl -i -H 'Range: bytes=0-99' http://localhost:3000/download/STORED_NAME
```

Conditional requests are supported through `ETag`, `If-None-Match`,
`If-Match`, `Last-Modified`, `If-Modified-Since`, and
`If-Unmodified-Since`.

The error routes demonstrate Problem Details and stable error codes:

```sh
curl -i http://localhost:3000/errors/conflict
curl -i http://localhost:3000/errors/rate-limit
curl -i http://localhost:3000/errors/internal
```

Internal details are masked by default. Set `DEBUG=1` only during local
development to include the underlying error message. Set `UPLOAD_DIR` to use a
different storage directory.
