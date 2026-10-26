# Streaming upload server

Run from the repository root:

```sh
go run ./examples/streaming-upload
```

Upload a file and inspect its streamed SHA-256 digest:

```sh
curl -i -F 'title=demo' -F 'file=@go.mod' http://127.0.0.1:8085/upload
curl 'http://127.0.0.1:8085/metrics?format=prometheus'
```

The server enables `WithStreamRequestBody(true)`. `bodylimit` caps every
request at 8 MiB, `contract` caps `/upload` at 6 MiB and requires multipart
content, and `StreamMultipart` caps each file at 5 MiB. The handler hashes
the file directly from its part reader without keeping it in memory. It
returns any read error so an exceeded limit becomes HTTP 413.

The Prometheus output includes counts and duration histograms labeled by
method, route template, and status class. `MaxRoutes` bounds route series.
The `/sample` endpoint is used by the companion
[bounded download example](../bounded-download/README.md).

The default listener is loopback only. Add authentication and protect the
metrics endpoint before exposing a similar service publicly.
