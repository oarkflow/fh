# Static File Serving

## Basic Usage

```go
// Serve files from a directory
app.Static("/static", "./public")

// File at /static/js/app.js -> ./public/js/app.js
```

## Configuration

```go
app.Static("/static", "./public", fh.StaticConfig{
    Compress:      true,     // enable gzip compression for text files
    MaxAge:        86400,    // Cache-Control max-age (seconds)
    Browse:        true,     // enable directory listing
    Index:         "index.html", // index file name
    CacheDuration: 5 * time.Minute, // file metadata cache
    StripSlash:    false,    // trailing slash handling
	MaxRanges:      16,        // maximum ranges in one request
})
```

### Config Fields

| Field | Default | Description |
|-------|---------|-------------|
| `Compress` | false | Gzip compress text responses |
| `MaxAge` | 0 | `Cache-Control: max-age` in seconds |
| `Browse` | false | Enable directory listing |
| `Index` | `"index.html"` | Index file for directories |
| `CacheDuration` | 0 | File metadata cache TTL |
| `StripSlash` | false | Remove trailing slash from path |
| `MaxRanges` | 16 | Maximum byte ranges accepted per request; zero uses 16 |

## From `embed.FS`

```go
import "embed"

//go:embed public/*
var publicFiles embed.FS

app.StaticFS("/static", publicFiles)

// Or with config
app.StaticFS("/static", publicFiles, fh.StaticConfig{
    Compress: true,
    Browse:   false,
})
```

## Security

- **Path traversal protection:** Resolved paths are validated against the root directory. Requests containing `..` segments are rejected.
- **Safe path resolution:** All paths are cleaned and checked before serving.

## Caching Features

### ETag

Auto-generated ETags based on file modification time and size. Conditional requests with `If-None-Match` return 304 Not Modified.

### Cache-Control

Set `MaxAge` in `StaticConfig` to control browser caching:

```go
app.Static("/static", "./public", fh.StaticConfig{
    MaxAge: 3600, // 1 hour
})
```

### Conditional Requests

- `If-Modified-Since` — returns 304 if file hasn't changed
- `If-None-Match` — returns 304 if ETag matches
- `If-Range` — for range requests

### Range Requests

Partial content (206 Partial Content) is supported for media files and large downloads,
including bounded multipart responses for multiple ranges.

### Content-Type Detection

Content-Type is determined by file extension using the built-in MIME type map.

### Compression

When `Compress: true`, text-based responses (HTML, CSS, JS, JSON, XML, SVG, etc.) are gzip-compressed if the client sends `Accept-Encoding: gzip`.

## Enhanced Static Middleware (`mw/static`)

For more control, use the `mw/static` middleware:

```go
import "github.com/oarkflow/fh/mw/static"

app.Use(static.New(static.Config{
    Root:    "./public",
    Prefix:  "/static",
    Browse:  true,
    MaxAge:  3600,
    Compress: true,
    Download: true,           // add Content-Disposition: attachment
    SafePaths: []string{"/static/public"}, // allowed paths
}))
```

See [Middleware](middleware.md) for the full `mw/static` middleware documentation.
