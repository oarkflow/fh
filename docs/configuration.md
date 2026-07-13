# Configuration

## App Configuration (`fh.Config`)

```go
type Config struct {
    ReadTimeout          time.Duration            // Default: 10s
	ReadHeaderTimeout    time.Duration            // Default: 5s (production)
    WriteTimeout         time.Duration            // Default: 10s
    IdleTimeout          time.Duration            // Default: 60s
    MaxConnections       int                      // Default: 0 (unlimited)
    ReadBufferSize       int                      // Default: 16384 (16KB)
    MaxRequestBodySize   int                      // Default: 4194304 (4MB)
    MaxHeaderListSize    int                      // Default: 65536 (64KB)
    MaxHeaderCount       int                      // Default: 64
    MaxRequestLineSize   int                      // Default: 8192 (8KB)
    MaxConcurrentStreams uint32                   // Default: 128 (HTTP/2)
    DisableKeepAlive     bool                     // Default: false
    DisableHTTP2         bool                     // Default: false
    ErrorHandler         ErrorHandler             // Default: logs + problem JSON
    NotFoundHandler      NotFoundHandler          // Default: 404 text/plain
    MethodNotAllowed     MethodNotAllowedHandler  // Default: 405 + Allow header
    OptionsHandler       OptionsHandler           // Default: 204 No Content
    Logger               *log.Logger              // Default: log.Default()
    TemplateEngine       TemplateEngine           // Default: nil
    Reliability          ReliabilityConfig        // Default: disabled
    Debug                bool                     // Default: false
}
```

### Timeouts

| Field | Default | Description |
|-------|---------|-------------|
| `ReadTimeout` | 10s | Maximum duration for reading the entire request |
| `ReadHeaderTimeout` | 5s | Absolute request-line/header budget after the first byte |
| `WriteTimeout` | 10s | Maximum duration for writing the response |
| `IdleTimeout` | 60s | Maximum idle time for keep-alive connections |

### Buffer/Limit Sizes

| Field | Default | Description |
|-------|---------|-------------|
| `ReadBufferSize` | 16KB | Initial buffer size for reading requests |
| `MaxRequestBodySize` | 4MB | Maximum allowed request body size |
| `MaxHeaderListSize` | 64KB | Maximum total header size |
| `MaxHeaderCount` | 64 | Maximum number of headers |
| `MaxRequestLineSize` | 8KB | Maximum request line length |

### HTTP/2

| Field | Default | Description |
|-------|---------|-------------|
| `MaxConcurrentStreams` | 128 | Maximum concurrent HTTP/2 streams |
| `DisableHTTP2` | false | Disable HTTP/2 support |

### Behavior

| Field | Default | Description |
|-------|---------|-------------|
| `DisableKeepAlive` | false | Disable HTTP/1.1 keep-alive |
| `Debug` | false | Enable debug logging |

### Custom Handlers

| Field | Default | Description |
|-------|---------|-------------|
| `ErrorHandler` | logs + problem JSON | Custom error response handler |
| `NotFoundHandler` | 404 text/plain | Custom 404 handler |
| `MethodNotAllowed` | 405 + Allow header | Custom 405 handler |
| `OptionsHandler` | 204 No Content | Custom OPTIONS handler |

### Template Engine

```go
app := fh.New(fh.Config{
    TemplateEngine: &MyTemplateEngine{},
})
// Then in handler:
c.Render("index", data)          // render without layout
c.Render("index", data, "main")  // render with layout
```

The `TemplateEngine` interface:

```go
type TemplateEngine interface {
    Render(w io.Writer, name string, data any, layout ...string) error
}
```

---

## Codec Options (`fh.CodecOptions`)

```go
type CodecOptions struct {
    MaxFormPairs          int   // Default: 10,000
    MaxFormKeyBytes       int   // Default: 4KB
    MaxFormValueBytes     int   // Default: 4MB
    MaxFormDepth          int   // Default: 32
    MaxMultipartParts     int   // Default: 10,000
    MaxMultipartFieldSize int64 // Default: 8MB
    MaxMultipartFileSize  int64 // Default: 64MB
    MaxNDJSONLineBytes    int   // Default: 8MB
    MaxCSVRecordBytes     int   // Default: 8MB
}
```

### Form Parsing

| Field | Default | Description |
|-------|---------|-------------|
| `MaxFormPairs` | 10,000 | Maximum form key-value pairs |
| `MaxFormKeyBytes` | 4KB | Maximum form key length |
| `MaxFormValueBytes` | 4MB | Maximum form value length |
| `MaxFormDepth` | 32 | Maximum nesting depth for bracket notation |

### Multipart

| Field | Default | Description |
|-------|---------|-------------|
| `MaxMultipartParts` | 10,000 | Maximum multipart parts |
| `MaxMultipartFieldSize` | 8MB | Maximum field size |
| `MaxMultipartFileSize` | 64MB | Maximum file upload size |

### Other

| Field | Default | Description |
|-------|---------|-------------|
| `MaxNDJSONLineBytes` | 8MB | Maximum NDJSON line length |
| `MaxCSVRecordBytes` | 8MB | Maximum CSV record length |

**Apply globally:**

```go
fh.DefaultCodecOptions.MaxFormPairs = 5000
```

**Apply per-parse:**

```go
c.BodyParserWithOpts(&data, fh.CodecOptions{
    MaxFormPairs: 5000,
})
```

---

## Reliability Configuration (`fh.ReliabilityConfig`)

```go
type ReliabilityConfig struct {
    Enabled                     bool
    DataDir                     string            // Default: .fh-data
    JournalEnabled              bool
    IdempotencyEnabled          bool
    QueueEnabled                bool
    JournalStore                RequestJournalStore
    IdempotencyRepository       IdempotencyRepository
    QueueStorage                QueueStorage
    RequestIDHeader             string            // Default: X-Request-ID
    IdempotencyHeader           string            // Default: Idempotency-Key
    RequireIdempotencyKey       bool
    IdempotencyTTL              time.Duration
    IdempotencyProcessingStatus int
    IdempotencyReplayHeaderValue string           // Default: replayed
    QueueDir                    string
    QueueWorkers                int
    QueueMaxAttempts            int               // Default: 5
    QueuePollInterval           time.Duration
    QueueBackoff                time.Duration
    QueueConcurrencyLimitByKey  bool
}
```

See [Reliability Layer](reliability.md) for full details.

---

## Static File Serving Configuration (`fh.StaticConfig`)

```go
type StaticConfig struct {
    Compress      bool              // Gzip compression for text responses
    MaxAge        int               // Cache-Control max-age in seconds
    Browse        bool              // Directory listing enabled
    Index         string            // Index filename (default: "index.html")
    CacheDuration time.Duration     // File metadata cache duration
    StripSlash    bool              // Trailing slash handling
}
```

See [Static Files](static-files.md) for full details.
