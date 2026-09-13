# Configuration

## App Configuration (`fh.Config`)

```go
type Config struct {
    Kernel               KernelConfig
    SecureByDefault      bool
    Mode                 Mode
    Compliance           ComplianceConfig
    Audit                AuditConfig
    Redaction            RedactionConfig
    ReadTimeout          time.Duration            // Default: 10s
    ReadHeaderTimeout    time.Duration            // Default: 5s
    RequestBodyTimeout   time.Duration            // Default: 10s
    WriteTimeout         time.Duration            // Default: 30s
    HandlerTimeout       time.Duration            // Default: 0 (no handler deadline)
    IdleTimeout          time.Duration            // Default: 120s
    TLSHandshakeTimeout  time.Duration            // Default: 10s
    HTTP2IdleTimeout     time.Duration            // Default: 120s
    MaxConnections       int                      // Default: 10,000
    MaxConnectionsPerIP  int                      // Default: 100 in production mode
    MaxInFlightRequests  int64                    // Default: 0; 5,000 with SecureByDefault
    MaxGoroutines        int                      // Default: 0; 20,000 with SecureByDefault
    MaxHeapBytes         uint64                   // Default: 0; 1 GiB with SecureByDefault
    ResourceCheckInterval time.Duration           // Default: 250ms when guards are enabled
    ReadBufferSize       int                      // Default: 16384 (16KB)
    WriteBufferSize      int                      // Default: ReadBufferSize
    MaxRequestBodySize   int                      // Default: 4194304 (4MB)
    MaxHeaderListSize    int                      // Default: 65536 (64KB)
    MaxHeaderCount       int                      // Default: 64
    MaxRequestLineSize   int                      // Default: 8192 (8KB)
    MaxConcurrentStreams uint32                   // Default: 128 (HTTP/2)
    DisableKeepAlive     bool                     // Default: false
    DisableHTTP2         bool                     // Default: false
    DisableH2C           bool                     // Default: false; true in SecureByDefault
    DisablePanicRecovery bool                     // Default: false
    SafeParams           bool                     // Copy route params for use after request
    CaptureResponseBody  bool                     // Default: false
    SendDateHeader       bool                     // Enabled in production mode
    SendKeepAliveHeader  bool
    ServerHeader         string                   // Empty by default
    AllowedHosts         []string
    ContentSecurityPolicy string
    HSTSPreload          bool
    RequestHeadHandler   HandlerFunc              // Optional pre-body admission/auth hook
    StreamRequestBody     bool                     // Defer request body buffering; use Ctx.StreamBody
    ConnContext          func(context.Context, net.Conn) context.Context // Optional per-connection context hook
    BaseContext          func(net.Listener) context.Context             // Optional listener base context hook
    ErrorHandler         ErrorHandler             // Default: logs + problem JSON
    NotFoundHandler      NotFoundHandler          // Default: 404 text/plain
    MethodNotAllowed     MethodNotAllowedHandler  // Default: 405 + Allow header
    OptionsHandler       OptionsHandler           // Default: 204 No Content
    Logger               Logger                   // Default: slog-backed logger
    TemplateEngine       TemplateEngine           // Default: nil
    Reliability          ReliabilityConfig        // Default: disabled
    Environment          Environment              // Default: production
    ErrorOptions         ErrorOptions
    Debug                bool                     // Default: false
    ShutdownTimeout      time.Duration
    StartupBanner        StartupBannerConfig
}
```

The declaration above is a field guide; comments and nested configuration types
are authoritative in `app.go`. Prefer functional options when zero is a
meaningful override. `NewWithConfig` fills zero-valued duration and size fields
from defaults, whereas an option such as `WithWriteTimeout(0)` can explicitly
disable a default.

### Profiles and security boundary

| Constructor/profile | Intended use | Important behavior |
|---|---|---|
| `fh.New()` / `fh.NewProduction()` | Normal services | Production mode, bounded I/O, panic recovery and redaction |
| `fh.New(...WithSecureByDefault(true))` | Internet-facing strict baseline | Strict parsing, h2c disabled, resource ceilings, HSTS and isolation headers |
| `fh.NewEnterprise(...)` | Compliance-oriented deployments | Audit/reliability/compliance defaults; protect evidence endpoints with `WithComplianceEndpointAuth` |
| `fh.NewFast()` | Trusted benchmarks | Trades shutdown and timeout protections for hot-path performance |

No constructor can infer authentication, authorization, CORS, CSRF, allowed
hosts or business-specific rate limits. Configure those explicitly.

### Timeouts

| Field | Default | Description |
|-------|---------|-------------|
| `ReadTimeout` | 10s | Maximum duration for reading the entire request |
| `ReadHeaderTimeout` | 5s | Absolute request-line/header budget after the first byte |
| `RequestBodyTimeout` | 10s | Absolute budget for receiving one request body |
| `WriteTimeout` | 30s | Per-write socket deadline; streaming writes refresh it |
| `HandlerTimeout` | disabled | Optional deadline exposed through `Ctx.Context()` |
| `IdleTimeout` | 120s | Maximum idle time for HTTP/1 keep-alive connections |
| `TLSHandshakeTimeout` | 10s | Maximum TLS handshake duration |
| `HTTP2IdleTimeout` | 120s | Maximum interval between HTTP/2 frames |

### Buffer/Limit Sizes

| Field | Default | Description |
|-------|---------|-------------|
| `ReadBufferSize` | 16KB | Initial buffer size for reading requests |
| `WriteBufferSize` | ReadBufferSize | Initial reusable response buffer size per HTTP/1 connection |
| `MaxRequestBodySize` | 4MB | Maximum allowed request body size |
| `MaxHeaderListSize` | 64KB | Maximum total header size |
| `MaxHeaderCount` | 64 | Maximum number of headers |
| `MaxRequestLineSize` | 8KB | Maximum request line length |
| `MaxConnectionsPerIP` | 100 in production/secure profiles | Maximum simultaneous TCP connections from one socket peer |

### HTTP/2

| Field | Default | Description |
|-------|---------|-------------|
| `MaxConcurrentStreams` | 128 | Maximum concurrent HTTP/2 streams |
| `DisableHTTP2` | false | Disable HTTP/2 support |
| `DisableH2C` | false | Disable cleartext prior-knowledge and upgrade HTTP/2 while retaining TLS ALPN HTTP/2 |

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
app := fh.NewWithConfig(fh.Config{
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

**Apply process-wide at startup:**

```go
fh.SetCodecOptions(fh.CodecOptions{MaxFormPairs: 5000})
```

Codec options are process-wide and concurrency-safe. Configure them during
startup for predictable behavior; `Ctx` does not expose a per-parse override.

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
    QueueLogError                func(string, ...any)
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
    IndexFiles    []string          // Ordered index candidates; supersedes Index
    CacheControl  string            // Supersedes MaxAge
    CacheDuration time.Duration     // File metadata cache duration
    StripSlash    bool              // Trailing slash handling
    ShowHidden    bool              // Default false
    NotFoundHandler fh.HandlerFunc  // Optional SPA/custom fallback
    PreCompressed bool              // Serve .br/.gz sidecars when accepted
    MaxRanges     int               // Default: 16
}
```

See [Static Files](static-files.md) for full details.
