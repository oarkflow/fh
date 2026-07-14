# privacy

Privacy-aware telemetry middleware for fh. Controls what data enters logs, traces, metrics, and audit systems.

## Why

Observability systems frequently leak sensitive data. Headers contain auth tokens, query strings contain API keys, paths contain user IDs, and bodies contain PII. Without privacy controls, a logging breach exposes everything. This middleware enforces data minimization at the telemetry boundary.

## Features

- Header allowlists (only safe headers appear in telemetry)
- Query string redaction for sensitive parameters
- Path parameter templating (`/users/123` -> `/users/:id`)
- Automatic secret detection and redaction
- Per-field SHA-256 hashing for correlation without exposure
- Per-tenant privacy policies
- Never-export field lists
- Body logging disabled by default

## Usage

```go
import "github.com/oarkflow/fh/mw/privacy"

app := fh.New()

filter := privacy.New(privacy.Config{
    HeaderAllowlist: []string{"Content-Type", "Accept", "X-Request-ID"},
    QueryRedact:     []string{"token", "api_key", "secret"},
    PathTemplate:    true,
    SecretDetection: true,
    NeverExport:     []string{"password", "ssn", "credit_card"},
})

app.Use(privacy.PrivacyMiddleware(filter))

app.Get("/users/:id", func(c fh.Ctx) error {
    // /users/123 appears as /users/:id in all telemetry
    return c.JSON(fh.Map{"user_id": c.Param("id")})
})
```

## Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| HeaderAllowlist | `[]string` | Content-Type, Accept, User-Agent, X-Request-ID | Headers allowed in telemetry |
| QueryRedact | `[]string` | [] | Query params to redact |
| PathTemplate | `bool` | true | Replace numeric path segments with :id |
| SecretDetection | `bool` | true | Auto-detect and redact secrets |
| BodyLogging | `bool` | false | Enable body capture in logs |
| HashFields | `[]string` | [] | Fields to hash instead of redact |
| NeverExport | `[]string` | [] | Fields that never appear in telemetry |
| TenantLogPolicies | `map[string]*TenantPolicy` | nil | Per-tenant overrides |

## TenantPolicy

```go
type TenantPolicy struct {
    HeaderAllowlist []string   // Override global header list
    BodyLogging     bool       // Enable body logging for this tenant
    SamplingRate    float64    // 0.0-1.0 sampling rate
    NeverExport     []string   // Additional never-export fields
}
```
