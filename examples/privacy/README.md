# Privacy Filter Example

Demonstrates privacy-aware telemetry with header allowlists, query redaction, and path templating.

## What it does

- Headers not in the allowlist are stripped from telemetry
- Sensitive query params (token, api_key) are redacted
- Numeric path segments are templated (/users/123 -> /users/:id)
- Per-tenant policies control what each tenant can emit

## Run

```bash
go run examples/privacy/main.go
```

## Test

```bash
# Path templated to /users/:id in telemetry
curl http://localhost:3000/users/123

# Query value redacted
curl "http://localhost:3000/search?token=secret123&q=test"
```
