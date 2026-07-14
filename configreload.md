# configreload

Atomic configuration reload with generation tracking for fh. Reloads configuration, routes, policies, and certificates as a single atomic operation.

## Why

Partial configuration reloads cause hard-to-diagnose failures. Reloading routes without certificates, or policies without routes, leaves the system in an inconsistent state. Atomic reload ensures all components transition together, with automatic rollback on failure. Generation tracking makes it trivial to correlate errors with specific configuration versions during deployments.

## Features

- 9-step transactional reload sequence
- Automatic rollback on validation/health check failure
- Configuration, route, policy, and certificate generation counters
- `X-Config-Generation` headers on every response
- Reload hooks for observability
- Custom validation and health check functions

## Usage

```go
app := fh.New()

reloader := fh.NewConfigReloader(app)

// Register hooks
reloader.OnReload(func(new, old *fh.ConfigGeneration) {
    log.Printf("Config reloaded: %s -> %s", old, new)
})

reloader.SetValidation(func(cfg *fh.Config) error {
    if cfg.ReadTimeout <= 0 {
        return fmt.Errorf("read_timeout must be positive")
    }
    return nil
})

// Apply generation headers
app.Use(fh.ConfigGenerationMiddleware(reloader))

app.Get("/", func(c fh.Ctx) error {
    gen := reloader.Generation()
    return c.JSON(fh.Map{
        "config_generation": gen.ConfigGeneration,
        "route_generation":  gen.RouteGeneration,
    })
})

// Trigger reload
app.Post("/admin/reload", func(c fh.Ctx) error {
    result := reloader.Reload(&fh.Config{
        ReadTimeout:  15 * time.Second,
        WriteTimeout: 30 * time.Second,
    })
    return c.JSON(result)
})
```

## ConfigGeneration

```go
type ConfigGeneration struct {
    ConfigGeneration    uint64
    RouteGeneration     uint64
    PolicyGeneration    uint64
    CertificateGeneration uint64
    Timestamp           time.Time
}
```

## Reload Sequence

1. Parse new configuration
2. Validate routes and policies
3. Compile routing structures
4. Load certificates and keys
5. Initialize dependencies
6. Run health checks
7. Swap configuration atomically
8. Drain old resources
9. Roll back automatically on failure
