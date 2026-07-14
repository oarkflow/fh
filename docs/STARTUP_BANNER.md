# Startup Banner

fh prints a Fiber-style ASCII startup banner when the server starts listening. It is enabled by default, fully configurable, custom-renderable, and can be disabled for machine-readable logging environments.

## Default

```go
app := fh.New()
app.Listen(":8080")
```

Prints the fh wordmark plus address, PID, Go version, mode, HTTP/2 status, and route count.

## Configuring

```go
app := fh.New(fh.WithStartupBanner(fh.StartupBannerConfig{
    Name:     "billing-api",
    Version:  "v2.4.1",
    Subtitle: "Orgware billing service",
    Color:    true,
}))
```

| Field | Description |
|---|---|
| `Disabled` | Suppress the banner entirely |
| `Name` | Application name shown in the banner (default: `"fh"`) |
| `Version` | Optional application/framework version string |
| `Subtitle` | Optional short description line |
| `Scheme` | Scheme used to build the displayed URL (default: `"http"`) |
| `Address` | Overrides the listener address shown in the banner |
| `ASCIIArt` | Overrides the default wordmark; `"-"` hides it |
| `Color` | Enable ANSI color output (default: `false`, so captured logs stay clean) |
| `Writer` | Destination for the banner (default: `os.Stdout`) |
| `Render` | Full custom renderer — receives `StartupBannerData`, returns the string to print |
| `ExtraLines` | Extra key/value rows appended after the built-in rows |
| `HideRoutes`, `HidePID`, `HideGoVersion`, `HideMode` | Hide individual built-in rows |

## Disabling

```go
app := fh.New(fh.WithStartupBannerDisabled(true))
```

## Custom rendering

```go
app := fh.New(fh.WithStartupBanner(fh.StartupBannerConfig{
    Render: func(d fh.StartupBannerData) string {
        return fmt.Sprintf("%s listening on %s (routes: %d)\n", d.Name, d.URL, d.Routes)
    },
}))
```

`StartupBannerData` carries `Name`, `Version`, `Subtitle`, `URL`, `Address`, `Scheme`, `Routes`, `PID`, `GoVersion`, `Mode`, `HTTP2`, and `Extra`.

## Standalone rendering

`fh.RenderStartupBanner(cfg, data)` renders a banner string without printing it — useful for tests or custom log pipelines.
