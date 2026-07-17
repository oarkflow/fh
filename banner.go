package fh

import (
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// StartupBannerConfig controls the pretty ASCII startup message shown when the
// application starts serving. The default is enabled and writes to stdout.
// Disable it in tests, machine-readable logging environments, or embedded use.
type StartupBannerConfig struct {
	// Disabled suppresses the startup banner entirely.
	Disabled bool
	// Name is the framework/application name shown in the banner. Default: "fh".
	Name string
	// Version is an optional application/framework version string.
	Version string
	// Subtitle is an optional short description below the title.
	Subtitle string
	// Scheme is used to build the displayed URL. Default: "http".
	Scheme string
	// Address overrides the listener address shown in the banner.
	Address string
	// ASCIIArt overrides the default small ASCII wordmark. Set to "-" to hide it.
	ASCIIArt string
	// Color enables ANSI color output. It is disabled by default so logs remain
	// clean when stdout is captured by process managers.
	Color bool
	// Writer receives the banner. Default: os.Stdout.
	Writer io.Writer
	// Render allows complete custom rendering. When set, fh passes StartupBannerData
	// and prints the returned string as-is.
	Render func(StartupBannerData) string
	// ExtraLines are appended as key/value rows after the built-in rows.
	ExtraLines []StartupBannerLine
	// HideRoutes hides the route count.
	HideRoutes bool
	// HidePID hides the current process id.
	HidePID bool
	// HideGoVersion hides runtime.Version().
	HideGoVersion bool
	// HideMode hides the configured fh mode.
	HideMode bool
}

// StartupBannerLine is one key/value row inside the startup banner.
type StartupBannerLine struct {
	Key   string
	Value string
}

// StartupBannerData is passed to custom startup banner renderers.
type StartupBannerData struct {
	Name      string
	Version   string
	Subtitle  string
	URL       string
	Address   string
	Scheme    string
	Routes    int
	PID       int
	GoVersion string
	Mode      Mode
	HTTP2     bool
	Extra     []StartupBannerLine
}

// WithStartupBanner replaces the whole startup banner configuration.
func WithStartupBanner(cfg StartupBannerConfig) Option {
	return func(c *Config) { c.StartupBanner = cfg }
}

// WithStartupBannerDisabled enables/disables the startup banner. Passing true
// disables it.
func WithStartupBannerDisabled(disabled bool) Option {
	return func(c *Config) { c.StartupBanner.Disabled = disabled }
}

// WithStartupBannerOutput sets the destination for the startup banner.
func WithStartupBannerOutput(w io.Writer) Option {
	return func(c *Config) { c.StartupBanner.Writer = w }
}

// WithStartupBannerName sets the displayed application/framework name.
func WithStartupBannerName(name string) Option {
	return func(c *Config) { c.StartupBanner.Name = name }
}

// WithStartupBannerVersion sets the displayed version string.
func WithStartupBannerVersion(version string) Option {
	return func(c *Config) { c.StartupBanner.Version = version }
}

// WithStartupBannerSubtitle sets the displayed subtitle.
func WithStartupBannerSubtitle(subtitle string) Option {
	return func(c *Config) { c.StartupBanner.Subtitle = subtitle }
}

func (a *App) printStartupBanner(ln net.Listener) {
	if a == nil || ln == nil || a.cfg.StartupBanner.Disabled {
		return
	}
	cfg := a.cfg.StartupBanner
	w := cfg.Writer
	if w == nil {
		w = os.Stdout
	}
	data := a.startupBannerData(ln)
	out := ""
	if cfg.Render != nil {
		out = cfg.Render(data)
	} else {
		out = RenderStartupBanner(cfg, data)
	}
	if out == "" {
		return
	}
	if !strings.HasSuffix(out, "\n") {
		out += "\n"
	}
	_, _ = io.WriteString(w, out)
}

func (a *App) startupBannerData(ln net.Listener) StartupBannerData {
	cfg := a.cfg.StartupBanner
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		name = "fh"
	}
	scheme := strings.TrimSpace(cfg.Scheme)
	if scheme == "" {
		scheme = "http"
	}
	addr := strings.TrimSpace(cfg.Address)
	if addr == "" && ln != nil && ln.Addr() != nil {
		addr = ln.Addr().String()
	}
	url := startupURL(scheme, addr)
	extra := make([]StartupBannerLine, 0, len(cfg.ExtraLines)+4)
	kernel := a.KernelRuntimeInfo()
	if kernel.Enabled {
		extra = append(extra,
			StartupBannerLine{Key: "Transport", Value: string(kernel.Backend)},
			StartupBannerLine{Key: "Reactors", Value: strconv.Itoa(kernel.Reactors)},
			StartupBannerLine{Key: "ReusePort", Value: enabledDisabled(kernel.ReusePort)},
		)
		if kernel.XDPAttached {
			extra = append(extra, StartupBannerLine{Key: "XDP", Value: kernel.XDPInterface})
		}
	}
	extra = append(extra, cfg.ExtraLines...)
	return StartupBannerData{
		Name:      name,
		Version:   strings.TrimSpace(cfg.Version),
		Subtitle:  strings.TrimSpace(cfg.Subtitle),
		URL:       url,
		Address:   addr,
		Scheme:    scheme,
		Routes:    len(a.Routes()),
		PID:       os.Getpid(),
		GoVersion: runtime.Version(),
		Mode:      a.cfg.Mode,
		HTTP2:     !a.cfg.DisableHTTP2,
		Extra:     extra,
	}
}

// RenderStartupBanner returns the default pretty ASCII startup banner. It is
// exported so tests and CLIs can render/preview the banner without starting a
// listener.
func RenderStartupBanner(cfg StartupBannerConfig, data StartupBannerData) string {
	lines := make([]string, 0, 16)
	if cfg.ASCIIArt != "-" {
		art := cfg.ASCIIArt
		if strings.TrimSpace(art) == "" {
			art = defaultStartupASCII(data.Name)
		}
		for _, line := range strings.Split(strings.TrimRight(art, "\n"), "\n") {
			if strings.TrimSpace(line) != "" {
				lines = append(lines, line)
			}
		}
	}
	title := data.Name
	if data.Version != "" {
		title += " " + data.Version
	}
	rows := []StartupBannerLine{{Key: "Name", Value: title}}
	if data.Subtitle != "" {
		rows = append(rows, StartupBannerLine{Key: "Info", Value: data.Subtitle})
	}
	if data.URL != "" {
		rows = append(rows, StartupBannerLine{Key: "URL", Value: data.URL})
	} else if data.Address != "" {
		rows = append(rows, StartupBannerLine{Key: "Address", Value: data.Address})
	}
	if !cfg.HideRoutes {
		rows = append(rows, StartupBannerLine{Key: "Routes", Value: strconv.Itoa(data.Routes)})
	}
	if !cfg.HideMode && data.Mode != "" {
		rows = append(rows, StartupBannerLine{Key: "Mode", Value: string(data.Mode)})
	}
	rows = append(rows, StartupBannerLine{Key: "HTTP/2", Value: enabledDisabled(data.HTTP2)})
	if !cfg.HideGoVersion {
		rows = append(rows, StartupBannerLine{Key: "Go", Value: data.GoVersion})
	}
	if !cfg.HidePID {
		rows = append(rows, StartupBannerLine{Key: "PID", Value: strconv.Itoa(data.PID)})
	}
	rows = append(rows, data.Extra...)

	keyWidth := 0
	valWidth := 0
	for _, row := range rows {
		if n := len(row.Key); n > keyWidth {
			keyWidth = n
		}
		if n := len(row.Value); n > valWidth {
			valWidth = n
		}
	}
	if keyWidth < 4 {
		keyWidth = 4
	}
	if valWidth < 8 {
		valWidth = 8
	}
	inner := keyWidth + valWidth + 5
	border := "+" + strings.Repeat("-", inner) + "+"
	lines = append(lines, border)
	for _, row := range rows {
		lines = append(lines, fmt.Sprintf("| %-*s : %-*s |", keyWidth, row.Key, valWidth, row.Value))
	}
	lines = append(lines, border)
	out := strings.Join(lines, "\n")
	if cfg.Color {
		return "\033[36m" + out + "\033[0m"
	}
	return out
}

func defaultStartupASCII(name string) string {
	name = strings.TrimSpace(name)
	if strings.EqualFold(name, "fh") || name == "" {
		return "   __ _     \n  / _| |__  \n | |_| '_ \\ \n |  _| | | |\n |_| |_| |_|"
	}
	return strings.ToUpper(name)
}

func startupURL(scheme, addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	if strings.Contains(addr, "://") {
		return addr
	}
	host := addr
	if strings.HasPrefix(host, ":") {
		host = "127.0.0.1" + host
	} else if strings.HasPrefix(host, "[::]") {
		host = strings.Replace(host, "[::]", "127.0.0.1", 1)
	} else if strings.HasPrefix(host, "0.0.0.0:") {
		host = strings.Replace(host, "0.0.0.0", "127.0.0.1", 1)
	}
	return strings.TrimRight(scheme, ":/") + "://" + host
}

func enabledDisabled(ok bool) string {
	if ok {
		return "enabled"
	}
	return "disabled"
}
