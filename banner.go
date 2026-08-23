package fh

import (
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"unicode/utf8"
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

// WithStartupBannerColor enables/disables ANSI color in the startup banner.
func WithStartupBannerColor(enabled bool) Option {
	return func(c *Config) { c.StartupBanner.Color = enabled }
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
				lines = append(lines, startupColor(cfg.Color, startupCyanBold, line))
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
	lines = append(lines, startupColor(cfg.Color, startupBorder, border))
	for _, row := range rows {
		key := startupColor(cfg.Color, startupKey, row.Key)
		value := startupBannerValue(cfg.Color, row)
		lines = append(lines,
			startupColor(cfg.Color, startupBorder, "|")+" "+
				startupPadRight(key, keyWidth)+" "+
				startupColor(cfg.Color, startupDivider, ":")+" "+
				startupPadRight(value, valWidth)+" "+
				startupColor(cfg.Color, startupBorder, "|"),
		)
	}
	lines = append(lines, startupColor(cfg.Color, startupBorder, border))
	return strings.Join(lines, "\n")
}

const (
	startupReset    = "\033[0m"
	startupBorder   = "\033[38;5;244m"
	startupCyanBold = "\033[1;38;5;51m"
	startupKey      = "\033[38;5;147m"
	startupDivider  = "\033[38;5;245m"
	startupValue    = "\033[38;5;255m"
	startupURLColor = "\033[4;38;5;81m"
	startupGreen    = "\033[38;5;82m"
	startupYellow   = "\033[38;5;221m"
	startupBlue     = "\033[38;5;75m"
	startupMagenta  = "\033[38;5;213m"
)

func startupBannerValue(color bool, row StartupBannerLine) string {
	if !color {
		return row.Value
	}
	switch strings.ToLower(row.Key) {
	case "url":
		return startupHyperlink(row.Value, startupColor(true, startupURLColor, row.Value))
	case "mode":
		switch strings.ToLower(row.Value) {
		case string(ModeProduction):
			return startupColor(true, startupGreen, row.Value)
		case string(ModeDevelopment):
			return startupColor(true, startupYellow, row.Value)
		default:
			return startupColor(true, startupBlue, row.Value)
		}
	case "http/2", "reuseport":
		if strings.EqualFold(row.Value, "enabled") {
			return startupColor(true, startupGreen, row.Value)
		}
		return startupColor(true, startupYellow, row.Value)
	case "name":
		return startupColor(true, startupMagenta, row.Value)
	default:
		return startupColor(true, startupValue, row.Value)
	}
}

func startupColor(enabled bool, code, text string) string {
	if !enabled || text == "" {
		return text
	}
	return code + text + startupReset
}

func startupHyperlink(url, text string) string {
	if strings.TrimSpace(url) == "" {
		return text
	}
	return "\033]8;;" + url + "\033\\" + text + "\033]8;;\033\\"
}

func startupPadRight(s string, width int) string {
	if pad := width - startupVisibleWidth(s); pad > 0 {
		return s + strings.Repeat(" ", pad)
	}
	return s
}

func startupVisibleWidth(s string) int {
	width := 0
	for i := 0; i < len(s); {
		if s[i] == '\033' {
			i++
			if i < len(s) && s[i] == ']' {
				i++
				for i < len(s) {
					if s[i] == '\a' {
						i++
						break
					}
					if s[i] == '\033' && i+1 < len(s) && s[i+1] == '\\' {
						i += 2
						break
					}
					i++
				}
				continue
			}
			if i < len(s) && s[i] == '[' {
				i++
				for i < len(s) {
					b := s[i]
					i++
					if b >= '@' && b <= '~' {
						break
					}
				}
				continue
			}
			for i < len(s) {
				b := s[i]
				i++
				if b >= '@' && b <= '~' {
					break
				}
			}
			continue
		}
		_, n := utf8.DecodeRuneInString(s[i:])
		width++
		i += n
	}
	return width
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
