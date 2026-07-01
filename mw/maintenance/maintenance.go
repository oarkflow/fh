package maintenance

import (
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type Switch struct {
	enabled atomic.Bool
	message atomic.Value
}

func NewSwitch() *Switch {
	s := &Switch{}
	s.message.Store("maintenance")
	return s
}
func (s *Switch) Enable(msg ...string) {
	if len(msg) > 0 && msg[0] != "" {
		s.message.Store(msg[0])
	}
	s.enabled.Store(true)
}
func (s *Switch) Disable()      { s.enabled.Store(false) }
func (s *Switch) Enabled() bool { return s.enabled.Load() }
func (s *Switch) Message() string {
	msg, _ := s.message.Load().(string)
	return msg
}

type Config struct {
	Switch       *Switch
	BypassHeader string
	BypassToken  string
	RetryAfter   time.Duration

	// StatusCode is used for direct maintenance responses. Default: 503.
	StatusCode int

	// Renderer allows custom maintenance page rendering. When Path is configured,
	// non-bypassed requests are redirected to Path and requests to Path are served
	// by Renderer to avoid redirect loops.
	Renderer fh.Handler

	// Path is the public maintenance page endpoint, for example "/maintenance".
	// When Path is configured with Renderer, non-bypassed requests are redirected
	// to this path instead of receiving JSON immediately. Requests to Path call the
	// configured Renderer directly.
	Path string

	// RedirectCode is used when Path and Renderer are configured. Default: 302.
	RedirectCode int
}

func New(cfg Config) fh.HandlerFunc {
	cfg = normalize(cfg)
	return func(c fh.Ctx) error {
		if !cfg.Switch.Enabled() {
			return c.Next()
		}
		if cfg.BypassHeader != "" && cfg.BypassToken != "" && fh.ConstantTimeEqual(c.Get(cfg.BypassHeader), cfg.BypassToken) {
			return c.Next()
		}
		setRetryAfter(c, cfg.RetryAfter)
		if cfg.Renderer != nil {
			if cfg.Path != "" && cleanPath(c.Path()) != cfg.Path {
				return c.Redirect(cfg.Path, cfg.RedirectCode)
			}
			return cfg.Renderer(c)
		}
		body := any(fh.Map{"error": "maintenance", "message": cfg.Switch.Message()})
		return c.Status(cfg.StatusCode).JSON(body)
	}
}

func normalize(cfg Config) Config {
	if cfg.Switch == nil {
		cfg.Switch = NewSwitch()
	}
	if cfg.RetryAfter <= 0 {
		cfg.RetryAfter = time.Minute
	}
	if cfg.StatusCode <= 0 {
		cfg.StatusCode = fh.StatusServiceUnavailable
	}
	if cfg.RedirectCode <= 0 {
		cfg.RedirectCode = fh.StatusFound
	}
	cfg.Path = cleanPath(cfg.Path)
	return cfg
}

func setRetryAfter(c fh.Ctx, d time.Duration) {
	if d > 0 {
		c.Set("Retry-After", strconv.Itoa(int(d.Seconds())))
	}
}

func cleanPath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if len(p) > 1 {
		p = strings.TrimRight(p, "/")
	}
	if p == "" {
		return "/"
	}
	return p
}
