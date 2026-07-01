package retrybudget

import (
	"github.com/oarkflow/fh"
	"strconv"
	"sync"
	"time"
)

type KeyFunc func(fh.Ctx) string
type Config struct {
	MaxRetries int
	Window     time.Duration
	Header     string
	Key        KeyFunc
	Error      func(fh.Ctx) error
}
type bucket struct {
	used  int
	reset time.Time
}
type limiter struct {
	mu      sync.Mutex
	buckets map[string]bucket
	cfg     Config
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.Window <= 0 {
		cfg.Window = time.Minute
	}
	if cfg.Header == "" {
		cfg.Header = "X-Retry-Attempt"
	}
	if cfg.Error == nil {
		cfg.Error = func(c fh.Ctx) error {
			c.Set("Retry-After", strconv.Itoa(int(cfg.Window.Seconds())))
			return c.Status(fh.StatusTooManyRequests).JSON(fh.Map{"error": "retry_budget_exceeded"})
		}
	}
	l := &limiter{buckets: map[string]bucket{}, cfg: cfg}
	return l.Handle
}
func (l *limiter) Handle(c fh.Ctx) error {
	attempt := c.Get(l.cfg.Header)
	if attempt == "" || attempt == "0" {
		return c.Next()
	}
	key := c.IP()
	if l.cfg.Key != nil {
		key = l.cfg.Key(c)
	}
	now := time.Now()
	l.mu.Lock()
	b := l.buckets[key]
	if b.reset.IsZero() || now.After(b.reset) {
		b = bucket{reset: now.Add(l.cfg.Window)}
	}
	if b.used >= l.cfg.MaxRetries {
		l.buckets[key] = b
		l.mu.Unlock()
		return l.cfg.Error(c)
	}
	b.used++
	l.buckets[key] = b
	l.mu.Unlock()
	return c.Next()
}
