package adaptiveconcurrency

import (
	"github.com/oarkflow/fh"
	"sync"
	"time"
)

type Config struct {
	InitialLimit  int
	MinLimit      int
	MaxLimit      int
	TargetLatency time.Duration
	Window        int
	Error         func(fh.Ctx, int) error
}

type limiter struct {
	mu       sync.Mutex
	inFlight int
	limit    int
	min      int
	max      int
	target   time.Duration
	window   int
	samples  int
	total    time.Duration
	cfg      Config
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.InitialLimit <= 0 {
		cfg.InitialLimit = 128
	}
	if cfg.MinLimit <= 0 {
		cfg.MinLimit = 1
	}
	if cfg.MaxLimit < cfg.InitialLimit {
		cfg.MaxLimit = cfg.InitialLimit
	}
	if cfg.TargetLatency <= 0 {
		cfg.TargetLatency = 100 * time.Millisecond
	}
	if cfg.Window <= 0 {
		cfg.Window = 100
	}
	if cfg.Error == nil {
		cfg.Error = func(c fh.Ctx, limit int) error {
			c.Set("Retry-After", "1")
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{"error": "adaptive_concurrency_limited", "limit": limit})
		}
	}
	l := &limiter{limit: cfg.InitialLimit, min: cfg.MinLimit, max: cfg.MaxLimit, target: cfg.TargetLatency, window: cfg.Window, cfg: cfg}
	return l.Handle
}
func (l *limiter) Handle(c fh.Ctx) error {
	l.mu.Lock()
	if l.inFlight >= l.limit {
		lim := l.limit
		l.mu.Unlock()
		return l.cfg.Error(c, lim)
	}
	l.inFlight++
	l.mu.Unlock()
	start := time.Now()
	err := c.Next()
	dur := time.Since(start)
	l.observe(dur, err)
	return err
}
func (l *limiter) observe(d time.Duration, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inFlight--
	l.samples++
	l.total += d
	if l.samples < l.window {
		return
	}
	avg := l.total / time.Duration(l.samples)
	if err != nil || avg > l.target {
		if l.limit > l.min {
			l.limit--
		}
	} else if l.limit < l.max {
		l.limit++
	}
	l.samples = 0
	l.total = 0
}
