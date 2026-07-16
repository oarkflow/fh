package ipthrottle

import (
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type RejectHandler func(fh.Ctx, string, int) error

type Config struct {
	MaxPerIP  int
	GlobalMax int
	MaxIPs    int
	Window    time.Duration
	KeyFunc   func(fh.Ctx) string
	Reject    RejectHandler
}

func New(cfg Config) fh.HandlerFunc {
	cfg = normalize(cfg)
	state := &throttleState{limiters: make(map[string]*ipLimiter)}

	return func(c fh.Ctx) error {
		key := cfg.KeyFunc(c)

		gConns := state.active.Add(1)
		defer state.active.Add(-1)

		if cfg.GlobalMax > 0 && int(gConns) > cfg.GlobalMax {
			return cfg.Reject(c, key, int(gConns))
		}

		ip := extractIP(key)
		now := time.Now()
		state.mu.Lock()
		l, ok := state.limiters[ip]
		if !ok {
			state.sweepExpired(now, cfg.Window)
			if cfg.MaxIPs > 0 && len(state.limiters) >= cfg.MaxIPs {
				state.mu.Unlock()
				return cfg.Reject(c, key, 0)
			}
			l = &ipLimiter{windowStart: now}
			state.limiters[ip] = l
		}
		if now.Sub(l.windowStart) >= cfg.Window {
			l.count = 0
			l.windowStart = now
		}
		l.count++
		count := l.count
		state.mu.Unlock()

		if cfg.MaxPerIP > 0 && count > cfg.MaxPerIP {
			return cfg.Reject(c, key, count)
		}

		return c.Next()
	}
}

func normalize(cfg Config) Config {
	if cfg.MaxPerIP <= 0 {
		cfg.MaxPerIP = 100
	}
	if cfg.GlobalMax <= 0 {
		cfg.GlobalMax = 10000
	}
	if cfg.MaxIPs <= 0 {
		cfg.MaxIPs = 100000
	}
	if cfg.Window <= 0 {
		cfg.Window = time.Minute
	}
	if cfg.KeyFunc == nil {
		cfg.KeyFunc = func(c fh.Ctx) string { return c.IP() }
	}
	if cfg.Reject == nil {
		cfg.Reject = func(c fh.Ctx, _ string, _ int) error {
			seconds := max(1, int((cfg.Window+time.Second-1)/time.Second))
			c.Set("Retry-After", strconv.Itoa(seconds))
			return c.Status(fh.StatusTooManyRequests).SendString("Too Many Requests")
		}
	}
	return cfg
}

func extractIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

type ipLimiter struct {
	count       int
	windowStart time.Time
}

type throttleState struct {
	mu        sync.Mutex
	active    atomic.Int64
	limiters  map[string]*ipLimiter
	lastSweep time.Time
}

func (s *throttleState) sweepExpired(now time.Time, window time.Duration) {
	interval := window
	if interval > time.Minute {
		interval = time.Minute
	}
	if !s.lastSweep.IsZero() && now.Sub(s.lastSweep) < interval {
		return
	}
	for key, limiter := range s.limiters {
		if now.Sub(limiter.windowStart) >= window {
			delete(s.limiters, key)
		}
	}
	s.lastSweep = now
}
