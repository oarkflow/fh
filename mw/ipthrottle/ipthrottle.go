package ipthrottle

import (
	"net"
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
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				limitersMu.Lock()
				now := time.Now()
				for k, l := range limiters {
					l.mu.Lock()
					if now.Sub(l.lastClean) > 5*time.Minute {
						delete(limiters, k)
					}
					l.mu.Unlock()
				}
				limitersMu.Unlock()
			case <-stop:
				return
			}
		}
	}()

	return func(c fh.Ctx) error {
		key := cfg.KeyFunc(c)

		gConns := globalConns.Add(1)
		defer globalConns.Add(-1)

		if cfg.GlobalMax > 0 && int(gConns) > cfg.GlobalMax {
			return cfg.Reject(c, key, int(gConns))
		}

		ip := extractIP(key)

		limitersMu.Lock()
		if cfg.MaxIPs > 0 && len(limiters) >= cfg.MaxIPs {
			oldestKey := ""
			oldestTime := time.Now()
			for k, l := range limiters {
				l.mu.Lock()
				if l.lastClean.Before(oldestTime) {
					oldestTime = l.lastClean
					oldestKey = k
				}
				l.mu.Unlock()
			}
			if oldestKey != "" {
				delete(limiters, oldestKey)
			}
		}
		l, ok := limiters[ip]
		if !ok {
			l = &ipLimiter{lastClean: time.Now()}
			limiters[ip] = l
		}
		l.mu.Lock()
		cleanupExpired(l, cfg.Window)
		count := l.count
		l.count++
		l.mu.Unlock()
		limitersMu.Unlock()

		if cfg.MaxPerIP > 0 && count >= cfg.MaxPerIP {
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
			c.Set("Retry-After", "1")
			return c.Status(fh.StatusServiceUnavailable).SendString("Service Unavailable")
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

func splitComma(s string) []string {
	var parts []string
	for len(s) > 0 {
		idx := -1
		for i := 0; i < len(s); i++ {
			if s[i] == ',' {
				idx = i
				break
			}
		}
		if idx < 0 {
			parts = append(parts, s)
			break
		}
		parts = append(parts, s[:idx])
		s = s[idx+1:]
	}
	return parts
}

var globalConns atomic.Int64

type ipLimiter struct {
	mu        sync.Mutex
	count     int
	lastClean time.Time
}

var (
	limitersMu sync.Mutex
	limiters   = make(map[string]*ipLimiter)
)

func cleanupExpired(l *ipLimiter, window time.Duration) {
	now := time.Now()
	if now.Sub(l.lastClean) > window {
		l.count = 0
		l.lastClean = now
	}
}
