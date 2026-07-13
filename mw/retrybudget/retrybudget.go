package retrybudget

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type KeyFunc func(fh.Ctx) string
type Config struct {
	MaxRetries int
	Window     time.Duration
	Header     string
	Key        KeyFunc
	Error      func(fh.Ctx) error

	// MaxKeys bounds the number of distinct retry-budget keys tracked at
	// once. Default: 65536. The key (by default the client IP, or whatever
	// Key returns) is attacker-influenceable — without a bound, a remote
	// client sending the retry-attempt header from many distinct keys
	// causes unbounded map growth with no eviction.
	MaxKeys int
}
type bucket struct {
	used  int
	reset time.Time
}
type limiter struct {
	mu          sync.Mutex
	buckets     map[string]bucket
	cfg         Config
	nextCleanup atomic.Int64
}

const defaultMaxKeys = 65536

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
	if cfg.MaxKeys <= 0 {
		cfg.MaxKeys = defaultMaxKeys
	}
	if cfg.Error == nil {
		cfg.Error = func(c fh.Ctx) error {
			c.Set("Retry-After", strconv.Itoa(int(cfg.Window.Seconds())))
			return c.Status(fh.StatusTooManyRequests).JSON(fh.Map{"error": "retry_budget_exceeded"})
		}
	}
	l := &limiter{buckets: map[string]bucket{}, cfg: cfg}
	l.nextCleanup.Store(time.Now().Add(cfg.Window).UnixNano())
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
	l.maybeCleanup(now)
	b, ok := l.buckets[key]
	if b.reset.IsZero() || now.After(b.reset) {
		b = bucket{reset: now.Add(l.cfg.Window)}
	}
	if b.used >= l.cfg.MaxRetries {
		l.buckets[key] = b
		l.mu.Unlock()
		return l.cfg.Error(c)
	}
	if !ok && len(l.buckets) >= l.cfg.MaxKeys {
		l.evictOneLocked()
	}
	b.used++
	l.buckets[key] = b
	l.mu.Unlock()
	return c.Next()
}

// maybeCleanup sweeps expired buckets at most once per Window, amortizing
// the cost across requests instead of scanning on every call.
func (l *limiter) maybeCleanup(now time.Time) {
	deadline := l.nextCleanup.Load()
	if now.UnixNano() < deadline {
		return
	}
	l.nextCleanup.Store(now.Add(l.cfg.Window).UnixNano())
	for k, b := range l.buckets {
		if now.After(b.reset) {
			delete(l.buckets, k)
		}
	}
}

// evictOneLocked drops a single arbitrary entry when at MaxKeys capacity
// and cleanup already ran; caller holds l.mu.
func (l *limiter) evictOneLocked() {
	for k := range l.buckets {
		delete(l.buckets, k)
		return
	}
}
