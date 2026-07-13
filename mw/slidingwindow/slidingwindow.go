// Package slidingwindow provides a sliding window rate limiter middleware.
// Unlike fixed-window limiters, sliding window accurately tracks request
// rates across window boundaries, preventing burst spikes at window edges.
//
// The algorithm uses a logarithmic counter approach:
// - Each request increments a counter with a timestamp
// - Old requests are expired based on the window size
// - The rate is calculated as requests-per-second over the window
//
// Features:
// - Per-IP, per-key, or global rate limiting
// - Burst allowance for short traffic spikes
// - Retry-After header support
// - Custom key extraction (by IP, header, route, etc.)
// - Clean headers: X-RateLimit-*, Retry-After
package slidingwindow

import (
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

// Limiter is a sliding window rate limiter.
type Limiter struct {
	mu         sync.Mutex
	windows    map[string]*window
	windowSize time.Duration
	rate       int
	burst      int
	maxKeys    int
}

type window struct {
	timestamps []time.Time
	head       int
	count      int
}

// Config holds configuration for the sliding window rate limiter.
type Config struct {
	// Rate is the maximum requests per window. Default: 100.
	Rate int

	// Burst is the maximum burst size above the rate. Default: same as Rate.
	Burst int

	// Window is the sliding window duration. Default: 1 second.
	Window time.Duration

	// KeyFunc extracts the rate limit key from the request.
	// Default: client IP address.
	KeyFunc func(ctx fh.Ctx) string

	// MaxKeys is the maximum number of tracked keys. Default: 65536.
	MaxKeys int

	// CleanupInterval is how often to clean expired keys. Default: 1 minute.
	CleanupInterval time.Duration

	// Message is the error message for rate-limited requests.
	Message string

	// StatusCode is the HTTP status for rate-limited requests. Default: 429.
	StatusCode int

	// Next is an optional skip function.
	Next func(ctx fh.Ctx) bool

	// OnLimitReached is called when a request is rate-limited.
	OnLimitReached func(ctx fh.Ctx, key string, remaining int)
}

// DefaultConfig returns the default configuration.
var DefaultConfig = Config{
	Rate:            100,
	Burst:           100,
	Window:          time.Second,
	MaxKeys:         65536,
	CleanupInterval: time.Minute,
	Message:         "Rate limit exceeded",
	StatusCode:      429,
}

// NewLimiter creates a new sliding window rate limiter.
func NewLimiter(config ...Config) *Limiter {
	cfg := DefaultConfig
	if len(config) > 0 {
		c := config[0]
		if c.Rate > 0 {
			cfg.Rate = c.Rate
		}
		if c.Burst > 0 {
			cfg.Burst = c.Burst
		}
		if c.Window > 0 {
			cfg.Window = c.Window
		}
		if c.MaxKeys > 0 {
			cfg.MaxKeys = c.MaxKeys
		}
		if c.CleanupInterval > 0 {
			cfg.CleanupInterval = c.CleanupInterval
		}
	}

	l := &Limiter{
		windows:    make(map[string]*window, cfg.MaxKeys),
		windowSize: cfg.Window,
		rate:       cfg.Rate,
		burst:      cfg.Burst,
		maxKeys:    cfg.MaxKeys,
	}

	go l.cleanup(cfg.CleanupInterval, cfg.MaxKeys)

	return l
}

// Allow checks if a request with the given key is allowed.
// Returns (allowed, remaining, retryAfter).
func (l *Limiter) Allow(key string) (bool, int, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	w, exists := l.windows[key]
	if !exists {
		maxKeys := l.maxKeys
		if maxKeys <= 0 {
			maxKeys = 65536
		}
		if len(l.windows) >= maxKeys {
			l.evictOldest()
		}
		w = &window{
			timestamps: make([]time.Time, 0, l.rate*2),
		}
		l.windows[key] = w
	}

	// Expire old timestamps outside the window.
	cutoff := now.Add(-l.windowSize)
	for w.head < w.count && w.timestamps[w.head].Before(cutoff) {
		w.head++
	}

	currentCount := w.count - w.head
	remaining := l.rate - currentCount

	if remaining <= 0 {
		// Calculate retry-after from oldest request in window.
		var retryAfter time.Duration
		if w.head < w.count {
			oldest := w.timestamps[w.head]
			retryAfter = l.windowSize - now.Sub(oldest)
			if retryAfter < 0 {
				retryAfter = 0
			}
		}
		return false, 0, retryAfter
	}

	// Allow burst above rate.
	if currentCount >= l.rate+l.burst {
		return false, 0, 0
	}

	// Add new timestamp.
	if w.count == len(w.timestamps) {
		// Compact: shift timestamps to front, reclaiming space freed by
		// expiry so we don't append (and grow the slice) unnecessarily.
		n := w.count - w.head
		copy(w.timestamps, w.timestamps[w.head:w.count])
		w.head = 0
		w.count = n
	}
	// timestamps is allocated with length 0 (only capacity pre-reserved),
	// so the first write — and any write once count has caught back up to
	// the slice's current length even after compaction — must grow the
	// slice via append rather than indexing past its length.
	if w.count < len(w.timestamps) {
		w.timestamps[w.count] = now
	} else {
		w.timestamps = append(w.timestamps, now)
	}
	w.count++

	return true, remaining - 1, 0
}

func (l *Limiter) evictOldest() {
	var oldestKey string
	var oldestTime time.Time
	for k, w := range l.windows {
		if w.head < w.count {
			t := w.timestamps[w.head]
			if oldestKey == "" || t.Before(oldestTime) {
				oldestKey = k
				oldestTime = t
			}
		}
	}
	if oldestKey != "" {
		delete(l.windows, oldestKey)
	}
}

func (l *Limiter) cleanup(interval time.Duration, maxKeys int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		l.mu.Lock()
		now := time.Now()
		cutoff := now.Add(-l.windowSize * 2)
		for k, w := range l.windows {
			if w.head >= w.count {
				delete(l.windows, k)
				continue
			}
			if w.timestamps[w.count-1].Before(cutoff) {
				delete(l.windows, k)
			}
		}
		l.mu.Unlock()
	}
}

// New creates a sliding window rate limiter middleware.
func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		c := config[0]
		if c.Rate > 0 {
			cfg.Rate = c.Rate
		}
		if c.Burst > 0 {
			cfg.Burst = c.Burst
		}
		if c.Window > 0 {
			cfg.Window = c.Window
		}
		if c.KeyFunc != nil {
			cfg.KeyFunc = c.KeyFunc
		}
		if c.MaxKeys > 0 {
			cfg.MaxKeys = c.MaxKeys
		}
		if c.CleanupInterval > 0 {
			cfg.CleanupInterval = c.CleanupInterval
		}
		if c.Message != "" {
			cfg.Message = c.Message
		}
		if c.StatusCode > 0 {
			cfg.StatusCode = c.StatusCode
		}
		if c.Next != nil {
			cfg.Next = c.Next
		}
		if c.OnLimitReached != nil {
			cfg.OnLimitReached = c.OnLimitReached
		}
	}

	limiter := NewLimiter(cfg)

	return func(ctx fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(ctx) {
			return ctx.Next()
		}

		key := ""
		if cfg.KeyFunc != nil {
			key = cfg.KeyFunc(ctx)
		} else {
			key = extractIP(ctx)
		}

		if key == "" {
			return ctx.Next()
		}

		allowed, remaining, retryAfter := limiter.Allow(key)

		// Always set rate limit headers.
		ctx.Set("X-RateLimit-Limit", strconv.Itoa(cfg.Rate))
		ctx.Set("X-RateLimit-Remaining", strconv.Itoa(remaining))

		if !allowed {
			ctx.Set("Retry-After", strconv.Itoa(int(math.Ceil(retryAfter.Seconds()))))
			if cfg.OnLimitReached != nil {
				cfg.OnLimitReached(ctx, key, remaining)
			}
			return ctx.Status(cfg.StatusCode).JSON(map[string]string{
				"error":   cfg.Message,
				"message": "You have exceeded the rate limit. Please try again later.",
			})
		}

		return ctx.Next()
	}
}

// ── Key extractors ─────────────────────────────────────────────────────────

// ByIP extracts the client IP as the rate limit key.
func ByIP(ctx fh.Ctx) string {
	return extractIP(ctx)
}

// ByHeader extracts a header value as the rate limit key.
func ByHeader(name string) func(fh.Ctx) string {
	return func(ctx fh.Ctx) string {
		v := ctx.Get(name)
		if v == "" {
			return extractIP(ctx)
		}
		return name + ":" + v
	}
}

// ByRoute extracts the route path as the rate limit key (global per-route limit).
func ByRoute(ctx fh.Ctx) string {
	return ctx.Method() + ":" + ctx.Path()
}

// ByComposite combines multiple key functions.
func ByComposite(fns ...func(fh.Ctx) string) func(fh.Ctx) string {
	return func(ctx fh.Ctx) string {
		var b []byte
		for _, fn := range fns {
			b = append(b, fn(ctx)...)
			b = append(b, ':')
		}
		if len(b) > 0 {
			b = b[:len(b)-1]
		}
		return string(b)
	}
}

func extractIP(ctx fh.Ctx) string {
	// Forwarding headers are intentionally not read here. Place mw/realip,
	// configured with trusted proxy CIDRs, before the limiter to override IP().
	return ctx.IP()
}
