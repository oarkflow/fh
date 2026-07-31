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
	"encoding/json"
	"math"
	"strconv"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

// Store holds per-key sliding window state for the rate limiter and decides
// whether a request for key is allowed right now. Rate, burst and window
// size are configured once when the Store is constructed (they are
// properties of a single limiter instance, not per-call parameters), so
// Allow only needs the key.
//
// Returns (allowed, remaining, retryAfter) with the same semantics as the
// former Limiter.Allow: remaining is the number of requests still permitted
// in the current window when allowed is true (0 otherwise), and retryAfter
// is how long the caller should wait before retrying when allowed is false.
type Store interface {
	Allow(key string) (allowed bool, remaining int, retryAfter time.Duration)
}

// windowState is the JSON-encoded value held per key inside the underlying
// kv.Store: the sorted, already-trimmed timestamps of requests still inside
// the sliding window as of the last Allow call for that key.
type windowState struct {
	Timestamps []time.Time `json:"timestamps"`
}

// MemoryStore is the default Store: a thin adapter over kv.MemoryStore, the
// shared in-process map+shard+mutex primitive used by every middleware
// package in this module. Per-key sliding-window state (the compacting log
// of in-window request timestamps) is JSON-encoded and mutated race-free via
// kv.Store.Mutate, reproducing the exact admission decision (rate, burst,
// retry-after) this package has always computed. Key cardinality (MaxKeys)
// and idle-key cleanup (CleanupInterval) are delegated to kv.MemoryStore's
// own WithMaxEntries/WithGCInterval options rather than reimplemented here.
//
// Limiter is kept as an alias so existing callers of NewLimiter keep
// compiling unchanged. Because state now lives inside the wrapped
// kv.MemoryStore rather than as directly-addressable unexported fields, code
// that used to construct a Limiter/MemoryStore as a struct literal or reach
// into l.mu/l.windows can no longer do so; see slidingwindow_test.go, which
// was updated to go through the public New/Allow/Len API instead.
type MemoryStore struct {
	store      *kv.MemoryStore
	windowSize time.Duration
	rate       int
	burst      int
	maxKeys    int
}

// Limiter is the historical name for MemoryStore, preserved for backward
// compatibility.
type Limiter = MemoryStore

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

	// Store holds per-key sliding window state. Defaults to a new
	// MemoryStore built from this Config (i.e. NewLimiter(cfg)), which
	// reproduces the exact behavior this package has always had. Set Store
	// to a FileStore (see file_store.go) to persist window state across
	// restarts.
	Store Store
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

	l := &MemoryStore{
		// A single shard (rather than kv.MemoryStore's default 16) is
		// deliberate: WithMaxEntries splits its cap evenly across shards, so
		// a small, user-configured MaxKeys (as in this package's tests, e.g.
		// 10) would round down to a fractional per-shard cap and let the
		// aggregate size overshoot the configured bound across shards. One
		// shard makes the cap exact, matching the single-mutex, exact-bound
		// behavior this package had before this retrofit; the trade-off is
		// giving up kv.MemoryStore's per-shard lock concurrency, which the
		// pre-retrofit implementation never had anyway (it used one mutex
		// for everything).
		store: kv.NewMemoryStore(
			kv.WithShardCount(1),
			kv.WithMaxEntries(cfg.MaxKeys),
			kv.WithGCInterval(cfg.CleanupInterval),
		),
		windowSize: cfg.Window,
		rate:       cfg.Rate,
		burst:      cfg.Burst,
		maxKeys:    cfg.MaxKeys,
	}

	return l
}

// Allow checks if a request with the given key is allowed.
// Returns (allowed, remaining, retryAfter).
func (l *MemoryStore) Allow(key string) (bool, int, time.Duration) {
	allowed, remaining, retryAfter, err := allowViaStore(l.store, key, l.rate, l.burst, l.windowSize)
	if err != nil {
		// kv.Store.Mutate only errors on ErrClosed or a marshal failure,
		// neither of which the Store.Allow signature (no error return) can
		// surface; fail closed rather than silently letting the request
		// through.
		return false, 0, 0
	}
	return allowed, remaining, retryAfter
}

// Len reports the number of distinct keys currently tracked. It is a thin
// pass-through to the underlying kv.MemoryStore, exposed so callers (and
// this package's own tests) can observe the MaxKeys cardinality bound
// without reaching into unexported fields.
func (l *MemoryStore) Len() (int, error) {
	return l.store.Len()
}

// allowViaStore implements Store.Allow on top of any kv.Store, shared by
// MemoryStore and FileStore. It reproduces the exact admission algorithm
// this package has always used (expire timestamps outside the window,
// compute remaining against rate, allow bursts up to rate+burst, otherwise
// reject with a retry-after derived from the oldest in-window timestamp);
// only the representation changes, from an in-memory compacting ring buffer
// (an optimization to avoid reallocating the timestamp slice on every
// request) to a plain trimmed slice that round-trips through JSON on every
// Mutate call, since kv.Store already reallocates on every write and the
// ring buffer's head/count bookkeeping has nothing left to optimize once
// that's true.
func allowViaStore(store kv.Store, key string, rate, burst int, windowSize time.Duration) (allowed bool, remaining int, retryAfter time.Duration, err error) {
	now := time.Now()
	err = store.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		var ws windowState
		if exists {
			if jsonErr := json.Unmarshal(current, &ws); jsonErr != nil {
				ws = windowState{}
			}
		}

		timestamps := ws.Timestamps
		cutoff := now.Add(-windowSize)
		trimmed := 0
		for trimmed < len(timestamps) && timestamps[trimmed].Before(cutoff) {
			trimmed++
		}
		if trimmed > 0 {
			timestamps = append([]time.Time(nil), timestamps[trimmed:]...)
		}

		currentCount := len(timestamps)
		remaining = rate - currentCount

		if remaining <= 0 {
			allowed = false
			remaining = 0
			if len(timestamps) > 0 {
				oldest := timestamps[0]
				retryAfter = windowSize - now.Sub(oldest)
				if retryAfter < 0 {
					retryAfter = 0
				}
			} else {
				retryAfter = 0
			}
			next, marshalErr := json.Marshal(windowState{Timestamps: timestamps})
			if marshalErr != nil {
				return nil, 0, false, marshalErr
			}
			return next, windowSize * 2, true, nil
		}

		if currentCount >= rate+burst {
			allowed = false
			remaining = 0
			retryAfter = 0
			next, marshalErr := json.Marshal(windowState{Timestamps: timestamps})
			if marshalErr != nil {
				return nil, 0, false, marshalErr
			}
			return next, windowSize * 2, true, nil
		}

		timestamps = append(timestamps, now)
		allowed = true
		remaining--
		next, marshalErr := json.Marshal(windowState{Timestamps: timestamps})
		if marshalErr != nil {
			return nil, 0, false, marshalErr
		}
		return next, windowSize * 2, true, nil
	})
	return allowed, remaining, retryAfter, err
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
		if c.Store != nil {
			cfg.Store = c.Store
		}
	}

	var store Store = cfg.Store
	if store == nil {
		store = NewLimiter(cfg)
	}

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

		allowed, remaining, retryAfter := store.Allow(key)

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
