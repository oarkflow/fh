package ratelimiter

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

const (
	HeaderLimit      = "X-RateLimit-Limit"
	HeaderRemaining  = "X-RateLimit-Remaining"
	HeaderReset      = "X-RateLimit-Reset"
	HeaderRetryAfter = "Retry-After"
)

var ErrLimitReached = errors.New("ratelimiter: limit reached")

type KeyFunc func(ctx fh.Ctx) string

type LimitReachedHandler func(ctx fh.Ctx, result Result) error

type SkipFunc func(ctx fh.Ctx) bool

type Result struct {
	Allowed    bool
	Limit      int
	Remaining  int
	Used       int
	ResetAt    time.Time
	RetryAfter time.Duration
}

type Store interface {
	Allow(key string, limit int, window time.Duration, now time.Time) (Result, error)
}

type Config struct {
	Max    int
	Window time.Duration

	KeyFunc KeyFunc
	Skip    SkipFunc

	Store Store

	SendHeaders bool

	LimitReached LimitReachedHandler
}

func New(config Config) fh.HandlerFunc {
	cfg := normalize(config)

	limitStr := strconv.Itoa(cfg.Max)

	return func(ctx fh.Ctx) error {
		if cfg.Skip != nil && cfg.Skip(ctx) {
			return ctx.Next()
		}

		key := cfg.KeyFunc(ctx)
		if key == "" {
			key = "unknown"
		}

		now := time.Now()
		result, err := cfg.Store.Allow(key, cfg.Max, cfg.Window, now)
		if err != nil {
			return err
		}

		if cfg.SendHeaders {
			ctx.Set(HeaderLimit, limitStr)

			var buf [32]byte

			rem := result.Remaining
			if rem < 0 {
				rem = 0
			}
			ctx.Set(HeaderRemaining, string(strconv.AppendInt(buf[:0], int64(rem), 10)))

			resetUnix := result.ResetAt.Unix()
			ctx.Set(HeaderReset, string(strconv.AppendInt(buf[:0], resetUnix, 10)))
		}

		if !result.Allowed {
			return cfg.LimitReached(ctx, result)
		}

		return ctx.Next()
	}
}

func normalize(cfg Config) Config {
	if cfg.Max <= 0 {
		cfg.Max = 100
	}
	if cfg.Window <= 0 {
		cfg.Window = time.Minute
	}
	if cfg.KeyFunc == nil {
		cfg.KeyFunc = func(ctx fh.Ctx) string {
			return ctx.IP()
		}
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore(256)
	}
	if cfg.LimitReached == nil {
		cfg.LimitReached = DefaultLimitReachedHandler
	}

	cfg.SendHeaders = true

	return cfg
}

func DefaultLimitReachedHandler(ctx fh.Ctx, result Result) error {
	retry := int(result.RetryAfter.Seconds())
	if retry < 1 {
		retry = 1
	}

	ctx.Set(HeaderRetryAfter, strconv.Itoa(retry))
	ctx.Set("Content-Type", "text/plain; charset=utf-8")

	return ctx.Status(429).SendString("Too Many Requests")
}

// -----------------------------------------------------------------------------
// In-memory sliding-window store
// -----------------------------------------------------------------------------

// windowState is the JSON-encoded value MemoryStore and FileStore persist per
// key in the underlying kv.Store. Count/PrevCount/WindowStart drive the
// sliding-window weighting; ResetAt is when the current window ends.
type windowState struct {
	Count       int       `json:"count"`
	PrevCount   int       `json:"prev_count"`
	WindowStart time.Time `json:"window_start"`
	ResetAt     time.Time `json:"reset_at"`
}

// MemoryStore is a sharded, in-process Store backed by kv.MemoryStore. It
// keeps the same sliding-window algorithm the original hand-rolled
// implementation used, but the sharding, locking and expiry mechanics are now
// provided once by pkg/storage/kv instead of being reimplemented here.
type MemoryStore struct {
	kv *kv.MemoryStore
}

func NewMemoryStore(shardCount int) *MemoryStore {
	if shardCount <= 0 {
		shardCount = 256
	}

	shardCount = nextPowerOfTwo(shardCount)

	return &MemoryStore{kv: kv.NewMemoryStore(kv.WithShardCount(shardCount))}
}

func (s *MemoryStore) Allow(key string, limit int, window time.Duration, now time.Time) (Result, error) {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Minute
	}

	var result Result
	err := s.kv.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		var st windowState
		if exists {
			if jerr := json.Unmarshal(current, &st); jerr != nil {
				exists = false
			}
		}
		if !exists {
			st = windowState{WindowStart: now, ResetAt: now.Add(window)}
		}

		if !now.Before(st.ResetAt) {
			elapsed := now.Sub(st.WindowStart)
			if elapsed >= 2*window {
				st.PrevCount = 0
			} else {
				st.PrevCount = st.Count
			}
			st.Count = 0
			st.WindowStart = now
			st.ResetAt = now.Add(window)
		}

		// Sliding window: weight previous window's count by elapsed fraction.
		elapsed := now.Sub(st.WindowStart).Seconds()
		windowSec := window.Seconds()
		var weightedCount float64
		if elapsed < windowSec && st.PrevCount > 0 {
			previousWeight := (windowSec - elapsed) / windowSec
			weightedCount = float64(st.PrevCount)*previousWeight + float64(st.Count)
		} else {
			weightedCount = float64(st.Count)
		}

		st.Count++
		used := int(weightedCount) + 1
		resetAt := st.ResetAt

		remaining := limit - used
		if remaining < 0 {
			remaining = 0
		}

		allowed := used <= limit
		retryAfter := time.Duration(0)
		if !allowed {
			retryAfter = resetAt.Sub(now)
			if retryAfter < time.Second {
				retryAfter = time.Second
			}
		}

		result = Result{
			Allowed:    allowed,
			Limit:      limit,
			Remaining:  remaining,
			Used:       used,
			ResetAt:    resetAt,
			RetryAfter: retryAfter,
		}

		data, merr := json.Marshal(st)
		if merr != nil {
			return nil, 0, false, merr
		}
		// Keep the window state alive for two full windows of inactivity so
		// the prevCount weighting above still has something to read right
		// after a window boundary; kv's own expiry then reclaims windows
		// that truly go stale (no requests for that long).
		return data, 2 * window, true, nil
	})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}

	p := 1
	for p < n {
		p <<= 1
	}

	return p
}
