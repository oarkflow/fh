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

type Config struct {
	Max    int
	Window time.Duration

	KeyFunc KeyFunc
	Skip    SkipFunc

	// Store backs the rate-limit counters. Defaults to an in-process
	// kv.MemoryStore. Pass a kv.NewFileStore(dir, ...) to persist counters
	// across restarts on a single node, or any other kv.Store implementation
	// for a distributed backend.
	Store kv.Store

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
		result, err := Allow(cfg.Store, key, cfg.Max, cfg.Window, now)
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
		cfg.Store = kv.NewMemoryStore(kv.WithShardCount(256))
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
// Fixed-window counting
// -----------------------------------------------------------------------------

// windowState is the JSON-encoded value Allow persists per key in the
// underlying kv.Store.
type windowState struct {
	Count       int       `json:"count"`
	WindowStart time.Time `json:"window_start"`
	ResetAt     time.Time `json:"reset_at"`
}

// Allow applies a fixed-window rate-limit check for key against store,
// admitting up to limit requests per window. One file-per-key or
// shard-per-key counter is read, incremented and written back atomically via
// kv.Store.Mutate, so the same algorithm works unchanged whether store is a
// kv.MemoryStore, a kv.FileStore, or any other kv.Store implementation.
func Allow(store kv.Store, key string, limit int, window time.Duration, now time.Time) (Result, error) {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Minute
	}

	var result Result
	err := store.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		var st windowState
		if exists {
			if jerr := json.Unmarshal(current, &st); jerr != nil {
				exists = false
			}
		}
		if !exists || !now.Before(st.ResetAt) {
			st = windowState{WindowStart: now, ResetAt: now.Add(window)}
		}

		st.Count++
		used := st.Count
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
		ttl := resetAt.Sub(now)
		if ttl <= 0 {
			ttl = window
		}
		return data, ttl, true, nil
	})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}
