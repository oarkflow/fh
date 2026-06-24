package ratelimiter

import (
	"errors"
	"hash/fnv"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
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
// In-memory fixed-window store
// -----------------------------------------------------------------------------

type MemoryStore struct {
	shards []memoryShard
	mask   uint64

	cleanupEvery time.Duration
	nextCleanup  atomic.Int64
}

type memoryShard struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	count   int
	resetAt time.Time
}

func NewMemoryStore(shardCount int) *MemoryStore {
	if shardCount <= 0 {
		shardCount = 256
	}

	shardCount = nextPowerOfTwo(shardCount)

	s := &MemoryStore{
		shards:       make([]memoryShard, shardCount),
		mask:         uint64(shardCount - 1),
		cleanupEvery: time.Minute,
	}

	for i := range s.shards {
		s.shards[i].buckets = make(map[string]*bucket, 64)
	}

	s.nextCleanup.Store(time.Now().Add(s.cleanupEvery).UnixNano())

	return s
}

func (s *MemoryStore) Allow(key string, limit int, window time.Duration, now time.Time) (Result, error) {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Minute
	}

	s.maybeCleanup(now)

	sh := &s.shards[s.hash(key)&s.mask]

	sh.mu.Lock()

	b := sh.buckets[key]
	if b == nil {
		b = &bucket{
			count:   0,
			resetAt: now.Add(window),
		}
		sh.buckets[key] = b
	}

	if !now.Before(b.resetAt) {
		b.count = 0
		b.resetAt = now.Add(window)
	}

	b.count++
	used := b.count
	resetAt := b.resetAt

	sh.mu.Unlock()

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

	return Result{
		Allowed:    allowed,
		Limit:      limit,
		Remaining:  remaining,
		Used:       used,
		ResetAt:    resetAt,
		RetryAfter: retryAfter,
	}, nil
}

func (s *MemoryStore) maybeCleanup(now time.Time) {
	deadline := s.nextCleanup.Load()
	if now.UnixNano() < deadline {
		return
	}

	next := now.Add(s.cleanupEvery).UnixNano()
	if !s.nextCleanup.CompareAndSwap(deadline, next) {
		return
	}

	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, b := range sh.buckets {
			if !now.Before(b.resetAt) {
				delete(sh.buckets, k)
			}
		}
		sh.mu.Unlock()
	}
}

func (s *MemoryStore) hash(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
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
