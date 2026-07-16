package timestamp

import (
	"strconv"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

type Config struct {
	Header     string
	MaxSkew    time.Duration
	Store      ReplayStore
	KeyFunc    func(fh.Ctx) string
	Reject     func(fh.Ctx, string) error
	Skip       func(fh.Ctx) bool
	Required   bool
}

type ReplayStore interface {
	Seen(key string, ttl time.Duration) (bool, error)
}

func New(cfg Config) fh.HandlerFunc {
	cfg = normalize(cfg)
	return func(c fh.Ctx) error {
		if cfg.Skip != nil && cfg.Skip(c) {
			return c.Next()
		}

		ts := c.Get(cfg.Header)
		if ts == "" && cfg.Required {
			return cfg.Reject(c, "missing timestamp header")
		}

		if ts != "" {
			timestamp, err := strconv.ParseInt(ts, 10, 64)
			if err != nil {
				return cfg.Reject(c, "invalid timestamp format")
			}

			reqTime := time.Unix(timestamp, 0)
			skew := time.Since(reqTime)
			if skew < 0 {
				skew = -skew
			}

			if skew > cfg.MaxSkew {
				return cfg.Reject(c, "request timestamp outside acceptable window")
			}

			key := cfg.KeyFunc(c) + ":" + ts
			seen, err := cfg.Store.Seen(key, cfg.MaxSkew)
			if err != nil {
				return err
			}
			if seen {
				return cfg.Reject(c, "duplicate request detected")
			}
		}

		return c.Next()
	}
}

func normalize(cfg Config) Config {
	if cfg.Header == "" {
		cfg.Header = "X-Request-Timestamp"
	}
	if cfg.MaxSkew <= 0 {
		cfg.MaxSkew = 5 * time.Minute
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	if cfg.KeyFunc == nil {
		cfg.KeyFunc = func(c fh.Ctx) string {
			return c.IP() + ":" + c.Method() + ":" + c.Path()
		}
	}
	if cfg.Reject == nil {
		cfg.Reject = func(c fh.Ctx, msg string) error {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{
				"error":  "timestamp_validation_failed",
				"detail": msg,
			})
		}
	}
	return cfg
}

type MemoryStore struct {
	mu sync.Mutex
	m  map[string]time.Time
}

func NewMemoryStore() *MemoryStore {
	s := &MemoryStore{m: make(map[string]time.Time)}
	go s.cleanup()
	return s
}

func (s *MemoryStore) Seen(key string, ttl time.Duration) (bool, error) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if exp, ok := s.m[key]; ok && exp.After(now) {
		return true, nil
	}

	s.m[key] = now.Add(ttl)
	return false, nil
}

func (s *MemoryStore) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		s.mu.Lock()
		for k, exp := range s.m {
			if exp.Before(now) {
				delete(s.m, k)
			}
		}
		s.mu.Unlock()
	}
}
