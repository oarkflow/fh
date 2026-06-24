package replay

import (
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

type Store interface {
	Seen(key string, ttl time.Duration) (bool, error)
}

type MemoryStore struct {
	mu sync.Mutex
	m  map[string]time.Time
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{m: map[string]time.Time{}} }
func (s *MemoryStore) Seen(key string, ttl time.Duration) (bool, error) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, exp := range s.m {
		if exp.Before(now) {
			delete(s.m, k)
		}
	}
	if exp, ok := s.m[key]; ok && exp.After(now) {
		return true, nil
	}
	s.m[key] = now.Add(ttl)
	return false, nil
}

type Config struct {
	Header string
	TTL    time.Duration
	Store  Store
	Key    func(*fh.Ctx) string
	Next   func(*fh.Ctx) bool
}

func New(config Config) fh.HandlerFunc {
	if config.Header == "" {
		config.Header = "X-Nonce"
	}
	if config.TTL <= 0 {
		config.TTL = 5 * time.Minute
	}
	if config.Store == nil {
		config.Store = NewMemoryStore()
	}
	return func(c *fh.Ctx) error {
		if config.Next != nil && config.Next(c) {
			return c.Next()
		}
		key := ""
		if config.Key != nil {
			key = config.Key(c)
		} else {
			key = c.Get(config.Header)
		}
		if key == "" {
			return fh.NewHTTPError(fh.StatusUnauthorized, "REPLAY_KEY_MISSING", "replay nonce is missing")
		}
		seen, err := config.Store.Seen(key, config.TTL)
		if err != nil {
			return err
		}
		if seen {
			return fh.NewHTTPError(fh.StatusConflict, "REPLAY_DETECTED", "request replay detected")
		}
		return c.Next()
	}
}
