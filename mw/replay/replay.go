package replay

import (
	"errors"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

type Store interface {
	Seen(key string, ttl time.Duration) (bool, error)
}

// MemoryStore is an in-process Store backed by kv.MemoryStore. It does not
// use kv's own WithMaxEntries/eviction (which evicts an arbitrary live entry
// to admit a new one): replay's contract is to fail closed at capacity and
// never evict a still-valid marker, so capacity is enforced here instead. mu
// serializes the whole Seen call, matching the single-mutex granularity the
// original hand-rolled MemoryStore used.
type MemoryStore struct {
	kv         *kv.MemoryStore
	maxEntries int
	mu         sync.Mutex
}

var ErrStoreFull = errors.New("replay: store capacity exhausted")

func NewMemoryStore(maxEntries ...int) *MemoryStore {
	maxSize := 100000
	if len(maxEntries) > 0 && maxEntries[0] > 0 {
		maxSize = maxEntries[0]
	}
	return &MemoryStore{kv: kv.NewMemoryStore(), maxEntries: maxSize}
}

func (s *MemoryStore) Seen(key string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.maxEntries > 0 {
		if _, exists, _ := s.kv.Get(key); !exists {
			// Len() sweeps expired entries as a side effect, reclaiming
			// capacity the same way the original implementation's opportunistic
			// cleanup-on-full pass did.
			n, err := s.kv.Len()
			if err != nil {
				return false, err
			}
			if n >= s.maxEntries {
				return false, ErrStoreFull
			}
		}
	}

	var seen bool
	err := s.kv.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if exists {
			seen = true
			return nil, 0, false, nil
		}
		seen = false
		return []byte{1}, ttl, true, nil
	})
	if err != nil {
		return false, err
	}
	return seen, nil
}

type Config struct {
	Header     string
	TTL        time.Duration
	MaxEntries int
	Store      Store
	Key        func(fh.Ctx) string
	Next       func(fh.Ctx) bool
}

func New(config Config) fh.HandlerFunc {
	if config.Header == "" {
		config.Header = "X-Nonce"
	}
	if config.TTL <= 0 {
		config.TTL = 5 * time.Minute
	}
	if config.Store == nil {
		config.Store = NewMemoryStore(config.MaxEntries)
	}
	return func(c fh.Ctx) error {
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
