package replay

import (
	"errors"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

var (
	ErrStoreFull   = errors.New("replay: store capacity exhausted")
	ErrKeyTooLarge = errors.New("replay: key exceeds maximum size")
)

const defaultMaxKeyBytes = 256

type Config struct {
	Header      string
	TTL         time.Duration
	MaxEntries  int
	MaxKeyBytes int
	Store       kv.Store
	Key         func(fh.Ctx) string
	Next        func(fh.Ctx) bool
}

func New(config Config) fh.HandlerFunc {
	if config.Header == "" {
		config.Header = "X-Nonce"
	}
	if config.TTL <= 0 {
		config.TTL = 5 * time.Minute
	}
	if config.MaxEntries <= 0 {
		config.MaxEntries = 100000
	}
	if config.MaxKeyBytes <= 0 {
		config.MaxKeyBytes = defaultMaxKeyBytes
	}
	if config.Store == nil {
		config.Store = kv.NewMemoryStore(
			kv.WithShardCount(256),
			kv.WithMaxEntries(config.MaxEntries),
			kv.WithRejectWhenFull(),
		)
	}
	var replayMu sync.Mutex
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
			return fh.NewHTTPError(fh.StatusBadRequest, "REPLAY_KEY_MISSING", "replay nonce is missing")
		}
		if len(key) > config.MaxKeyBytes {
			return fh.NewHTTPError(fh.StatusBadRequest, "REPLAY_KEY_TOO_LARGE", "replay key is too long")
		}
		seen, err := seen(config.Store, &replayMu, key, config.TTL, config.MaxEntries, config.MaxKeyBytes)
		if err != nil {
			return err
		}
		if seen {
			return fh.NewHTTPError(fh.StatusConflict, "REPLAY_DETECTED", "request replay detected")
		}
		return c.Next()
	}
}

func Seen(store kv.Store, mu *sync.Mutex, key string, ttl time.Duration, maxEntries int) (bool, error) {
	return seen(store, mu, key, ttl, maxEntries, defaultMaxKeyBytes)
}

func seen(store kv.Store, mu *sync.Mutex, key string, ttl time.Duration, maxEntries, maxKeyBytes int) (bool, error) {
	if len(key) > maxKeyBytes {
		return false, ErrKeyTooLarge
	}
	if maxEntries <= 0 {
		maxEntries = 100000
	}
	if bounded, ok := store.(interface{ CapacityLimit() int }); ok && bounded.CapacityLimit() > 0 && bounded.CapacityLimit() <= maxEntries {
		var seen bool
		err := store.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
			seen = exists
			if exists {
				return nil, 0, false, nil
			}
			return []byte{1}, ttl, true, nil
		})
		if errors.Is(err, kv.ErrCapacityExceeded) {
			return false, ErrStoreFull
		}
		return seen, err
	}
	if mu != nil {
		mu.Lock()
		defer mu.Unlock()
	}
	if _, exists, err := store.Get(key); err != nil {
		return false, err
	} else if exists {
		return true, nil
	}
	n, err := store.Len()
	if err != nil {
		return false, err
	}
	if n >= maxEntries {
		return false, ErrStoreFull
	}
	var seen bool
	err = store.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		seen = exists
		if exists {
			return nil, 0, false, nil
		}
		return []byte{1}, ttl, true, nil
	})
	return seen, err
}
