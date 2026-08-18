package replay

import (
	"errors"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

var ErrStoreFull = errors.New("replay: store capacity exhausted")

type Config struct {
	Header     string
	TTL        time.Duration
	MaxEntries int
	Store      kv.Store
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
		config.Store = kv.NewMemoryStore(kv.WithShardCount(1))
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
		seen, err := Seen(config.Store, &replayMu, key, config.TTL, config.MaxEntries)
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
	if maxEntries <= 0 {
		maxEntries = 100000
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
	if err := store.Set(key, []byte{1}, ttl); err != nil {
		return false, err
	}
	return false, nil
}
