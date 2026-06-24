package actor

import (
	"sync"

	"github.com/oarkflow/fh"
)

type Config struct{ Key func(*fh.Ctx) string }

var registry = struct {
	sync.Mutex
	locks map[string]*sync.Mutex
}{locks: map[string]*sync.Mutex{}}

func New(cfg Config) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		if cfg.Key == nil {
			return c.Next()
		}
		key := cfg.Key(c)
		if key == "" {
			return c.Next()
		}
		registry.Lock()
		lock := registry.locks[key]
		if lock == nil {
			lock = &sync.Mutex{}
			registry.locks[key] = lock
		}
		registry.Unlock()
		lock.Lock()
		defer lock.Unlock()
		return c.Next()
	}
}
