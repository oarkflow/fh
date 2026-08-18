package actor

import (
	"sync"

	"github.com/oarkflow/fh"
)

type Config struct{ Key func(fh.Ctx) string }

var registry sync.Map

func New(cfg Config) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		if cfg.Key == nil {
			return c.Next()
		}
		key := cfg.Key(c)
		if key == "" {
			return c.Next()
		}
		val, _ := registry.LoadOrStore(key, &sync.Mutex{})
		lock := val.(*sync.Mutex)
		lock.Lock()
		defer lock.Unlock()
		return c.Next()
	}
}
