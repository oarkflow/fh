package actor

import (
	"hash/maphash"
	"sync"
	"sync/atomic"

	"github.com/oarkflow/fh"
)

type Config struct{ Key func(fh.Ctx) string }

const (
	maxRegistryKeys = 65_536
	maxKeyBytes     = 256
)

type lockEntry struct {
	mu    sync.Mutex
	shard *lockShard
	refs  int
}

type lockShard struct {
	mu      sync.Mutex
	entries map[string]*lockEntry
}

type lockRegistry struct {
	seed   maphash.Seed
	shards [64]lockShard
	active atomic.Int64
}

var registry = lockRegistry{seed: maphash.MakeSeed()}

func New(cfg Config) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		if cfg.Key == nil {
			return c.Next()
		}
		key := cfg.Key(c)
		if key == "" {
			return c.Next()
		}
		if len(key) > maxKeyBytes {
			return fh.NewHTTPError(fh.StatusBadRequest, "ACTOR_KEY_TOO_LARGE", "actor key is too long")
		}
		lock := acquireLock(key)
		if lock == nil {
			return fh.NewHTTPError(fh.StatusServiceUnavailable, "ACTOR_CAPACITY", "actor lock capacity reached")
		}
		lock.mu.Lock()
		defer releaseLock(key, lock)
		return c.Next()
	}
}

func acquireLock(key string) *lockEntry {
	shard := &registry.shards[maphash.String(registry.seed, key)%uint64(len(registry.shards))]
	shard.mu.Lock()
	entry := shard.entries[key]
	if entry == nil {
		if registry.active.Add(1) > maxRegistryKeys {
			registry.active.Add(-1)
			shard.mu.Unlock()
			return nil
		}
		if shard.entries == nil {
			shard.entries = make(map[string]*lockEntry)
		}
		entry = &lockEntry{shard: shard}
		shard.entries[key] = entry
	}
	entry.refs++
	shard.mu.Unlock()
	return entry
}

func releaseLock(key string, entry *lockEntry) {
	entry.mu.Unlock()
	shard := entry.shard
	shard.mu.Lock()
	entry.refs--
	if entry.refs == 0 && shard.entries[key] == entry {
		delete(shard.entries, key)
		registry.active.Add(-1)
	}
	shard.mu.Unlock()
}
