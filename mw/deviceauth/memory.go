package deviceauth

import (
	"crypto/ed25519"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryDevices is a bounded single-process device registry. Production clusters
// should provide a durable shared DeviceStore.
type MemoryDevices struct {
	items      sync.Map
	count      atomic.Int64
	MaxDevices int
}
type memoryDevice struct {
	subject string
	key     ed25519.PublicKey
}

func NewMemoryDevices(max int) *MemoryDevices {
	if max <= 0 {
		max = 100_000
	}
	return &MemoryDevices{MaxDevices: max}
}
func (s *MemoryDevices) Get(id string) (string, ed25519.PublicKey, bool, error) {
	v, ok := s.items.Load(id)
	if !ok {
		return "", nil, false, nil
	}
	d := v.(memoryDevice)
	return d.subject, append(ed25519.PublicKey(nil), d.key...), true, nil
}
func (s *MemoryDevices) Put(id, subject string, key ed25519.PublicKey) error {
	if _, ok := s.items.Load(id); ok {
		return ErrDeviceExists
	}
	if s.count.Add(1) > int64(s.MaxDevices) {
		s.count.Add(-1)
		return ErrCapacity
	}
	_, loaded := s.items.LoadOrStore(id, memoryDevice{subject: subject, key: append(ed25519.PublicKey(nil), key...)})
	if loaded {
		s.count.Add(-1)
		return ErrDeviceExists
	}
	return nil
}
func (s *MemoryDevices) Delete(id string) error {
	if _, ok := s.items.LoadAndDelete(id); ok {
		s.count.Add(-1)
	}
	return nil
}

// MemoryReplayGuard is suitable only for a single process. Use a shared store
// with an atomic insert-if-absent operation for multiple nodes.
type MemoryReplayGuard struct {
	shards     [64]replayShard
	MaxEntries int
	seed       maphash.Seed
	Clock      func() time.Time
}

type replayShard struct {
	mu         sync.Mutex
	items      map[string]time.Time
	nextSweep  time.Time
	maxEntries int
}

func NewMemoryReplayGuard(max int) *MemoryReplayGuard {
	if max <= 0 {
		max = 250_000
	}
	g := &MemoryReplayGuard{MaxEntries: max, seed: maphash.MakeSeed()}
	perShard := (max + len(g.shards) - 1) / len(g.shards)
	for i := range g.shards {
		g.shards[i].items = make(map[string]time.Time)
		g.shards[i].maxEntries = perShard
	}
	return g
}
func (g *MemoryReplayGuard) Use(key string, expires time.Time) (bool, error) {
	shard := &g.shards[maphash.String(g.seed, key)%uint64(len(g.shards))]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	now := time.Now()
	if g.Clock != nil {
		now = g.Clock()
	}
	if !now.Before(shard.nextSweep) || len(shard.items) >= shard.maxEntries {
		for k, t := range shard.items {
			if !t.After(now) {
				delete(shard.items, k)
			}
		}
		shard.nextSweep = now.Add(5 * time.Second)
	}
	if t, exists := shard.items[key]; exists {
		if t.After(now) {
			return false, nil
		}
		delete(shard.items, key)
	}
	if len(shard.items) >= shard.maxEntries {
		return false, ErrCapacity
	}
	shard.items[key] = expires
	return true, nil
}
