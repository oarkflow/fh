package deviceauth

import (
	"crypto/ed25519"
	"crypto/sha256"
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
	if !validDeviceID(id) {
		return "", nil, false, ErrInvalid
	}
	v, ok := s.items.Load(id)
	if !ok {
		return "", nil, false, nil
	}
	d := v.(memoryDevice)
	return d.subject, append(ed25519.PublicKey(nil), d.key...), true, nil
}
func (s *MemoryDevices) Put(id, subject string, key ed25519.PublicKey) error {
	if !validDeviceID(id) || len(subject) == 0 || len(subject) > 256 || len(key) != ed25519.PublicKeySize {
		return ErrInvalid
	}
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
	if !validDeviceID(id) {
		return ErrInvalid
	}
	if _, ok := s.items.LoadAndDelete(id); ok {
		s.count.Add(-1)
	}
	return nil
}

func validDeviceID(id string) bool {
	return len(id) > 0 && len(id) <= 128
}

// MemoryReplayGuard is suitable only for a single process. Use a shared store
// with an atomic insert-if-absent operation for multiple nodes.
type MemoryReplayGuard struct {
	shards      [64]replayShard
	MaxEntries  int
	MaxKeyBytes int
	count       atomic.Int64
	seed        maphash.Seed
	Clock       func() time.Time
}

type replayShard struct {
	mu         sync.Mutex
	items      map[[32]byte]time.Time
	nextSweep  time.Time
	maxEntries int
}

func NewMemoryReplayGuard(max int) *MemoryReplayGuard {
	if max <= 0 {
		max = 250_000
	}
	g := &MemoryReplayGuard{MaxEntries: max, MaxKeyBytes: 256, seed: maphash.MakeSeed()}
	perShard := (max + len(g.shards) - 1) / len(g.shards)
	for i := range g.shards {
		g.shards[i].items = make(map[[32]byte]time.Time)
		g.shards[i].maxEntries = perShard
	}
	return g
}
func (g *MemoryReplayGuard) Use(key string, expires time.Time) (bool, error) {
	if len(key) == 0 || len(key) > g.MaxKeyBytes {
		return false, ErrInvalid
	}
	keyHash := sha256.Sum256([]byte(key))
	shard := &g.shards[maphash.String(g.seed, key)%uint64(len(g.shards))]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	now := time.Now()
	if g.Clock != nil {
		now = g.Clock()
	}
	if !expires.After(now) {
		return false, ErrInvalid
	}
	if !now.Before(shard.nextSweep) || len(shard.items) >= shard.maxEntries {
		for k, t := range shard.items {
			if !t.After(now) {
				delete(shard.items, k)
				g.count.Add(-1)
			}
		}
		shard.nextSweep = now.Add(5 * time.Second)
	}
	if t, exists := shard.items[keyHash]; exists {
		if t.After(now) {
			return false, nil
		}
		delete(shard.items, keyHash)
		g.count.Add(-1)
	}
	if len(shard.items) >= shard.maxEntries {
		return false, ErrCapacity
	}
	if g.count.Add(1) > int64(g.MaxEntries) {
		g.count.Add(-1)
		return false, ErrCapacity
	}
	shard.items[keyHash] = expires
	return true, nil
}
