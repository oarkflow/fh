package kv

import (
	"hash/maphash"
	"sync"
	"time"
)

// MemoryStore is an in-process, sharded implementation of Store. It is the
// default backend for every middleware package that embeds a kv.Store — fast,
// non-durable, and safe for concurrent use across many goroutines via
// per-shard locking (mirroring the sharding scheme mw/ratelimiter's original
// hand-rolled MemoryStore used, generalized here for reuse).
type MemoryStore struct {
	shards     []*memShard
	seed       maphash.Seed
	maxEntries int // per-store soft cap, spread across shards; 0 = unlimited

	closeOnce sync.Once
	stopGC    chan struct{}
	closed    bool
	mu        sync.Mutex // guards closed/stopGC lifecycle only
}

type memShard struct {
	mu   sync.Mutex
	data map[string]entry
}

// MemoryOption configures a MemoryStore.
type MemoryOption func(*memoryConfig)

type memoryConfig struct {
	shardCount int
	maxEntries int
	gcInterval time.Duration
}

// WithShardCount sets the number of internal shards (default 16). Higher
// values reduce lock contention under high concurrency at the cost of more
// memory overhead; only tune this after profiling.
func WithShardCount(n int) MemoryOption {
	return func(c *memoryConfig) {
		if n > 0 {
			c.shardCount = n
		}
	}
}

// WithMaxEntries sets a soft cap on total stored entries, enforced by
// evicting expired-then-oldest entries within a shard when it is full.
// 0 (default) means unlimited.
func WithMaxEntries(n int) MemoryOption {
	return func(c *memoryConfig) { c.maxEntries = n }
}

// WithGCInterval starts a background goroutine that sweeps expired entries
// every interval. 0 (default) disables background sweeping — expired entries
// are still skipped lazily on Get and evicted opportunistically on Set.
func WithGCInterval(d time.Duration) MemoryOption {
	return func(c *memoryConfig) { c.gcInterval = d }
}

// NewMemoryStore creates a ready-to-use MemoryStore.
func NewMemoryStore(opts ...MemoryOption) *MemoryStore {
	cfg := memoryConfig{shardCount: 16}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.shardCount <= 0 {
		cfg.shardCount = 16
	}
	s := &MemoryStore{
		shards:     make([]*memShard, cfg.shardCount),
		seed:       maphash.MakeSeed(),
		maxEntries: cfg.maxEntries,
	}
	for i := range s.shards {
		s.shards[i] = &memShard{data: make(map[string]entry)}
	}
	if cfg.gcInterval > 0 {
		s.stopGC = make(chan struct{})
		go s.gcLoop(cfg.gcInterval)
	}
	return s
}

func (s *MemoryStore) shardFor(key string) *memShard {
	var h maphash.Hash
	h.SetSeed(s.seed)
	_, _ = h.WriteString(key)
	return s.shards[h.Sum64()%uint64(len(s.shards))]
}

func (s *MemoryStore) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *MemoryStore) Get(key string) ([]byte, bool, error) {
	if s.isClosed() {
		return nil, false, ErrClosed
	}
	shard := s.shardFor(key)
	now := time.Now()
	shard.mu.Lock()
	defer shard.mu.Unlock()
	e, ok := shard.data[key]
	if !ok {
		return nil, false, nil
	}
	if e.expired(now) {
		delete(shard.data, key)
		return nil, false, nil
	}
	out := append([]byte(nil), e.Value...)
	return out, true, nil
}

func (s *MemoryStore) Set(key string, value []byte, ttl time.Duration) error {
	if s.isClosed() {
		return ErrClosed
	}
	var expiresAt time.Time
	now := time.Now()
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	shard := s.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if s.maxEntries > 0 {
		if _, exists := shard.data[key]; !exists {
			perShardCap := s.maxEntries / len(s.shards)
			if perShardCap <= 0 {
				perShardCap = 1
			}
			if len(shard.data) >= perShardCap {
				if !evictOne(shard, now) {
					return ErrCapacityExceeded
				}
			}
		}
	}
	shard.data[key] = entry{Value: append([]byte(nil), value...), ExpiresAt: expiresAt}
	return nil
}

// evictOne removes one expired entry if any exists, otherwise the oldest
// (arbitrary, since Go map iteration order is randomized — acceptable for a
// soft capacity guard) entry in the shard. Returns false if the shard was
// already empty (nothing to evict, caller must reject the new key).
func evictOne(shard *memShard, now time.Time) bool {
	for k, e := range shard.data {
		if e.expired(now) {
			delete(shard.data, k)
			return true
		}
	}
	for k := range shard.data {
		delete(shard.data, k)
		return true
	}
	return false
}

func (s *MemoryStore) Mutate(key string, fn func(current []byte, exists bool) (next []byte, ttl time.Duration, ok bool, err error)) error {
	if s.isClosed() {
		return ErrClosed
	}
	shard := s.shardFor(key)
	now := time.Now()
	shard.mu.Lock()
	defer shard.mu.Unlock()

	e, exists := shard.data[key]
	if exists && e.expired(now) {
		exists = false
	}
	var current []byte
	if exists {
		current = e.Value
	}
	next, ttl, ok, err := fn(current, exists)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if s.maxEntries > 0 && !exists {
		perShardCap := s.maxEntries / len(s.shards)
		if perShardCap <= 0 {
			perShardCap = 1
		}
		if len(shard.data) >= perShardCap {
			if !evictOne(shard, now) {
				return ErrCapacityExceeded
			}
		}
	}
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	shard.data[key] = entry{Value: append([]byte(nil), next...), ExpiresAt: expiresAt}
	return nil
}

func (s *MemoryStore) Delete(key string) error {
	if s.isClosed() {
		return ErrClosed
	}
	shard := s.shardFor(key)
	shard.mu.Lock()
	delete(shard.data, key)
	shard.mu.Unlock()
	return nil
}

func (s *MemoryStore) Len() (int, error) {
	if s.isClosed() {
		return 0, ErrClosed
	}
	now := time.Now()
	total := 0
	for _, shard := range s.shards {
		shard.mu.Lock()
		for k, e := range shard.data {
			if e.expired(now) {
				delete(shard.data, k)
				continue
			}
			total++
		}
		shard.mu.Unlock()
	}
	return total, nil
}

func (s *MemoryStore) gcLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sweep()
		case <-s.stopGC:
			return
		}
	}
}

func (s *MemoryStore) sweep() {
	now := time.Now()
	for _, shard := range s.shards {
		shard.mu.Lock()
		for k, e := range shard.data {
			if e.expired(now) {
				delete(shard.data, k)
			}
		}
		shard.mu.Unlock()
	}
}

// Close stops any background GC goroutine. Safe to call multiple times.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.closeOnce.Do(func() {
		if s.stopGC != nil {
			close(s.stopGC)
		}
	})
	return nil
}
