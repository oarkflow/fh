package kv

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

const defaultMaxMemoryValueBytes = 8 << 20
const defaultMaxKeyBytes = 4096
const defaultMaxMemoryEntries = 100_000
const defaultMaxMemoryBytes int64 = 256 << 20

// MemoryStore is an in-process, sharded implementation of Store. It is the
// default backend for every middleware package that embeds a kv.Store — fast,
// non-durable, and safe for concurrent use across many goroutines via
// per-shard locking (mirroring the sharding scheme mw/ratelimiter's original
// hand-rolled MemoryStore used, generalized here for reuse).
type MemoryStore struct {
	shards        []*memShard
	seed          maphash.Seed
	maxEntries    int // per-store soft cap, spread across shards; 0 = unlimited
	maxValueBytes int
	maxKeyBytes   int
	rejectFull    bool

	closeOnce sync.Once
	stopGC    chan struct{}
	gcDone    chan struct{}
	closed    atomic.Bool
}

type memShard struct {
	mu         sync.Mutex
	data       map[string]entry
	maxEntries int
	bounded    bool
	maxBytes   int64
	usedBytes  int64
}

// MemoryOption configures a MemoryStore.
type MemoryOption func(*memoryConfig)

type memoryConfig struct {
	shardCount     int
	maxEntries     int
	maxValueBytes  int
	maxKeyBytes    int
	maxMemoryBytes int64
	gcInterval     time.Duration
	rejectFull     bool
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

// WithMaxEntries sets the soft cap on total stored entries, enforced by
// evicting expired entries first and then an arbitrary live entry within a
// shard when it is full. The cap is divided across shards, so a skewed key
// distribution can fill one shard and trigger eviction or rejection before
// the store reaches the aggregate limit. The default is 100,000 entries. Set
// n to 0 to disable the count limit; negative values leave the default
// unchanged.
func WithMaxEntries(n int) MemoryOption {
	return func(c *memoryConfig) {
		if n >= 0 {
			c.maxEntries = n
		}
	}
}

// WithMaxValueBytes sets the maximum size of one stored value. The default is
// 8 MiB, matching FileStore. Larger values require an explicit override.
func WithMaxValueBytes(n int) MemoryOption {
	return func(c *memoryConfig) {
		if n > 0 {
			c.maxValueBytes = n
		}
	}
}

// WithMaxMemoryKeyBytes sets the maximum key length. The default is 4 KiB; larger
// keys require an explicit override.
func WithMaxMemoryKeyBytes(n int) MemoryOption {
	return func(c *memoryConfig) {
		if n > 0 {
			c.maxKeyBytes = n
		}
	}
}

// WithMaxMemoryBytes sets the maximum logical bytes stored across keys and
// values; allocator and map overhead are not included. The default is 256 MiB.
// The budget is divided evenly among shards, so skewed key distribution can
// reject writes before the whole-store limit is reached. Set n to 0 to disable
// this budget.
func WithMaxMemoryBytes(n int64) MemoryOption {
	return func(c *memoryConfig) {
		if n >= 0 {
			c.maxMemoryBytes = n
		}
	}
}

// WithRejectWhenFull makes Set and Mutate return ErrCapacityExceeded when the
// target shard is full of live entries. The default behavior evicts a live
// entry to make room; rejection is appropriate for replay and rate-limit
// state where early eviction could weaken protection. The byte budget always
// rejects writes that exceed its shard quota.
func WithRejectWhenFull() MemoryOption {
	return func(c *memoryConfig) { c.rejectFull = true }
}

// WithGCInterval starts a background goroutine that sweeps expired entries
// every interval. 0 (default) disables background sweeping — expired entries
// are still skipped lazily on Get and evicted opportunistically on Set.
func WithGCInterval(d time.Duration) MemoryOption {
	return func(c *memoryConfig) { c.gcInterval = d }
}

// NewMemoryStore creates a ready-to-use MemoryStore with a 4 KiB per-key cap,
// a 100,000-entry soft cap, an 8 MiB per-value cap, and a 256 MiB logical
// key-and-value budget.
// Configure explicit limits for the expected workload; use WithRejectWhenFull
// for state that must never evict live entries.
func NewMemoryStore(opts ...MemoryOption) *MemoryStore {
	cfg := memoryConfig{shardCount: 16, maxEntries: defaultMaxMemoryEntries, maxValueBytes: defaultMaxMemoryValueBytes, maxKeyBytes: defaultMaxKeyBytes, maxMemoryBytes: defaultMaxMemoryBytes}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.shardCount <= 0 {
		cfg.shardCount = 16
	}
	if cfg.maxEntries > 0 && cfg.shardCount > cfg.maxEntries {
		cfg.shardCount = cfg.maxEntries
	}
	if cfg.maxMemoryBytes > 0 && int64(cfg.shardCount) > cfg.maxMemoryBytes {
		// Avoid creating shards with a zero-byte quota. Such shards can never
		// accept a normal non-empty key even while other shards have capacity.
		cfg.shardCount = int(cfg.maxMemoryBytes)
	}
	s := &MemoryStore{
		shards:        make([]*memShard, cfg.shardCount),
		seed:          maphash.MakeSeed(),
		maxEntries:    cfg.maxEntries,
		maxValueBytes: cfg.maxValueBytes,
		maxKeyBytes:   cfg.maxKeyBytes,
		rejectFull:    cfg.rejectFull,
	}
	perShardEntries := 0
	remainingEntries := 0
	if cfg.maxEntries > 0 {
		perShardEntries = cfg.maxEntries / len(s.shards)
		remainingEntries = cfg.maxEntries % len(s.shards)
	}
	perShardBytes := cfg.maxMemoryBytes / int64(len(s.shards))
	remainingBytes := cfg.maxMemoryBytes % int64(len(s.shards))
	for i := range s.shards {
		shardBytes := perShardBytes
		if int64(i) < remainingBytes {
			shardBytes++
		}
		shardEntries := perShardEntries
		if i < remainingEntries {
			shardEntries++
		}
		s.shards[i] = &memShard{data: make(map[string]entry), maxEntries: shardEntries, bounded: cfg.maxMemoryBytes > 0, maxBytes: shardBytes}
	}
	if cfg.gcInterval > 0 {
		s.stopGC = make(chan struct{})
		s.gcDone = make(chan struct{})
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
	return s.closed.Load()
}

// CapacityLimit returns the configured aggregate key limit when the store
// rejects new keys at capacity, or zero when it may evict live keys or has no
// limit. Per-shard enforcement may reject earlier when keys are unevenly
// distributed.
func (s *MemoryStore) CapacityLimit() int {
	if !s.rejectFull {
		return 0
	}
	return s.maxEntries
}

func logicalEntryBytes(key string, value []byte) int64 {
	keyBytes, valueBytes := int64(len(key)), int64(len(value))
	if keyBytes > int64(^uint64(0)>>1)-valueBytes {
		return int64(^uint64(0) >> 1)
	}
	return keyBytes + valueBytes
}

// adjustMemoryBytes reserves the change in logical key/value bytes. The
// caller holds shard.mu, so accounting adds no cross-shard contention.
func (s *MemoryStore) adjustMemoryBytes(shard *memShard, oldBytes, newBytes int64) bool {
	if !shard.bounded {
		return true
	}
	if shard.usedBytes < oldBytes {
		return false
	}
	base := shard.usedBytes - oldBytes
	if newBytes < 0 || base > shard.maxBytes || newBytes > shard.maxBytes-base {
		return false
	}
	shard.usedBytes = base + newBytes
	return true
}

func (s *MemoryStore) Get(key string) ([]byte, bool, error) {
	if s.isClosed() {
		return nil, false, ErrClosed
	}
	if len(key) > s.maxKeyBytes {
		return nil, false, ErrKeyTooLarge
	}
	shard := s.shardFor(key)
	now := time.Now()
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if s.isClosed() {
		return nil, false, ErrClosed
	}
	e, ok := shard.data[key]
	if !ok {
		return nil, false, nil
	}
	if e.expired(now) {
		delete(shard.data, key)
		_ = s.adjustMemoryBytes(shard, logicalEntryBytes(key, e.Value), 0)
		return nil, false, nil
	}
	out := append([]byte(nil), e.Value...)
	return out, true, nil
}

func (s *MemoryStore) Set(key string, value []byte, ttl time.Duration) error {
	if s.isClosed() {
		return ErrClosed
	}
	if len(value) > s.maxValueBytes {
		return ErrTooLarge
	}
	if len(key) > s.maxKeyBytes {
		return ErrKeyTooLarge
	}
	var expiresAt time.Time
	now := time.Now()
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	shard := s.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if s.isClosed() {
		return ErrClosed
	}
	old, exists := shard.data[key]
	if exists && old.expired(now) {
		delete(shard.data, key)
		_ = s.adjustMemoryBytes(shard, logicalEntryBytes(key, old.Value), 0)
		exists = false
	}
	oldBytes := int64(0)
	if exists {
		oldBytes = logicalEntryBytes(key, old.Value)
	}
	newBytes := logicalEntryBytes(key, value)
	if !s.adjustMemoryBytes(shard, oldBytes, newBytes) {
		// Expired entries may be the only reason this shard is out of space.
		// Sweep them once under pressure, then retry the reservation before
		// rejecting the write.
		s.evictExpiredLocked(shard, now)
		if !s.adjustMemoryBytes(shard, oldBytes, newBytes) {
			return ErrCapacityExceeded
		}
	}
	if shard.maxEntries > 0 {
		if !exists {
			if len(shard.data) >= shard.maxEntries {
				if !s.evictOneLocked(shard, now, s.rejectFull) {
					_ = s.adjustMemoryBytes(shard, newBytes, oldBytes)
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
func (s *MemoryStore) evictOneLocked(shard *memShard, now time.Time, rejectLive bool) bool {
	for k, e := range shard.data {
		if e.expired(now) {
			delete(shard.data, k)
			_ = s.adjustMemoryBytes(shard, logicalEntryBytes(k, e.Value), 0)
			return true
		}
	}
	if !rejectLive {
		for k, e := range shard.data {
			delete(shard.data, k)
			_ = s.adjustMemoryBytes(shard, logicalEntryBytes(k, e.Value), 0)
			return true
		}
	}
	return false
}

func (s *MemoryStore) evictExpiredLocked(shard *memShard, now time.Time) bool {
	removed := false
	for k, e := range shard.data {
		if !e.expired(now) {
			continue
		}
		delete(shard.data, k)
		_ = s.adjustMemoryBytes(shard, logicalEntryBytes(k, e.Value), 0)
		removed = true
	}
	return removed
}

func (s *MemoryStore) Mutate(key string, fn func(current []byte, exists bool) (next []byte, ttl time.Duration, ok bool, err error)) error {
	if s.isClosed() {
		return ErrClosed
	}
	if len(key) > s.maxKeyBytes {
		return ErrKeyTooLarge
	}
	shard := s.shardFor(key)
	now := time.Now()
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if s.isClosed() {
		return ErrClosed
	}

	e, exists := shard.data[key]
	if exists && e.expired(now) {
		delete(shard.data, key)
		_ = s.adjustMemoryBytes(shard, logicalEntryBytes(key, e.Value), 0)
		exists = false
	}
	var current []byte
	if exists {
		// The callback receives an isolated snapshot. Passing e.Value directly
		// would let it mutate stored state even when it returns an error or
		// ok=false, and could let a retained slice change the store later.
		current = append([]byte(nil), e.Value...)
	}
	next, ttl, ok, err := fn(current, exists)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if len(next) > s.maxValueBytes {
		return ErrTooLarge
	}
	oldBytes := int64(0)
	if exists {
		oldBytes = logicalEntryBytes(key, e.Value)
	}
	newBytes := logicalEntryBytes(key, next)
	if !s.adjustMemoryBytes(shard, oldBytes, newBytes) {
		// Match Set's byte-pressure recovery so TTL state does not make an
		// otherwise admissible atomic mutation fail.
		s.evictExpiredLocked(shard, now)
		if !s.adjustMemoryBytes(shard, oldBytes, newBytes) {
			return ErrCapacityExceeded
		}
	}
	if shard.maxEntries > 0 && !exists {
		if len(shard.data) >= shard.maxEntries {
			if !s.evictOneLocked(shard, now, s.rejectFull) {
				_ = s.adjustMemoryBytes(shard, newBytes, oldBytes)
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
	if len(key) > s.maxKeyBytes {
		return ErrKeyTooLarge
	}
	shard := s.shardFor(key)
	shard.mu.Lock()
	if s.isClosed() {
		shard.mu.Unlock()
		return ErrClosed
	}
	if e, ok := shard.data[key]; ok {
		delete(shard.data, key)
		_ = s.adjustMemoryBytes(shard, logicalEntryBytes(key, e.Value), 0)
	}
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
		if s.isClosed() {
			shard.mu.Unlock()
			return 0, ErrClosed
		}
		for k, e := range shard.data {
			if e.expired(now) {
				delete(shard.data, k)
				_ = s.adjustMemoryBytes(shard, logicalEntryBytes(k, e.Value), 0)
				continue
			}
			total++
		}
		shard.mu.Unlock()
	}
	return total, nil
}

func (s *MemoryStore) gcLoop(interval time.Duration) {
	defer close(s.gcDone)
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
		if s.isClosed() {
			shard.mu.Unlock()
			return
		}
		for k, e := range shard.data {
			if e.expired(now) {
				delete(shard.data, k)
				_ = s.adjustMemoryBytes(shard, logicalEntryBytes(k, e.Value), 0)
			}
		}
		shard.mu.Unlock()
	}
}

// Close stops the background GC goroutine and releases stored entries. Safe to
// call multiple times.
func (s *MemoryStore) Close() error {
	s.closed.Store(true)
	s.closeOnce.Do(func() {
		if s.stopGC != nil {
			close(s.stopGC)
		}
	})
	if s.gcDone != nil {
		<-s.gcDone
	}
	for _, shard := range s.shards {
		shard.mu.Lock()
		shard.data = nil
		shard.usedBytes = 0
		shard.mu.Unlock()
	}
	return nil
}
