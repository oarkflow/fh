package slidingwindow

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// fileEntry pairs the live in-memory sliding-window log (the same trimmed
// timestamp slice windowState/allowViaStore operate on) with bookkeeping for
// deciding when to flush it to the wrapped kv.FileStore.
type fileEntry struct {
	timestamps []time.Time
	dirty      bool
	lastFlush  time.Time
	sinceFlush int
}

// FileStore is a file-backed Store implementing the same sliding-window
// algorithm as MemoryStore, with per-key state additionally persisted
// through a kv.FileStore so it survives a process restart.
//
// Design tradeoff: a sliding-window log is checked and mutated on every
// single request, and this is meant to sit on a hot path. Fsync-per-request
// (which is what kv.FileStore.Set/Mutate does — see its doc comment) would
// turn every incoming request into a blocking disk write, which is
// unacceptable throughput-wise for a rate limiter. So FileStore keeps the
// authoritative, always-consistent state in memory (identical admission
// algorithm to MemoryStore, via the same allowViaStore-shaped logic inlined
// below, so correctness of the Allow decision itself is unaffected) and
// only coalesces writes to the wrapped kv.FileStore: a key's state is
// persisted at most once per flushInterval, or immediately if flushEvery
// request-increments have accumulated without a flush, whichever comes
// first. On a clean process exit callers should invoke Flush to persist any
// state written since the last coalesced write; on a crash, at most
// flushInterval worth of very recent window entries (or flushEvery
// requests, whichever triggers first) can be lost, which only makes the
// limiter briefly more permissive right after restart, never less
// permissive than intended. On startup, an existing on-disk snapshot for a
// key is lazily loaded (via kv.FileStore.Get) the first time that key is
// seen again, so state is durable across restarts as advertised.
type FileStore struct {
	kv         *kv.FileStore
	rate       int
	burst      int
	windowSize time.Duration
	maxKeys    int

	flushInterval time.Duration
	flushEvery    int

	mu      sync.Mutex
	entries map[string]*fileEntry

	stopGC   chan struct{}
	stopOnce sync.Once
	initErr  error
}

// FileStoreConfig configures a FileStore's sliding-window algorithm
// parameters (mirroring Config's Rate/Burst/Window/MaxKeys) plus the
// write-coalescing knobs described on FileStore.
type FileStoreConfig struct {
	Rate       int
	Burst      int
	Window     time.Duration
	MaxKeys    int
	GCInterval time.Duration

	// FlushInterval is the minimum time between persisting a given key's
	// state to disk. Default: 1 second.
	FlushInterval time.Duration

	// FlushEvery forces a flush of a key's state once this many requests
	// have been recorded for it since the last flush, even if
	// FlushInterval hasn't elapsed. Default: 20.
	FlushEvery int
}

// NewFileStore creates a FileStore rooted at dir using the given algorithm
// and coalescing parameters. Pass 0 for GCInterval to disable automatic
// background GC of in-memory (and, transitively, on-disk-if-flushed)
// entries that have gone idle.
func NewFileStore(dir string, cfg FileStoreConfig) *FileStore {
	if cfg.Rate <= 0 {
		cfg.Rate = 100
	}
	if cfg.Burst <= 0 {
		cfg.Burst = cfg.Rate
	}
	if cfg.Window <= 0 {
		cfg.Window = time.Second
	}
	if cfg.MaxKeys <= 0 {
		cfg.MaxKeys = 65536
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.FlushEvery <= 0 {
		cfg.FlushEvery = 20
	}

	// GC of the working set is driven locally (see gcLoop/gc below), not by
	// kv.FileStore's own WithFileGCInterval, so FileStore keeps exactly the
	// same "flush-then-drop-from-memory" semantics it had before this
	// retrofit rather than kv.FileStore unilaterally deleting a file kv
	// doesn't know is still logically live.
	kvStore, err := kv.NewFileStore(dir)

	s := &FileStore{
		kv:            kvStore,
		rate:          cfg.Rate,
		burst:         cfg.Burst,
		windowSize:    cfg.Window,
		maxKeys:       cfg.MaxKeys,
		flushInterval: cfg.FlushInterval,
		flushEvery:    cfg.FlushEvery,
		entries:       make(map[string]*fileEntry),
		initErr:       err,
	}
	if err == nil && cfg.GCInterval > 0 {
		s.stopGC = make(chan struct{})
		go s.gcLoop(cfg.GCInterval)
	}
	return s
}

// StopGC stops the background garbage collection goroutine. The store
// remains usable afterward.
func (s *FileStore) StopGC() {
	if s.stopGC != nil {
		s.stopOnce.Do(func() { close(s.stopGC) })
	}
}

func (s *FileStore) gcLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.gc()
		case <-s.stopGC:
			return
		}
	}
}

func (s *FileStore) gc() {
	now := time.Now()
	cutoff := now.Add(-s.windowSize * 2)
	s.mu.Lock()
	for k, e := range s.entries {
		if len(e.timestamps) == 0 || e.timestamps[len(e.timestamps)-1].Before(cutoff) {
			if e.dirty {
				_ = s.flushLocked(k, e, now)
			}
			delete(s.entries, k)
		}
	}
	s.mu.Unlock()
}

// Flush persists every key with unwritten in-memory state to disk. Call it
// before process shutdown to minimize the window of state that a crash
// could lose.
func (s *FileStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var firstErr error
	for k, e := range s.entries {
		if !e.dirty {
			continue
		}
		if err := s.flushLocked(k, e, now); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// loadLocked returns the in-memory entry for key, lazily hydrating it from
// the wrapped kv.FileStore on first use. Must be called with s.mu held.
func (s *FileStore) loadLocked(key string) *fileEntry {
	if e, ok := s.entries[key]; ok {
		return e
	}
	e := &fileEntry{timestamps: make([]time.Time, 0, s.rate*2)}
	if data, ok, err := s.kv.Get(key); err == nil && ok {
		var ws windowState
		if json.Unmarshal(data, &ws) == nil {
			e.timestamps = ws.Timestamps
		}
	}
	if len(s.entries) >= s.maxKeys {
		s.evictOldestLocked()
	}
	s.entries[key] = e
	return e
}

func (s *FileStore) evictOldestLocked() {
	var oldestKey string
	var oldestTime time.Time
	for k, e := range s.entries {
		if len(e.timestamps) > 0 {
			t := e.timestamps[0]
			if oldestKey == "" || t.Before(oldestTime) {
				oldestKey = k
				oldestTime = t
			}
		}
	}
	if oldestKey != "" {
		if e := s.entries[oldestKey]; e.dirty {
			_ = s.flushLocked(oldestKey, e, time.Now())
		}
		delete(s.entries, oldestKey)
	}
}

// flushLocked persists e's current window log through the wrapped
// kv.FileStore (atomic write, per kv.FileStore's own guarantees). Must be
// called with s.mu held.
func (s *FileStore) flushLocked(key string, e *fileEntry, now time.Time) error {
	ws := windowState{Timestamps: append([]time.Time(nil), e.timestamps...)}
	data, err := json.Marshal(ws)
	if err != nil {
		return err
	}
	if err := s.kv.Set(key, data, s.windowSize*2); err != nil {
		return err
	}
	e.dirty = false
	e.lastFlush = now
	e.sinceFlush = 0
	return nil
}

// Allow implements Store. The admission algorithm is identical to
// MemoryStore.Allow/allowViaStore (same expire-then-count-then-admit rule,
// same burst allowance); only the persistence strategy differs, per the
// FileStore doc comment — state changes are applied to the in-memory
// working copy first and only coalesced to the wrapped kv.FileStore on the
// flush schedule.
func (s *FileStore) Allow(key string) (bool, int, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.initErr != nil {
		return false, 0, 0
	}

	now := time.Now()
	e := s.loadLocked(key)

	cutoff := now.Add(-s.windowSize)
	trimmed := 0
	for trimmed < len(e.timestamps) && e.timestamps[trimmed].Before(cutoff) {
		trimmed++
	}
	if trimmed > 0 {
		e.timestamps = append([]time.Time(nil), e.timestamps[trimmed:]...)
	}

	currentCount := len(e.timestamps)
	remaining := s.rate - currentCount

	if remaining <= 0 {
		var retryAfter time.Duration
		if len(e.timestamps) > 0 {
			oldest := e.timestamps[0]
			retryAfter = s.windowSize - now.Sub(oldest)
			if retryAfter < 0 {
				retryAfter = 0
			}
		}
		s.maybeFlushLocked(key, e, now)
		return false, 0, retryAfter
	}

	if currentCount >= s.rate+s.burst {
		s.maybeFlushLocked(key, e, now)
		return false, 0, 0
	}

	e.timestamps = append(e.timestamps, now)
	e.dirty = true
	e.sinceFlush++
	s.maybeFlushLocked(key, e, now)

	return true, remaining - 1, 0
}

func (s *FileStore) maybeFlushLocked(key string, e *fileEntry, now time.Time) {
	if !e.dirty {
		return
	}
	if e.sinceFlush >= s.flushEvery || now.Sub(e.lastFlush) >= s.flushInterval {
		_ = s.flushLocked(key, e, now)
	}
}
