package cache

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// maxCachedBodySize bounds how large a single cached response body may be
// before FileStore refuses to persist it. Entry.Body is arbitrary HTTP
// response bytes backing this cache; without a cap, unbounded response
// bodies written to disk are a real disk-exhaustion risk. This is a
// defensive backstop independent of Config.MaxBodySize.
const maxCachedBodySize = 8 << 20 // 8MB

// maxStoredEntrySize bounds the kv.FileStore value size (the JSON-encoded
// Entry, whose Body is base64-encoded inside that JSON and so is ~4/3 the
// size of maxCachedBodySize, plus a little metadata overhead). The explicit
// maxCachedBodySize check on the raw Body below is the operative gate; this
// is only a generous backstop so kv's own cap never rejects an entry that
// already passed the raw-body check.
const maxStoredEntrySize = maxCachedBodySize + maxCachedBodySize/2 + 4096

// FileStore is a file-backed Store. Each cache entry is persisted via the
// shared kv.FileStore primitive (one JSON file per key, named after a hash
// of the key, written atomically), so arbitrary keys (which may be full
// request paths + query strings) never become filesystem paths directly.
//
// kv.Store has no way to enumerate its keys, which the original
// oldest-entry-first eviction policy needs. Rather than reimplement kv's
// directory scanning/locking locally to get that enumeration, FileStore
// keeps a small side index of key -> Entry.Created purely for eviction
// ranking; the authoritative entry data still lives in the kv store.
type FileStore struct {
	store      *kv.FileStore
	maxEntries int
	initErr    error

	mu      sync.Mutex
	created map[string]time.Time // key -> Entry.Created, for oldest-eviction ranking only
}

// NewFileStore creates a FileStore rooted at dir. maxEntries optionally
// bounds the number of tracked entries (default 1024); once at capacity,
// Set evicts the oldest entry first.
func NewFileStore(dir string, maxEntries int) *FileStore {
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	store, err := kv.NewFileStore(dir, kv.WithMaxEntrySize(maxStoredEntrySize))
	return &FileStore{
		store:      store,
		maxEntries: maxEntries,
		initErr:    err,
		created:    make(map[string]time.Time),
	}
}

// NewFileStoreWithGC creates a FileStore with a background eviction loop
// that runs every gcInterval, removing expired entries. Pass 0 for
// gcInterval to disable it (equivalent to NewFileStore). Use StopGC to stop
// the loop.
func NewFileStoreWithGC(dir string, maxEntries int, gcInterval time.Duration) *FileStore {
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	opts := []kv.FileOption{kv.WithMaxEntrySize(maxStoredEntrySize)}
	if gcInterval > 0 {
		opts = append(opts, kv.WithFileGCInterval(gcInterval))
	}
	store, err := kv.NewFileStore(dir, opts...)
	return &FileStore{
		store:      store,
		maxEntries: maxEntries,
		initErr:    err,
		created:    make(map[string]time.Time),
	}
}

// StopGC stops the background garbage collection goroutine, if any.
func (s *FileStore) StopGC() {
	if s.store != nil {
		_ = s.store.Close()
	}
}

// GC removes all expired entries from disk.
func (s *FileStore) GC() {
	if s.initErr != nil || s.store == nil {
		return
	}
	s.store.GC()
}

func (s *FileStore) Get(key string) (Entry, bool) {
	if s.initErr != nil {
		return Entry{}, false
	}
	data, ok, err := s.store.Get(key)
	if err != nil || !ok {
		s.mu.Lock()
		delete(s.created, key)
		s.mu.Unlock()
		return Entry{}, false
	}
	var e Entry
	if json.Unmarshal(data, &e) != nil {
		return Entry{}, false
	}
	return e, true
}

func (s *FileStore) Set(key string, e Entry) {
	if s.initErr != nil {
		return
	}
	if len(e.Body) > maxCachedBodySize {
		return
	}
	data, err := json.Marshal(&e)
	if err != nil {
		return
	}
	ttl := time.Until(e.Expires)
	if ttl <= 0 {
		ttl = time.Nanosecond
	}

	s.mu.Lock()
	if _, exists := s.created[key]; !exists && len(s.created) >= s.maxEntries {
		s.evictOldestLocked()
	}
	s.created[key] = e.Created
	s.mu.Unlock()

	if err := s.store.Set(key, data, ttl); err != nil {
		s.mu.Lock()
		delete(s.created, key)
		s.mu.Unlock()
	}
}

// evictOldestLocked removes the single tracked key with the oldest Created
// time. Caller holds s.mu.
func (s *FileStore) evictOldestLocked() {
	var oldestKey string
	var oldest time.Time
	first := true
	for candidate, created := range s.created {
		if first || created.Before(oldest) {
			oldestKey, oldest = candidate, created
			first = false
		}
	}
	if oldestKey != "" {
		delete(s.created, oldestKey)
		_ = s.store.Delete(oldestKey)
	}
}

func (s *FileStore) Delete(key string) {
	if s.initErr != nil {
		return
	}
	s.mu.Lock()
	delete(s.created, key)
	s.mu.Unlock()
	_ = s.store.Delete(key)
}
