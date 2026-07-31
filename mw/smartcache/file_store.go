package smartcache

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// maxCachedBodySize caps the size of a Response's body persisted to disk.
// This store backs an HTTP response cache; without a cap, an
// attacker-influenced or simply large upstream response could grow the
// on-disk cache without bound and exhaust disk space. Entries whose body
// exceeds this are silently skipped on Set (treated as a cache miss going
// forward), matching the "best effort" nature of a cache. This is checked as
// a pre-check against the raw body (matching the pre-kv.FileStore
// behavior exactly) rather than against the JSON-serialized envelope size,
// since base64-inflating the body before comparing would reject some bodies
// the old implementation accepted.
const maxCachedBodySize = 8 << 20 // 8MB

// maxCacheEntrySize bounds how large the full JSON-serialized Response
// (headers + body + kv's own entry envelope) may be before the underlying
// kv.FileStore refuses to persist it — a defensive backstop below the
// pre-check above, leaving headroom for JSON/header overhead.
const maxCacheEntrySize = 16 << 20 // 16MB

// FileStore is a file-backed implementation of Store. It is a thin adapter
// over kv.FileStore: it JSON-encodes/decodes *Response around the shared
// mechanics (atomic writes, key-to-filename hashing, background expiry
// sweeps) instead of reimplementing them. Every method fails safe — any
// underlying kv error is treated as a miss/no-op, since the Store interface
// this adapts to has no error return.
type FileStore struct {
	mu         sync.RWMutex
	store      *kv.FileStore
	dir        string
	gcInterval time.Duration
	initErr    error
}

// NewFileStore creates a FileStore rooted at dir. Pass 0 for gcInterval to
// disable the automatic background GC loop.
func NewFileStore(dir string, gcInterval time.Duration) *FileStore {
	store, err := kv.NewFileStore(dir, kv.WithMaxEntrySize(maxCacheEntrySize), kv.WithFileGCInterval(gcInterval))
	return &FileStore{store: store, dir: dir, gcInterval: gcInterval, initErr: err}
}

// StopGC stops the background garbage collection goroutine. It fully closes
// the underlying kv.FileStore (kv.Store has no narrower "stop GC only but
// keep serving" primitive), so — matching how callers in this package
// actually use it — it should be treated as shutdown, not a pause.
func (s *FileStore) StopGC() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.store != nil {
		_ = s.store.Close()
	}
}

// GC removes all expired cache entries from disk immediately (in addition to
// whatever background GC interval was configured).
func (s *FileStore) GC() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.initErr != nil || s.store == nil {
		return
	}
	s.store.GC()
}

func (s *FileStore) Get(key string) (*Response, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.initErr != nil || s.store == nil {
		return nil, false
	}
	data, ok, err := s.store.Get(key)
	if err != nil || !ok {
		return nil, false
	}
	var resp Response
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, false
	}
	return &resp, true
}

func (s *FileStore) Set(key string, resp *Response, ttl time.Duration) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.initErr != nil || s.store == nil || resp == nil {
		return
	}
	if len(resp.Body) > maxCachedBodySize {
		// Silently skip persisting oversized bodies (disk-exhaustion guard),
		// matching the pre-kv.FileStore behavior.
		return
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return
	}
	// Any Set error (e.g. kv.ErrTooLarge as a defensive backstop) is
	// swallowed: a cache is best-effort and this Store interface has no
	// error return.
	_ = s.store.Set(key, data, ttl)
}

func (s *FileStore) Delete(key string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.initErr != nil || s.store == nil {
		return
	}
	_ = s.store.Delete(key)
}

// Purge discards every cached entry. kv.Store has no "delete everything"
// operation, and unlike MemoryStore, simply closing and reconstructing a
// kv.FileStore rooted at the same directory would NOT clear it — the fresh
// instance would still see every existing entry file on disk. So Purge here
// closes the store, wipes the directory outright, and reopens a fresh
// kv.FileStore (with the same options) in its place under the write lock.
// Purge is not a hot-path operation, so the extra directory I/O is
// acceptable.
func (s *FileStore) Purge() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initErr != nil || s.store == nil {
		return
	}
	_ = s.store.Close()
	_ = os.RemoveAll(s.dir)
	newStore, err := kv.NewFileStore(s.dir, kv.WithMaxEntrySize(maxCacheEntrySize), kv.WithFileGCInterval(s.gcInterval))
	if err != nil {
		s.initErr = err
		s.store = nil
		return
	}
	s.store = newStore
}

// Len returns the number of cache entries currently on disk.
func (s *FileStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.initErr != nil || s.store == nil {
		return 0
	}
	n, err := s.store.Len()
	if err != nil {
		return 0
	}
	return n
}
