package ipthrottle

import (
	"sync"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// FileStore is a file-backed Store. Each key's window/count state is
// persisted through a kv.FileStore, which hashes the (caller-supplied,
// unvalidated) key to a filename and writes it atomically, so restarts do
// not reset throttling.
//
// FileStore only wraps kv.FileStore with the fixed-window Increment
// semantics (see incrementViaStore in ipthrottle.go, shared with
// MemoryStore) plus its own background-GC start/stop lifecycle: the GC
// sweep runs against the wrapped kv.FileStore's own exported GC method
// rather than the kv.FileStore's built-in gc-interval option, so that
// StopGC only stops the sweep goroutine (as its name promises) without
// closing the underlying store the way kv.FileStore.Close would.
type FileStore struct {
	store *kv.FileStore

	initErr error

	stopGC   chan struct{}
	stopOnce sync.Once
}

// NewFileStore creates a FileStore rooted at dir. Pass 0 for gcInterval to
// disable automatic background GC.
func NewFileStore(dir string, gcInterval time.Duration) *FileStore {
	kvStore, err := kv.NewFileStore(dir)
	s := &FileStore{store: kvStore, initErr: err}
	if err == nil && gcInterval > 0 {
		s.stopGC = make(chan struct{})
		go s.gcLoop(gcInterval)
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
			s.GC()
		case <-s.stopGC:
			return
		}
	}
}

// GC removes bucket entries whose fixed window has expired. It is called
// automatically on the schedule passed to NewFileStore but may also be
// invoked directly.
func (s *FileStore) GC() {
	if s.store != nil {
		s.store.GC()
	}
}

// Increment implements Store on top of the wrapped kv.FileStore, sharing
// the exact fixed-window algorithm (and the same maxKeys trade-off note)
// that MemoryStore uses.
func (s *FileStore) Increment(key string, window time.Duration, maxKeys int, now time.Time) (int, error) {
	if s.initErr != nil {
		return 0, s.initErr
	}
	return incrementViaStore(s.store, key, window, maxKeys, now)
}
