package timestamp

import (
	"sync"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// FileStore is a file-backed ReplayStore that persists seen replay keys
// across restarts. It is a thin adapter over kv.FileStore, which already
// handles key hashing (so arbitrary, unvalidated keys derived from
// client-controlled headers never touch the filesystem directly), atomic
// writes, and expiry bookkeeping.
type FileStore struct {
	store    *kv.FileStore
	initErr  error
	stopGC   chan struct{}
	stopOnce sync.Once
}

// NewFileStore creates a FileStore rooted at dir, creating it if necessary.
// Pass 0 for gcInterval to disable automatic background garbage collection
// of expired markers; otherwise a goroutine calls GC on that interval until
// StopGC is called.
func NewFileStore(dir string, gcInterval time.Duration) *FileStore {
	store, err := kv.NewFileStore(dir)
	s := &FileStore{store: store, initErr: err}
	if gcInterval > 0 {
		s.stopGC = make(chan struct{})
		go s.gcLoop(gcInterval)
	}
	return s
}

// StopGC stops the background garbage collection goroutine, if running.
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

// Seen records key on first sight and returns true if key was already seen
// and has not yet expired, matching MemoryStore's semantics. The check and
// the (possible) write happen atomically under kv.FileStore.Mutate.
func (s *FileStore) Seen(key string, ttl time.Duration) (bool, error) {
	if s.initErr != nil {
		return false, s.initErr
	}
	var seen bool
	err := s.store.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if exists {
			seen = true
			return nil, 0, false, nil
		}
		seen = false
		return seenMarker, ttl, true, nil
	})
	if err != nil {
		return false, err
	}
	return seen, nil
}

// GC removes all expired replay marker files.
func (s *FileStore) GC() {
	if s.store != nil {
		s.store.GC()
	}
}
