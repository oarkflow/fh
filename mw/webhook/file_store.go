package webhook

import (
	"sync"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// FileStore is a file-backed ReplayStore that persists seen webhook
// signatures across restarts. It is a thin adapter over kv.FileStore, which
// already handles key hashing (so keys derived from attacker-controlled
// request headers never touch the filesystem directly), atomic writes, and
// expiry bookkeeping.
//
// Unlike MemoryStore, Seen has no error return to satisfy. On any file I/O
// failure (including a failed store initialization) the key is treated as
// already seen (Seen returns true), so a storage failure fails closed as a
// replay rejection rather than silently letting a forged or replayed
// webhook through.
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

// Seen reports whether key has already been recorded and not yet expired.
// If not, it records key with the given ttl and returns false. Any file
// I/O error (including a corrupt or oversized marker file) is treated as
// fail-safe "seen" so callers reject rather than accept.
func (s *FileStore) Seen(key string, ttl time.Duration) bool {
	if s.initErr != nil {
		return true
	}
	var seen bool
	err := s.store.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if exists {
			seen = true
			return nil, 0, false, nil
		}
		seen = false
		return replayMarker, ttl, true, nil
	})
	if err != nil {
		return true
	}
	return seen
}

// GC removes all expired replay marker files.
func (s *FileStore) GC() {
	if s.store != nil {
		s.store.GC()
	}
}
