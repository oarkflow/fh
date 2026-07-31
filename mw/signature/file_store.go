package signature

import (
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// FileStore is a file-backed ReplayStore. Each seen key is persisted by the
// shared kv.FileStore primitive (one JSON file per key, named after a hash of
// the key, written atomically), so restarts do not forget replayed
// signatures and arbitrary key bytes never touch the filesystem path
// directly.
//
// ReplayStore.Seen has no error return, so any I/O failure here fails
// closed: the key is treated as already seen (i.e. rejected as a replay)
// rather than silently allowed through.
type FileStore struct {
	kv      *kv.FileStore
	initErr error
}

// NewFileStore creates a FileStore rooted at dir. Pass 0 for gcInterval to
// disable automatic background GC.
func NewFileStore(dir string, gcInterval time.Duration) *FileStore {
	var opts []kv.FileOption
	if gcInterval > 0 {
		opts = append(opts, kv.WithFileGCInterval(gcInterval))
	}
	store, err := kv.NewFileStore(dir, opts...)
	return &FileStore{kv: store, initErr: err}
}

// StopGC stops the background garbage collection goroutine and releases the
// underlying store's resources. Safe to call multiple times.
func (s *FileStore) StopGC() {
	if s.kv != nil {
		_ = s.kv.Close()
	}
}

// GC removes all expired marker files.
func (s *FileStore) GC() {
	if s.kv != nil {
		s.kv.GC()
	}
}

// Seen implements ReplayStore. It returns true if key was already recorded
// and has not yet expired, or if any underlying store error occurs while
// checking or recording (fail safe: treat as a replay rather than risk
// silently allowing one through). Otherwise it records key with the given
// ttl and returns false.
func (s *FileStore) Seen(key string, ttl time.Duration) bool {
	if s.initErr != nil {
		return true
	}

	var seen bool
	err := s.kv.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if exists {
			seen = true
			return nil, 0, false, nil
		}
		seen = false
		return []byte{1}, ttl, true, nil
	})
	if err != nil {
		return true
	}
	return seen
}
