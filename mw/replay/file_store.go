package replay

import (
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// FileStore is a file-backed replay Store. Each seen key is persisted by the
// shared kv.FileStore primitive (one JSON file per key, named after a hash of
// the key, written atomically), so restarts do not forget replayed keys and
// arbitrary key bytes never touch the filesystem path directly.
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

// Seen implements Store. It returns true if key was already recorded and has
// not yet expired; otherwise it records key with the given ttl and returns
// false. Matches MemoryStore.Seen semantics: on first sight (or after
// expiry) the key is (re)recorded and false is returned.
func (s *FileStore) Seen(key string, ttl time.Duration) (bool, error) {
	if s.initErr != nil {
		return false, s.initErr
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
		return false, err
	}
	return seen, nil
}
