package httpsignature

import (
	"sync"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// FileStore is a file-backed NonceStore that persists accepted response
// signature nonces across restarts. It is a thin adapter over kv.FileStore,
// which already handles key hashing (so arbitrary, unvalidated keys never
// touch the filesystem directly), atomic writes, and expiry bookkeeping.
//
// CheckAndStore takes an absolute expiresAt, which is converted to a ttl for
// kv.FileStore's Mutate. kv.Store has no notion of "already expired" TTL
// (ttl<=0 means "never expires" in its Set/Mutate contract) so, matching
// MemoryNonceStore, an expiresAt already in the past is not written at all:
// the original implementation would have written a marker file that is
// indistinguishable from absent (any subsequent read sees it as expired),
// so skipping the write has the same net effect without abusing kv's
// no-expiry sentinel.
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

// CheckAndStore atomically checks whether key is already recorded and not
// yet expired; if so it returns accepted=false without modifying storage.
// Otherwise it records key with the given expiry and returns accepted=true,
// matching MemoryNonceStore's semantics.
func (s *FileStore) CheckAndStore(key string, expiresAt time.Time) (bool, error) {
	if s.initErr != nil {
		return false, s.initErr
	}
	ttl := time.Until(expiresAt)
	var accepted bool
	err := s.store.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if exists {
			accepted = false
			return nil, 0, false, nil
		}
		if ttl <= 0 {
			accepted = true
			return nil, 0, false, nil
		}
		accepted = true
		return nonceMarker, ttl, true, nil
	})
	if err != nil {
		return false, err
	}
	return accepted, nil
}

// GC removes all expired nonce marker files.
func (s *FileStore) GC() {
	if s.store != nil {
		s.store.GC()
	}
}
