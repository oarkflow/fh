package cors

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// -----------------------------------------------------------------------------
// File-backed origin store
// -----------------------------------------------------------------------------

// FileOriginStore is an OriginStore backed by a JSON file containing an array
// of origin patterns (the same pattern syntax accepted by NewMemoryOriginStore:
// exact origins, "*" for a wildcard, and "<scheme>://*.<domain>" subdomain
// wildcards). It is intended for small, operator-managed allow-lists — not a
// high-churn per-key store.
//
// The file is loaded once at construction (an error is returned if it is
// missing or malformed). If reloadInterval > 0, a background goroutine polls
// the file and atomically swaps in the newly compiled list whenever it
// changes and parses successfully. If a later read or parse fails, the last
// known-good list keeps serving lookups silently.
type FileOriginStore struct {
	path string

	store atomic.Pointer[MemoryOriginStore]

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewFileOriginStore creates a FileOriginStore backed by the JSON array of
// origin patterns at path. Pass 0 for reloadInterval to disable polling.
func NewFileOriginStore(path string, reloadInterval time.Duration) (*FileOriginStore, error) {
	s := &FileOriginStore{path: path}

	store, err := loadOriginStore(path)
	if err != nil {
		return nil, err
	}
	s.store.Store(store)

	if reloadInterval > 0 {
		s.stop = make(chan struct{})
		s.wg.Add(1)
		go s.watch(reloadInterval)
	}

	return s, nil
}

func loadOriginStore(path string) (*MemoryOriginStore, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cors: failed to read origin store file %q: %w", path, err)
	}

	var origins []string
	if err := json.Unmarshal(data, &origins); err != nil {
		return nil, fmt.Errorf("cors: failed to parse origin store file %q: %w", path, err)
	}

	return NewMemoryOriginStore(origins...), nil
}

func (s *FileOriginStore) watch(interval time.Duration) {
	defer s.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if store, err := loadOriginStore(s.path); err == nil {
				s.store.Store(store)
			}
			// On error, keep serving the last known-good list.
		case <-s.stop:
			return
		}
	}
}

// Allowed reports whether origin is present in the currently loaded list.
func (s *FileOriginStore) Allowed(origin string) bool {
	return s.store.Load().Allowed(origin)
}

// StopWatch stops the background polling goroutine, if one was started. It
// is safe to call multiple times and safe to call even if reloadInterval was 0.
func (s *FileOriginStore) StopWatch() {
	if s.stop == nil {
		return
	}
	s.stopOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
}
