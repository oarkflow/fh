package ipwhitelist

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// -----------------------------------------------------------------------------
// File-backed whitelist store
// -----------------------------------------------------------------------------

// FileStore is a Store backed by a JSON file containing an array of strings,
// each either a bare IP address or a CIDR range — the same format accepted by
// NewMemoryStore's variadic arguments. It is intended for small,
// operator-managed allow/block lists — not a high-churn per-key store.
//
// The file is loaded once at construction (an error is returned if it is
// missing or malformed). If reloadInterval > 0, a background goroutine polls
// the file and atomically swaps in the newly parsed list whenever it changes
// and parses successfully. If a later read or parse fails, the last
// known-good list keeps serving lookups silently.
type FileStore struct {
	path string

	store atomic.Pointer[MemoryStore]

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewFileStore creates a FileStore backed by the JSON array of IPs/CIDRs at
// path. Pass 0 for reloadInterval to disable polling.
func NewFileStore(path string, reloadInterval time.Duration) (*FileStore, error) {
	s := &FileStore{path: path}

	store, err := loadMemoryStore(path)
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

func loadMemoryStore(path string) (*MemoryStore, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ipwhitelist: failed to read store file %q: %w", path, err)
	}

	var entries []string
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("ipwhitelist: failed to parse store file %q: %w", path, err)
	}

	store, err := NewMemoryStore(entries...)
	if err != nil {
		return nil, fmt.Errorf("ipwhitelist: invalid entry in store file %q: %w", path, err)
	}

	return store, nil
}

func (s *FileStore) watch(interval time.Duration) {
	defer s.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if store, err := loadMemoryStore(s.path); err == nil {
				s.store.Store(store)
			}
			// On error, keep serving the last known-good list.
		case <-s.stop:
			return
		}
	}
}

// Allowed reports whether ip is present in the currently loaded list.
func (s *FileStore) Allowed(ip net.IP) bool {
	return s.store.Load().Allowed(ip)
}

// StopWatch stops the background polling goroutine, if one was started. It
// is safe to call multiple times and safe to call even if reloadInterval was 0.
func (s *FileStore) StopWatch() {
	if s.stop == nil {
		return
	}
	s.stopOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
}
