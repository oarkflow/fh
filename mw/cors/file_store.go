package cors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxOriginStoreFileBytes = 1 << 20
	maxOriginPatterns       = 10000
	maxOriginPatternBytes   = 4096
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
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cors: failed to read origin store file %q: %w", path, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("cors: failed to stat origin store file %q: %w", path, err)
	}
	if info.Size() > maxOriginStoreFileBytes {
		return nil, fmt.Errorf("cors: origin store file %q exceeds %d bytes", path, maxOriginStoreFileBytes)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxOriginStoreFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("cors: failed to read origin store file %q: %w", path, err)
	}
	if len(data) > maxOriginStoreFileBytes {
		return nil, fmt.Errorf("cors: origin store file %q exceeds %d bytes", path, maxOriginStoreFileBytes)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("cors: failed to parse origin store file %q: %w", path, err)
	}
	if token != json.Delim('[') {
		return nil, fmt.Errorf("cors: origin store file %q must contain a JSON array", path)
	}
	origins := make([]string, 0, 64)
	for decoder.More() {
		if len(origins) >= maxOriginPatterns {
			return nil, fmt.Errorf("cors: origin store file %q exceeds %d patterns", path, maxOriginPatterns)
		}
		var origin string
		if err := decoder.Decode(&origin); err != nil {
			return nil, fmt.Errorf("cors: failed to parse origin store file %q: %w", path, err)
		}
		if len(origin) > maxOriginPatternBytes {
			return nil, fmt.Errorf("cors: origin store file %q contains a pattern exceeding %d bytes", path, maxOriginPatternBytes)
		}
		origins = append(origins, origin)
	}
	endToken, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("cors: failed to parse origin store file %q: %w", path, err)
	}
	if endToken != json.Delim(']') {
		return nil, fmt.Errorf("cors: origin store file %q must contain a JSON array", path)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("cors: origin store file %q contains trailing JSON data", path)
		}
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
	if s == nil {
		return false
	}
	store := s.store.Load()
	return store != nil && store.Allowed(origin)
}

// StopWatch stops the background polling goroutine, if one was started. It
// is safe to call multiple times and safe to call even if reloadInterval was 0.
func (s *FileOriginStore) StopWatch() {
	if s == nil || s.stop == nil {
		return
	}
	s.stopOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
}
