package apikey

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

// registryKey is the single fixed key under which the whole API key
// registry is stored in the underlying kv.FileStore. Unlike the per-request
// hot-path stores elsewhere in fh (session, smartcache), this store keeps
// the entire registry as one value rather than one kv entry per key,
// because it's a read-heavy, admin-managed list loaded/reloaded as a whole.
const registryKey = "registry"

// maxRegistryFileSize bounds how large the on-disk key registry may be
// before the underlying kv.FileStore refuses to persist/read it. API key
// registries are small, admin-managed lists, so this is a generous but
// non-infinite cap guarding against a corrupted or malicious file being read
// fully into memory.
const maxRegistryFileSize = 32 << 20 // 32MB

// FileStore is a file-backed implementation of Store that loads the full API
// key registry from a kv.FileStore (rooted at dir, storing the registry as a
// JSON-encoded []KeyRecord under registryKey) at construction time, and
// optionally polls it on an interval so an operator can update the registry
// without restarting the process.
//
// Persisting through kv.FileStore (instead of raw os.ReadFile/os.WriteFile
// against an arbitrary path, as a prior version of this store did) gives
// SaveRegistry atomic-write safety for free. The tradeoff is that the
// registry is no longer a hand-editable plain JSON file at a well-known
// path: on-disk it is one hashed-filename entry inside dir, wrapped in
// kv's own entry envelope. Operators managing the registry programmatically
// should do so through SaveRegistry (or by driving a kv.FileStore rooted at
// the same dir directly), not by hand-editing files in dir.
type FileStore struct {
	store     *kv.FileStore
	interval  time.Duration
	snapshot  atomic.Pointer[map[string]KeyRecord]
	stopWatch chan struct{}
	stopOnce  sync.Once
}

// NewFileStore opens (creating if necessary) a kv.FileStore rooted at dir and
// loads the key registry stored there under registryKey. If reloadInterval
// is greater than 0, a background goroutine polls that key on that interval
// and atomically swaps in the new snapshot when it parses successfully.
//
// If the registry is missing or malformed at construction, an error is
// returned. If it becomes malformed during a later reload poll, the
// last-good snapshot keeps serving lookups and the bad reload is skipped.
func NewFileStore(dir string, reloadInterval time.Duration) (*FileStore, error) {
	store, err := kv.NewFileStore(dir, kv.WithMaxEntrySize(maxRegistryFileSize))
	if err != nil {
		return nil, err
	}
	s := &FileStore{store: store, interval: reloadInterval}
	records, err := s.loadRecords()
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	s.snapshot.Store(recordsToMap(records))
	if reloadInterval > 0 {
		s.stopWatch = make(chan struct{})
		go s.watchLoop(reloadInterval)
	}
	return s, nil
}

// StopWatch stops the background reload-polling goroutine and releases the
// underlying kv.FileStore. There is no per-record expiry in this store (just
// a periodic config reload), so this mirrors the StopGC pattern used by the
// other file-backed stores under a name that reflects what it actually
// stops.
func (s *FileStore) StopWatch() {
	if s.stopWatch != nil {
		s.stopOnce.Do(func() { close(s.stopWatch) })
	}
	_ = s.store.Close()
}

func (s *FileStore) watchLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.reload()
		case <-s.stopWatch:
			return
		}
	}
}

// reload attempts to re-read and re-parse the registry. On any error
// (missing entry, oversized entry, malformed JSON) it silently keeps serving
// the last-good snapshot.
func (s *FileStore) reload() {
	records, err := s.loadRecords()
	if err != nil {
		return
	}
	s.snapshot.Store(recordsToMap(records))
}

func (s *FileStore) loadRecords() ([]KeyRecord, error) {
	data, ok, err := s.store.Get(registryKey)
	if err != nil {
		return nil, fmt.Errorf("apikey: reading key registry: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("apikey: no key registry found")
	}
	var records []KeyRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("apikey: parsing key registry: %w", err)
	}
	return records, nil
}

// SaveRegistry persists records as the whole registry, atomically (via
// kv.FileStore.Set), and updates the in-memory snapshot immediately so a
// subsequent Lookup observes the change without waiting for the next reload
// poll. This is the intended way to manage the registry programmatically
// now that persistence goes through kv.FileStore instead of a hand-editable
// JSON file.
func (s *FileStore) SaveRegistry(records []KeyRecord) error {
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	if err := s.store.Set(registryKey, data, 0); err != nil {
		return err
	}
	s.snapshot.Store(recordsToMap(records))
	return nil
}

func recordsToMap(records []KeyRecord) *map[string]KeyRecord {
	m := make(map[string]KeyRecord, len(records))
	for _, r := range records {
		if r.ID == "" {
			continue
		}
		m[r.ID] = r
	}
	return &m
}

func (s *FileStore) Lookup(ctx fh.Ctx, id string) (KeyRecord, bool, error) {
	if s == nil {
		return KeyRecord{}, false, nil
	}
	m := s.snapshot.Load()
	if m == nil {
		return KeyRecord{}, false, nil
	}
	rec, ok := (*m)[id]
	return rec, ok, nil
}
