package ipreputation

import (
	"encoding/json"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// FileStore is a file-backed ReputationStore. Each IP's Score is persisted
// via the shared kv.FileStore primitive (one JSON file per key, named after
// a hash of the key, written atomically), so restarts do not lose reputation
// data and arbitrary (attacker-influenced) IP strings never become
// filesystem paths directly.
//
// kv.Store has no way to enumerate its keys, which the original lowest-
// score-first eviction policy needs. Rather than reimplement kv's directory
// scanning/locking locally to get that enumeration, FileStore keeps a small
// side index of ip -> current Score value (float64 only) purely for eviction
// ranking; the authoritative data for every IP still lives in the kv store.
type FileStore struct {
	store   *kv.FileStore
	maxSize int
	initErr error

	mu       sync.Mutex
	index    map[string]float64 // ip -> current Score.Value, for eviction ranking only
	stopGC   chan struct{}
	stopOnce sync.Once
}

// NewFileStore creates a FileStore rooted at dir. maxEntries optionally
// bounds the number of tracked IPs (default 100000); once at capacity, Set
// and Update evict the lowest-scoring entries first.
func NewFileStore(dir string, maxEntries int) *FileStore {
	if maxEntries <= 0 {
		maxEntries = 100000
	}
	store, err := kv.NewFileStore(dir)
	return &FileStore{
		store:   store,
		maxSize: maxEntries,
		initErr: err,
		index:   make(map[string]float64),
	}
}

// NewFileStoreWithGC creates a FileStore with a background eviction/decay
// loop that runs every gcInterval. Pass 0 for gcInterval to disable it
// (equivalent to NewFileStore). Use StopGC to stop the loop.
//
// This GC removes zero-value scores, which is not something kv.FileStore's
// own TTL-based background sweep can do (Set/Update never assign a TTL,
// since a score is valid until explicitly decayed to zero), so it is driven
// by ipreputation's own ticker, same as before this package sat on kv.Store.
func NewFileStoreWithGC(dir string, maxEntries int, gcInterval time.Duration) *FileStore {
	s := NewFileStore(dir, maxEntries)
	if gcInterval > 0 {
		s.stopGC = make(chan struct{})
		go s.gcLoop(gcInterval)
	}
	return s
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

// StopGC stops the background garbage collection goroutine, if any.
func (s *FileStore) StopGC() {
	if s.stopGC != nil {
		s.stopOnce.Do(func() { close(s.stopGC) })
	}
}

// GC removes zero-value scores from disk. It does not decay values; callers
// wanting decay should periodically call Update with a negative delta, or
// drive an external decay loop like ipreputation.New does for MemoryStore.
func (s *FileStore) GC() {
	if s.initErr != nil || s.store == nil {
		return
	}
	s.mu.Lock()
	ips := make([]string, 0, len(s.index))
	for ip, v := range s.index {
		if v <= 0 {
			ips = append(ips, ip)
		}
	}
	s.mu.Unlock()
	for _, ip := range ips {
		s.mu.Lock()
		delete(s.index, ip)
		s.mu.Unlock()
		_ = s.store.Delete(ip)
	}
}

func (s *FileStore) Get(ip string) (*Score, bool) {
	if s.initErr != nil {
		return nil, false
	}
	data, ok, err := s.store.Get(ip)
	if err != nil || !ok {
		return nil, false
	}
	var sc Score
	if json.Unmarshal(data, &sc) != nil {
		return nil, false
	}
	return &sc, true
}

func (s *FileStore) Set(ip string, score *Score) {
	if score == nil || s.initErr != nil {
		return
	}
	clone := *score
	clone.Reasons = append([]string(nil), score.Reasons...)
	data, err := json.Marshal(&clone)
	if err != nil {
		return
	}
	if err := s.store.Set(ip, data, 0); err != nil {
		return
	}
	s.mu.Lock()
	s.index[ip] = clone.Value
	s.mu.Unlock()
	s.evict()
}

func (s *FileStore) Update(ip string, delta float64, reason string) {
	if s.initErr != nil {
		return
	}
	var newValue float64
	err := s.store.Mutate(ip, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		var sc Score
		if exists {
			if json.Unmarshal(current, &sc) != nil {
				sc = Score{}
			}
		}
		sc.Value = math.Max(0, sc.Value+delta)
		if reason != "" {
			sc.Reasons = append(sc.Reasons, reason)
			if len(sc.Reasons) > 20 {
				sc.Reasons = sc.Reasons[len(sc.Reasons)-20:]
			}
		}
		sc.UpdatedAt = time.Now()
		newValue = sc.Value
		data, err := json.Marshal(&sc)
		if err != nil {
			return nil, 0, false, err
		}
		return data, 0, true, nil
	})
	if err != nil {
		return
	}
	s.mu.Lock()
	s.index[ip] = newValue
	s.mu.Unlock()
	s.evict()
}

// evict mirrors the original FileStore's policy: once over maxSize, sort
// tracked IPs by score value ascending and drop the lowest 10% (at least 1).
func (s *FileStore) evict() {
	s.mu.Lock()
	if len(s.index) <= s.maxSize {
		s.mu.Unlock()
		return
	}
	type entry struct {
		ip    string
		value float64
	}
	entries := make([]entry, 0, len(s.index))
	for ip, v := range s.index {
		entries = append(entries, entry{ip, v})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].value < entries[j].value
	})
	removeCount := len(entries) / 10
	if removeCount < 1 {
		removeCount = 1
	}
	victims := make([]string, 0, removeCount)
	for i := 0; i < removeCount && i < len(entries); i++ {
		victims = append(victims, entries[i].ip)
		delete(s.index, entries[i].ip)
	}
	s.mu.Unlock()
	for _, ip := range victims {
		_ = s.store.Delete(ip)
	}
}
