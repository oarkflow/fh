package deviceauth

import (
	"encoding/json"
	"sync"
	"time"
)

// MemoryChallengeStore is a bounded process-local challenge store.
type MemoryChallengeStore struct {
	mu        sync.Mutex
	items     map[string]ChallengeRecord
	itemBytes map[string]int64
	max       int
	maxBytes  int64
	usedBytes int64
	nextSweep time.Time
	Clock     func() time.Time
}

func NewMemoryChallengeStore(max int) *MemoryChallengeStore {
	return NewMemoryChallengeStoreWithLimits(max, 0)
}

func NewMemoryChallengeStoreWithLimits(max int, maxBytes int64) *MemoryChallengeStore {
	if max <= 0 {
		max = 100_000
	}
	if maxBytes <= 0 {
		maxBytes = 16 << 20
	}
	return &MemoryChallengeStore{items: make(map[string]ChallengeRecord), itemBytes: make(map[string]int64), max: max, maxBytes: maxBytes}
}

func (s *MemoryChallengeStore) Create(id string, record ChallengeRecord, ttl time.Duration) error {
	if len(id) == 0 || len(id) > 128 || ttl <= 0 || len(record.Subject) == 0 || len(record.Subject) > 256 || !validDeviceID(record.Device) || len(record.Value) > 4096 {
		return ErrInvalid
	}
	size := challengeRecordBytes(id, record)
	if size > s.maxBytes {
		return ErrCapacity
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.Clock != nil {
		now = s.Clock()
	}
	ttlExpiry := now.Add(ttl)
	if record.Expires.IsZero() || record.Expires.After(ttlExpiry) {
		record.Expires = ttlExpiry
	}
	if !record.Expires.After(now) {
		return ErrInvalid
	}
	if !now.Before(s.nextSweep) || len(s.items) >= s.max || s.usedBytes+size > s.maxBytes {
		for key, c := range s.items {
			if !c.Expires.After(now) {
				s.removeLocked(key)
			}
		}
		s.nextSweep = now.Add(5 * time.Second)
	}
	if _, ok := s.items[id]; ok {
		return ErrInvalid
	}
	if len(s.items) >= s.max {
		return ErrCapacity
	}
	if s.usedBytes+size > s.maxBytes {
		return ErrCapacity
	}
	record.Value = append([]byte(nil), record.Value...)
	s.items[id] = record
	s.itemBytes[id] = size
	s.usedBytes += size
	return nil
}

func challengeRecordBytes(id string, record ChallengeRecord) int64 {
	return int64(128 + len(id) + len(record.Value) + len(record.Subject) + len(record.Device))
}

func (s *MemoryChallengeStore) removeLocked(id string) {
	delete(s.items, id)
	s.usedBytes -= s.itemBytes[id]
	delete(s.itemBytes, id)
}

func (s *MemoryChallengeStore) Take(id string) (ChallengeRecord, bool, error) {
	if len(id) == 0 || len(id) > 128 {
		return ChallengeRecord{}, false, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.items[id]
	if ok {
		s.removeLocked(id)
	}
	now := time.Now()
	if s.Clock != nil {
		now = s.Clock()
	}
	if !ok || !c.Expires.After(now) {
		return ChallengeRecord{}, false, nil
	}
	c.Value = append([]byte(nil), c.Value...)
	return c, true, nil
}

// decodeChallengeRecord is shared by the distributed KV adapter.
func decodeChallengeRecord(data []byte) (ChallengeRecord, error) {
	var record ChallengeRecord
	err := json.Unmarshal(data, &record)
	return record, err
}
