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
	max       int
	nextSweep time.Time
	Clock     func() time.Time
}

func NewMemoryChallengeStore(max int) *MemoryChallengeStore {
	if max <= 0 {
		max = 100_000
	}
	return &MemoryChallengeStore{items: make(map[string]ChallengeRecord), max: max}
}

func (s *MemoryChallengeStore) Create(id string, record ChallengeRecord, ttl time.Duration) error {
	if id == "" || ttl <= 0 {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.Clock != nil {
		now = s.Clock()
	}
	if !now.Before(s.nextSweep) || len(s.items) >= s.max {
		for key, c := range s.items {
			if !c.Expires.After(now) {
				delete(s.items, key)
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
	record.Value = append([]byte(nil), record.Value...)
	s.items[id] = record
	return nil
}

func (s *MemoryChallengeStore) Take(id string) (ChallengeRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.items[id]
	delete(s.items, id)
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
