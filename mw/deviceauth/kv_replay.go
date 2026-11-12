package deviceauth

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// KVReplayGuard adapts an atomic kv.Store to ReplayGuard. The underlying store
// must be shared by all auth replicas and shard/lock by key. The built-in
// memory and file stores are not distributed merely by using this adapter.
type KVReplayGuard struct {
	Store kv.Store
	Clock func() time.Time
}

// KVChallengeStore stores challenges in a shared atomic kv.Store. Take leaves
// a short-lived tombstone so concurrent consumers cannot both complete a login.
type KVChallengeStore struct {
	Store kv.Store
	Clock func() time.Time
}

func (g *KVChallengeStore) Create(id string, record ChallengeRecord, ttl time.Duration) error {
	if g == nil || g.Store == nil || id == "" || ttl <= 0 {
		return errors.New("deviceauth: challenge store unavailable")
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	created := false
	err = g.Store.Mutate("deviceauth:challenge:"+id, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if exists {
			return nil, 0, false, nil
		}
		created = true
		return data, ttl, true, nil
	})
	if err != nil {
		return err
	}
	if !created {
		return ErrInvalid
	}
	return nil
}

func (g *KVChallengeStore) Take(id string) (ChallengeRecord, bool, error) {
	if g == nil || g.Store == nil || id == "" {
		return ChallengeRecord{}, false, errors.New("deviceauth: challenge store unavailable")
	}
	var record ChallengeRecord
	found := false
	now := time.Now()
	if g.Clock != nil {
		now = g.Clock()
	}
	err := g.Store.Mutate("deviceauth:challenge:"+id, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if !exists || len(current) == 0 || current[0] == 0 {
			return nil, 0, false, nil
		}
		decoded, err := decodeChallengeRecord(current)
		if err != nil {
			return nil, 0, false, err
		}
		record = decoded
		found = record.Expires.After(now)
		ttl := time.Until(record.Expires)
		if g.Clock != nil {
			ttl = record.Expires.Sub(now)
		}
		if ttl <= 0 {
			ttl = time.Second
		}
		return []byte{0}, ttl, true, nil
	})
	return record, found, err
}

func (g *KVReplayGuard) Use(key string, expires time.Time) (bool, error) {
	if g == nil || g.Store == nil || key == "" {
		return false, errors.New("deviceauth: replay store unavailable")
	}
	now := time.Now()
	if g.Clock != nil {
		now = g.Clock()
	}
	ttl := expires.Sub(now)
	if ttl <= 0 {
		return false, ErrInvalid
	}
	accepted := false
	err := g.Store.Mutate("deviceauth:replay:"+key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if exists {
			return nil, 0, false, nil
		}
		accepted = true
		return []byte{1}, ttl, true, nil
	})
	return accepted, err
}
