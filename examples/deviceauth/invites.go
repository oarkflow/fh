package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"sync"
	"time"
)

type registrationInvite struct {
	userID  string
	expires time.Time
}

type inviteStore struct {
	mu    sync.Mutex
	items map[string]registrationInvite
}

func newInviteStore() *inviteStore { return &inviteStore{items: make(map[string]registrationInvite)} }

func (s *inviteStore) create(userID string, ttl time.Duration) (string, time.Time, bool) {
	codeBytes := make([]byte, 32)
	if _, err := rand.Read(codeBytes); err != nil {
		return "", time.Time{}, false
	}
	code := base64.RawURLEncoding.EncodeToString(codeBytes)
	expires := time.Now().Add(ttl)
	hash := sha256.Sum256([]byte(code))
	key := hex.EncodeToString(hash[:])

	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for stored, invite := range s.items {
		if !invite.expires.After(now) {
			delete(s.items, stored)
		}
	}
	if len(s.items) >= 10_000 {
		return "", time.Time{}, false
	}
	s.items[key] = registrationInvite{userID: userID, expires: expires}
	return code, expires, true
}

func (s *inviteStore) consume(userID, code string) bool {
	hash := sha256.Sum256([]byte(code))
	key := hex.EncodeToString(hash[:])
	s.mu.Lock()
	defer s.mu.Unlock()
	invite, ok := s.items[key]
	if !ok || !invite.expires.After(time.Now()) || invite.userID != userID {
		return false
	}
	delete(s.items, key)
	return true
}
