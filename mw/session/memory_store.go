package session

import (
	"sync"
	"time"
)

// defaultMaxSessions bounds MemoryStore when no explicit limit is given.
// Without a bound, an attacker who can trigger session creation (any
// unauthenticated request reaching Begin) can flood the store with
// sessions up to MaxAge before GC reclaims them — unbounded memory growth.
const defaultMaxSessions = 100000

// MemoryStore is an in-memory session store protected by an RWMutex.
type MemoryStore struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	stopGC   chan struct{}
	stopOnce sync.Once
	maxSize  int
}

// NewMemoryStore creates a MemoryStore with an optional GC interval.
// Pass 0 to disable automatic GC. maxSessions optionally bounds the number
// of concurrently tracked sessions (default 100000); once at capacity, a
// new session eagerly reclaims expired entries first, then evicts one
// arbitrary entry rather than growing further.
func NewMemoryStore(gcInterval time.Duration, maxSessions ...int) *MemoryStore {
	max := defaultMaxSessions
	if len(maxSessions) > 0 && maxSessions[0] > 0 {
		max = maxSessions[0]
	}
	s := &MemoryStore{
		sessions: make(map[string]*Session),
		maxSize:  max,
	}
	if gcInterval > 0 {
		s.stopGC = make(chan struct{})
		go s.gcLoop(gcInterval)
	}
	return s
}

// StopGC stops the background garbage collection goroutine.
func (s *MemoryStore) StopGC() {
	if s.stopGC != nil {
		s.stopOnce.Do(func() { close(s.stopGC) })
	}
}

func (s *MemoryStore) gcLoop(interval time.Duration) {
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

// GC removes all expired sessions.
func (s *MemoryStore) GC() {
	now := time.Now()
	s.mu.Lock()
	for id, session := range s.sessions {
		session.mu.RLock()
		expires := session.ExpiresAt
		session.mu.RUnlock()
		if !expires.IsZero() && now.After(expires) {
			delete(s.sessions, id)
		}
	}
	s.mu.Unlock()
}

func (s *MemoryStore) Get(id string) (*Session, error) {
	s.mu.RLock()
	session, ok := s.sessions[id]
	s.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	return session.snapshot(), nil
}

func (s *MemoryStore) Set(session *Session) error {
	if session == nil || !validSessionID(session.ID) {
		return ErrInvalidSession
	}
	snapshot := session.snapshot()
	s.mu.Lock()
	if _, exists := s.sessions[snapshot.ID]; !exists && s.maxSize > 0 && len(s.sessions) >= s.maxSize {
		s.evictLocked()
	}
	s.sessions[snapshot.ID] = snapshot
	s.mu.Unlock()
	return nil
}

// evictLocked reclaims space for a new session; caller holds s.mu. It first
// removes any already-expired session, and only falls back to dropping an
// arbitrary entry if nothing was reclaimable.
func (s *MemoryStore) evictLocked() {
	now := time.Now()
	for id, session := range s.sessions {
		session.mu.RLock()
		expired := !session.ExpiresAt.IsZero() && now.After(session.ExpiresAt)
		session.mu.RUnlock()
		if expired {
			delete(s.sessions, id)
			return
		}
	}
	for id := range s.sessions {
		delete(s.sessions, id)
		return
	}
}

func (s *MemoryStore) Delete(id string) error {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
	return nil
}
