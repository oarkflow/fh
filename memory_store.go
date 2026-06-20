package fasthttp

import (
	"sync"
	"time"
)

// MemoryStore is an in-memory session store protected by an RWMutex.
type MemoryStore struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	stopGC   chan struct{}
}

// NewMemoryStore creates a MemoryStore with an optional GC interval.
// Pass 0 to disable automatic GC.
func NewMemoryStore(gcInterval time.Duration) *MemoryStore {
	s := &MemoryStore{
		sessions: make(map[string]*Session),
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
		close(s.stopGC)
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
		if !session.ExpiresAt.IsZero() && now.After(session.ExpiresAt) {
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
	return session, nil
}

func (s *MemoryStore) Set(session *Session) error {
	s.mu.Lock()
	s.sessions[session.ID] = session
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Delete(id string) error {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
	return nil
}
