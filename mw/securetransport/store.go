package securetransport

import (
	"errors"
	"sync"
	"time"

	protocol "github.com/oarkflow/fh/pkg/securetransport"
)

var (
	ErrDeviceNotFound  = errors.New("secure transport: device not found")
	ErrDeviceRevoked   = errors.New("secure transport: device revoked")
	ErrSessionNotFound = errors.New("secure transport: session not found")
)

type Device struct {
	ID        protocol.ID16
	PublicKey [32]byte
	Name      string
	Principal string
	CreatedAt time.Time
	LastSeen  time.Time
	RevokedAt time.Time
}

func (d Device) Revoked() bool { return !d.RevokedAt.IsZero() }

type Session struct {
	ID        protocol.ID16
	DeviceID  protocol.ID16
	Principal string
	Keys      protocol.SessionKeys
	CreatedAt time.Time
	ExpiresAt time.Time
	KeyID     string
}

// SessionInfo is the key-free session view exposed to application handlers.
type SessionInfo struct {
	ID        protocol.ID16
	DeviceID  protocol.ID16
	Principal string
	CreatedAt time.Time
	ExpiresAt time.Time
	KeyID     string
}

func publicSession(session Session) SessionInfo {
	return SessionInfo{
		ID: session.ID, DeviceID: session.DeviceID, Principal: session.Principal,
		CreatedAt: session.CreatedAt, ExpiresAt: session.ExpiresAt, KeyID: session.KeyID,
	}
}

type DeviceStore interface {
	Register(device Device) error
	Get(id protocol.ID16) (Device, error)
	Touch(id protocol.ID16, at time.Time) error
	Revoke(id protocol.ID16, at time.Time) error
}

type SessionStore interface {
	Create(session Session) error
	Get(id protocol.ID16) (Session, error)
	Delete(id protocol.ID16) error
	DeleteByDevice(deviceID protocol.ID16) error
}

type ReplayStore interface {
	CheckAndStore(key string, expiresAt time.Time) (accepted bool, err error)
}

type MemoryDeviceStore struct {
	mu sync.RWMutex
	m  map[protocol.ID16]Device
}

func NewMemoryDeviceStore() *MemoryDeviceStore {
	return &MemoryDeviceStore{m: make(map[protocol.ID16]Device)}
}

func (s *MemoryDeviceStore) Register(device Device) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m[device.ID]; exists {
		return errors.New("secure transport: duplicate device id")
	}
	s.m[device.ID] = device
	return nil
}

func (s *MemoryDeviceStore) Get(id protocol.ID16) (Device, error) {
	s.mu.RLock()
	device, ok := s.m[id]
	s.mu.RUnlock()
	if !ok {
		return Device{}, ErrDeviceNotFound
	}
	if device.Revoked() {
		return Device{}, ErrDeviceRevoked
	}
	return device, nil
}

func (s *MemoryDeviceStore) Touch(id protocol.ID16, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	device, ok := s.m[id]
	if !ok {
		return ErrDeviceNotFound
	}
	device.LastSeen = at
	s.m[id] = device
	return nil
}

func (s *MemoryDeviceStore) Revoke(id protocol.ID16, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	device, ok := s.m[id]
	if !ok {
		return ErrDeviceNotFound
	}
	device.RevokedAt = at
	s.m[id] = device
	return nil
}

type MemorySessionStore struct {
	mu sync.RWMutex
	m  map[protocol.ID16]Session
}

func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{m: make(map[protocol.ID16]Session)}
}

func (s *MemorySessionStore) Create(session Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m[session.ID]; exists {
		return errors.New("secure transport: duplicate session id")
	}
	s.m[session.ID] = session
	return nil
}

func (s *MemorySessionStore) Get(id protocol.ID16) (Session, error) {
	now := time.Now()
	s.mu.RLock()
	session, ok := s.m[id]
	s.mu.RUnlock()
	if !ok {
		return Session{}, ErrSessionNotFound
	}
	if !session.ExpiresAt.After(now) {
		s.mu.Lock()
		if current, exists := s.m[id]; exists && !current.ExpiresAt.After(now) {
			zeroSession(&current)
			s.m[id] = current
			delete(s.m, id)
		}
		s.mu.Unlock()
		zeroSession(&session)
		return Session{}, ErrSessionNotFound
	}
	return session, nil
}

func (s *MemorySessionStore) Delete(id protocol.ID16) error {
	s.mu.Lock()
	if session, ok := s.m[id]; ok {
		zeroSession(&session)
		s.m[id] = session
		delete(s.m, id)
	}
	s.mu.Unlock()
	return nil
}

func (s *MemorySessionStore) DeleteByDevice(deviceID protocol.ID16) error {
	s.mu.Lock()
	for id, session := range s.m {
		if protocol.EqualID(session.DeviceID, deviceID) {
			zeroSession(&session)
			s.m[id] = session
			delete(s.m, id)
		}
	}
	s.mu.Unlock()
	return nil
}

func zeroSession(session *Session) {
	if session == nil {
		return
	}
	wipe(session.Keys.ClientToServer[:])
	wipe(session.Keys.ServerToClient[:])
}

type replayEntry struct {
	expiresAt time.Time
}

type MemoryReplayStore struct {
	mu         sync.Mutex
	m          map[string]replayEntry
	maxEntries int
}

func NewMemoryReplayStore(maxEntries int) *MemoryReplayStore {
	if maxEntries <= 0 {
		maxEntries = 250_000
	}
	return &MemoryReplayStore{m: make(map[string]replayEntry), maxEntries: maxEntries}
}

func (s *MemoryReplayStore) CheckAndStore(key string, expiresAt time.Time) (bool, error) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.m) >= s.maxEntries {
		for k, entry := range s.m {
			if !entry.expiresAt.After(now) {
				delete(s.m, k)
			}
		}
	}
	if len(s.m) >= s.maxEntries {
		return false, errors.New("secure transport: replay store capacity exhausted")
	}
	if entry, exists := s.m[key]; exists && entry.expiresAt.After(now) {
		return false, nil
	}
	s.m[key] = replayEntry{expiresAt: expiresAt}
	return true, nil
}
