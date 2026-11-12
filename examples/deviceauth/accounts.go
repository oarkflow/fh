package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"strings"
	"sync"
	"time"
)

var errAccountExists = errors.New("account already registered")

type demoAccount struct {
	deviceID     string
	recoveryHash [32]byte
}

type accountStore struct {
	mu    sync.RWMutex
	items map[string]demoAccount
}

func newAccountStore() *accountStore { return &accountStore{items: make(map[string]demoAccount)} }

func (s *accountStore) exists(userID string) bool {
	s.mu.RLock()
	_, ok := s.items[userID]
	s.mu.RUnlock()
	return ok
}

func (s *accountStore) create(userID, deviceID, recoveryCode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[userID]; ok {
		return errAccountExists
	}
	s.items[userID] = demoAccount{deviceID: deviceID, recoveryHash: sha256.Sum256([]byte(recoveryCode))}
	return nil
}

func (s *accountStore) device(userID string) (string, bool) {
	s.mu.RLock()
	account, ok := s.items[userID]
	s.mu.RUnlock()
	return account.deviceID, ok
}

func (s *accountStore) verifyRecovery(userID, recoveryCode string) (string, bool) {
	s.mu.RLock()
	account, ok := s.items[userID]
	s.mu.RUnlock()
	if !ok {
		return "", false
	}
	candidate := sha256.Sum256([]byte(recoveryCode))
	if subtle.ConstantTimeCompare(candidate[:], account.recoveryHash[:]) != 1 {
		return "", false
	}
	return account.deviceID, true
}

func (s *accountStore) replaceDevice(userID, oldDeviceID, newDeviceID, newRecoveryCode string, rotateRecovery bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	account, ok := s.items[userID]
	if !ok || account.deviceID != oldDeviceID {
		return false
	}
	account.deviceID = newDeviceID
	if rotateRecovery {
		account.recoveryHash = sha256.Sum256([]byte(newRecoveryCode))
	}
	s.items[userID] = account
	return true
}

type pendingFlow struct {
	kind, userID, deviceID, oldDeviceID, recoveryCode string
	expires                                           time.Time
}

type pendingStore struct {
	mu    sync.Mutex
	items map[string]pendingFlow
}

type registrationRequest struct {
	userID     string
	email      string
	inviteCode string
	codeHash   [32]byte
	attempts   int
	verified   bool
	expires    time.Time
	approved   bool
}

type registrationRequestStore struct {
	mu         sync.Mutex
	items      map[string]registrationRequest
	ipAttempts map[string][]time.Time
}

func newRegistrationRequestStore() *registrationRequestStore {
	return &registrationRequestStore{items: make(map[string]registrationRequest), ipAttempts: make(map[string][]time.Time)}
}

func verificationCodeHash(requestID, code string) [32]byte {
	return sha256.Sum256([]byte(requestID + ":" + code))
}

func (s *registrationRequestStore) create(id, userID, email, code, remoteIP string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for key, request := range s.items {
		if !request.expires.After(now) {
			delete(s.items, key)
			continue
		}
		if request.userID == userID || strings.EqualFold(request.email, email) {
			return false
		}
	}
	active := s.ipAttempts[remoteIP][:0]
	for _, at := range s.ipAttempts[remoteIP] {
		if now.Sub(at) < time.Hour {
			active = append(active, at)
		}
	}
	if len(active) >= 20 {
		s.ipAttempts[remoteIP] = active
		return false
	}
	active = append(active, now)
	s.ipAttempts[remoteIP] = active
	if len(s.items) >= 10000 {
		return false
	}
	s.items[id] = registrationRequest{userID: userID, email: email, codeHash: verificationCodeHash(id, code), expires: now.Add(10 * time.Minute)}
	return true
}

func (s *registrationRequestStore) verify(id, code string) (registrationRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	request, ok := s.items[id]
	if !ok || !request.expires.After(time.Now()) || request.approved || request.attempts >= 5 {
		return registrationRequest{}, false
	}
	candidate := verificationCodeHash(id, code)
	if subtle.ConstantTimeCompare(candidate[:], request.codeHash[:]) != 1 {
		request.attempts++
		if request.attempts >= 5 {
			request.expires = time.Now()
		}
		s.items[id] = request
		return registrationRequest{}, false
	}
	request.verified = true
	s.items[id] = request
	return request, true
}

func (s *registrationRequestStore) approve(id, inviteCode string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	request, ok := s.items[id]
	if !ok || !request.expires.After(time.Now()) || !request.verified || request.approved {
		return false
	}
	request.inviteCode, request.approved = inviteCode, true
	s.items[id] = request
	return true
}

func (s *registrationRequestStore) status(id string) (registrationRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	request, ok := s.items[id]
	if ok && !request.expires.After(time.Now()) {
		delete(s.items, id)
		return registrationRequest{}, false
	}
	return request, ok
}

func (s *registrationRequestStore) delete(id string) {
	s.mu.Lock()
	delete(s.items, id)
	s.mu.Unlock()
}

func newPendingStore() *pendingStore { return &pendingStore{items: make(map[string]pendingFlow)} }
func (s *pendingStore) put(id string, flow pendingFlow) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for key, value := range s.items {
		if !value.expires.After(now) {
			delete(s.items, key)
		}
	}
	if len(s.items) >= 1000 {
		return false
	}
	s.items[id] = flow
	return true
}
func (s *pendingStore) get(id string) (pendingFlow, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	flow, ok := s.items[id]
	if ok && !flow.expires.After(time.Now()) {
		delete(s.items, id)
		return pendingFlow{}, false
	}
	return flow, ok
}
func (s *pendingStore) delete(id string) { s.mu.Lock(); delete(s.items, id); s.mu.Unlock() }
