package session

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

var (
	ErrInvalidSessionSecret = errors.New("fasthttp: session secret must contain at least 32 bytes")
	ErrInvalidSession       = errors.New("fasthttp: invalid session")
)

type Session struct {
	ID        string
	Data      map[string]any
	CreatedAt time.Time
	ExpiresAt time.Time
	mu        sync.RWMutex
	destroyed bool
}

const flashKey = "__fasthttp_flash"

// Flash stores a value for the next request. Called without a value, it
// retrieves and consumes the stored value.
func (s *Session) Flash(key string, value ...any) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Data == nil {
		s.Data = make(map[string]any)
	}
	flashes, _ := s.Data[flashKey].(map[string]any)
	if len(value) > 0 {
		if flashes == nil {
			flashes = make(map[string]any)
			s.Data[flashKey] = flashes
		}
		flashes[key] = value[0]
		return value[0]
	}
	if flashes == nil {
		return nil
	}
	result := flashes[key]
	delete(flashes, key)
	if len(flashes) == 0 {
		delete(s.Data, flashKey)
	}
	return result
}

func (s *Session) Set(key string, value any) {
	s.mu.Lock()
	if s.Data == nil {
		s.Data = make(map[string]any)
	}
	s.Data[key] = value
	s.mu.Unlock()
}

func (s *Session) Get(key string) any {
	s.mu.RLock()
	v := s.Data[key]
	s.mu.RUnlock()
	return v
}

func (s *Session) Delete(key string) {
	s.mu.Lock()
	delete(s.Data, key)
	s.mu.Unlock()
}

func (s *Session) Clear() {
	s.mu.Lock()
	clear(s.Data)
	s.mu.Unlock()
}

func (s *Session) Expired() bool {
	s.mu.RLock()
	expires := s.ExpiresAt
	s.mu.RUnlock()
	if expires.IsZero() {
		return false
	}
	return time.Now().After(expires)
}

func (s *Session) snapshot() *Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := make(map[string]any, len(s.Data))
	for k, v := range s.Data {
		data[k] = v
	}
	return &Session{ID: s.ID, Data: data, CreatedAt: s.CreatedAt, ExpiresAt: s.ExpiresAt, destroyed: s.destroyed}
}

type sessionJSON struct {
	ID        string         `json:"ID"`
	Data      map[string]any `json:"Data"`
	CreatedAt time.Time      `json:"CreatedAt"`
	ExpiresAt time.Time      `json:"ExpiresAt"`
}

func (s *Session) MarshalJSON() ([]byte, error) {
	snapshot := s.snapshot()
	return json.Marshal(sessionJSON{ID: snapshot.ID, Data: snapshot.Data, CreatedAt: snapshot.CreatedAt, ExpiresAt: snapshot.ExpiresAt})
}

func (s *Session) UnmarshalJSON(data []byte) error {
	var decoded sessionJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	s.mu.Lock()
	s.ID, s.Data, s.CreatedAt, s.ExpiresAt = decoded.ID, decoded.Data, decoded.CreatedAt, decoded.ExpiresAt
	s.destroyed = false
	if s.Data == nil {
		s.Data = make(map[string]any)
	}
	s.mu.Unlock()
	return nil
}

// SessionStore is the persistent backend for session data.
type SessionStore interface {
	// Get retrieves a session by ID. Returns nil, nil if not found.
	Get(id string) (*Session, error)
	// Set persists a session.
	Set(session *Session) error
	// Delete removes a session by ID.
	Delete(id string) error
}

// SessionManager handles session lifecycle: create, load, save, destroy.
type SessionManager struct {
	store      SessionStore
	cookieName string
	secrets    [][]byte
	maxAge     time.Duration
	httpOnly   bool
	secure     bool
	sameSite   fh.SameSite
	path       string
	domain     string
	locks      [64]sync.Mutex
}

// SessionOption configures a SessionManager.
type SessionOption func(*SessionManager)

func SessionCookieName(name string) SessionOption {
	return func(m *SessionManager) { m.cookieName = name }
}

func SessionSecret(secret []byte) SessionOption {
	return SessionSecrets(secret)
}

// SessionSecrets configures the active signing key followed by keys accepted
// during rotation. New cookies are always signed by the first key.
func SessionSecrets(secrets ...[]byte) SessionOption {
	return func(m *SessionManager) {
		m.secrets = m.secrets[:0]
		for _, secret := range secrets {
			m.secrets = append(m.secrets, append([]byte(nil), secret...))
		}
	}
}

func SessionMaxAge(d time.Duration) SessionOption {
	return func(m *SessionManager) { m.maxAge = d }
}

func SessionHTTPOnly(v bool) SessionOption {
	return func(m *SessionManager) { m.httpOnly = v }
}

func SessionSecure(v bool) SessionOption {
	return func(m *SessionManager) { m.secure = v }
}

func SessionSameSite(s fh.SameSite) SessionOption {
	return func(m *SessionManager) { m.sameSite = s }
}

func SessionPath(p string) SessionOption {
	return func(m *SessionManager) { m.path = p }
}

func SessionDomain(domain string) SessionOption { return func(m *SessionManager) { m.domain = domain } }

func NewSessionManager(store SessionStore, opts ...SessionOption) *SessionManager {
	if store == nil {
		panic("fasthttp: nil session store")
	}
	m := &SessionManager{
		store:      store,
		cookieName: "session_id",
		maxAge:     7 * 24 * time.Hour,
		httpOnly:   true,
		secure:     true,
		sameSite:   fh.SameSiteLax,
		path:       "/",
	}
	for _, opt := range opts {
		opt(m)
	}
	if len(m.secrets) == 0 {
		b := make([]byte, 32)
		if _, err := io.ReadFull(rand.Reader, b); err != nil {
			panic(fmt.Errorf("fasthttp: session randomness: %w", err))
		}
		m.secrets = [][]byte{b}
	}
	for _, secret := range m.secrets {
		if len(secret) < 32 {
			panic(ErrInvalidSessionSecret)
		}
	}
	if !fh.ValidToken([]byte(m.cookieName)) || m.maxAge < time.Second || m.path == "" || stringsContainsCTL(m.path) {
		panic(ErrInvalidSession)
	}
	probe := &fh.Cookie{Name: m.cookieName, Value: "probe", Path: m.path, Domain: m.domain, Secure: m.secure, HttpOnly: m.httpOnly, SameSite: m.sameSite}
	if err := probe.Valid(); err != nil {
		panic(err)
	}
	return m
}

func generateSessionID() string {
	b := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(fmt.Errorf("fasthttp: session randomness: %w", err))
	}
	return hex.EncodeToString(b)
}

// CookieName returns the configured cookie name.
func (m *SessionManager) CookieName() string { return m.cookieName }

// Store returns the underlying session store.
func (m *SessionManager) Store() SessionStore { return m.store }

// Begin serializes a request against other requests carrying the same session
// token and returns a one-shot completion hook that persists and unlocks it.
// Middleware should register complete with Ctx.OnBeforeResponse.
func (m *SessionManager) Begin(ctx fh.Ctx) (*Session, func(fh.Ctx) error, error) {
	raw := ctx.GetCookie(m.cookieName)
	if raw == "" {
		session := m.NewSession()
		var once sync.Once
		var completeErr error
		return session, func(responseCtx fh.Ctx) error {
			once.Do(func() { completeErr = m.Save(responseCtx, session) })
			return completeErr
		}, nil
	}
	lockKey := raw
	if id, ok := m.verifyToken(raw); ok {
		lockKey = id
	}
	hash := sha256.Sum256([]byte(lockKey))
	lock := &m.locks[int(hash[0])%len(m.locks)]
	lock.Lock()
	session, err := m.Load(ctx)
	if err != nil {
		lock.Unlock()
		return nil, nil, err
	}
	var once sync.Once
	var completeErr error
	complete := func(responseCtx fh.Ctx) error {
		once.Do(func() { completeErr = m.Save(responseCtx, session); lock.Unlock() })
		return completeErr
	}
	return session, complete, nil
}

// NewSession creates a new session with a unique ID and the configured max age.
func (m *SessionManager) NewSession() *Session {
	now := time.Now()
	return &Session{
		ID:        generateSessionID(),
		Data:      make(map[string]any),
		CreatedAt: now,
		ExpiresAt: now.Add(m.maxAge),
	}
}

// Get retrieves the session from the request cookie. Returns a new session
// when the cookie is missing, invalid, or the stored session has expired.
func (m *SessionManager) Get(ctx fh.Ctx) *Session {
	session, err := m.Load(ctx)
	if err != nil {
		return m.NewSession()
	}
	return session
}

// Load retrieves a session while preserving backend errors for production
// middleware and callers that must fail closed.
func (m *SessionManager) Load(ctx fh.Ctx) (*Session, error) {
	raw := ctx.GetCookie(m.cookieName)
	if raw == "" {
		return m.NewSession(), nil
	}
	id, ok := m.verifyToken(raw)
	if !ok || !validSessionID(id) {
		return m.NewSession(), nil
	}

	session, err := m.store.Get(id)
	if err != nil {
		return nil, err
	}
	if session == nil || session.Expired() {
		if session != nil {
			if err := m.store.Delete(session.ID); err != nil {
				return nil, err
			}
		}
		return m.NewSession(), nil
	}
	return session, nil
}

// Save persists the session to the store and sets the session cookie on the response.
func (m *SessionManager) Save(ctx fh.Ctx, session *Session) error {
	if session == nil {
		return ErrInvalidSession
	}
	session.mu.Lock()
	id := session.ID
	if !validSessionID(id) {
		session.mu.Unlock()
		return ErrInvalidSession
	}
	if session.destroyed {
		session.mu.Unlock()
		return nil
	}
	session.ExpiresAt = time.Now().Add(m.maxAge)
	expires := session.ExpiresAt
	session.mu.Unlock()
	if err := m.store.Set(session); err != nil {
		return err
	}
	val := m.signToken(id)
	ctx.SetCookie(&fh.Cookie{
		Name:     m.cookieName,
		Value:    val,
		Path:     m.path,
		Domain:   m.domain,
		MaxAge:   int(m.maxAge.Seconds()),
		Expires:  expires,
		HttpOnly: m.httpOnly,
		Secure:   m.secure,
		SameSite: m.sameSite,
	})
	return nil
}

// Destroy removes the session from the store and clears the session cookie.
func (m *SessionManager) Destroy(ctx fh.Ctx, session *Session) error {
	if session == nil {
		return ErrInvalidSession
	}
	session.mu.RLock()
	id := session.ID
	session.mu.RUnlock()
	if !validSessionID(id) {
		return ErrInvalidSession
	}
	if err := m.store.Delete(id); err != nil {
		return err
	}
	session.mu.Lock()
	session.destroyed = true
	session.mu.Unlock()
	ctx.SetCookie(&fh.Cookie{
		Name:     m.cookieName,
		Value:    "",
		Path:     m.path,
		Domain:   m.domain,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
		HttpOnly: m.httpOnly,
		Secure:   m.secure,
		SameSite: m.sameSite,
	})
	return nil
}

// Regenerate creates a new session ID while preserving the session data.
// Call after login to prevent session fixation.
func (m *SessionManager) Regenerate(ctx fh.Ctx, session *Session) error {
	if session == nil {
		return ErrInvalidSession
	}
	session.mu.Lock()
	oldID, oldCreatedAt := session.ID, session.CreatedAt
	if !validSessionID(oldID) {
		session.mu.Unlock()
		return ErrInvalidSession
	}
	session.destroyed = false
	session.ID = generateSessionID()
	session.CreatedAt = time.Now()
	session.mu.Unlock()
	if err := m.Save(ctx, session); err != nil {
		session.mu.Lock()
		session.ID, session.CreatedAt = oldID, oldCreatedAt
		session.mu.Unlock()
		return err
	}
	return m.store.Delete(oldID)
}

func (m *SessionManager) signToken(id string) string {
	payload := "v1." + id
	mac := hmac.New(sha256.New, m.secrets[0])
	mac.Write([]byte(m.cookieName))
	mac.Write([]byte{0})
	mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (m *SessionManager) verifyToken(token string) (string, bool) {
	last := stringsLastIndexByte(token, '.')
	if last <= 0 {
		return "", false
	}
	payload := token[:last]
	if len(payload) != 67 || payload[:3] != "v1." {
		return "", false
	}
	encodedSig := token[last+1:]
	sig, err := base64.RawURLEncoding.DecodeString(encodedSig)
	if err != nil || len(sig) != sha256.Size {
		return "", false
	}
	if base64.RawURLEncoding.EncodeToString(sig) != encodedSig {
		return "", false
	}
	valid := 0
	for _, secret := range m.secrets {
		mac := hmac.New(sha256.New, secret)
		mac.Write([]byte(m.cookieName))
		mac.Write([]byte{0})
		mac.Write([]byte(payload))
		if hmac.Equal(sig, mac.Sum(nil)) {
			valid = 1
		}
	}
	return payload[3:], valid == 1
}

func validSessionID(id string) bool {
	if len(id) != 64 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func stringsContainsCTL(s string) bool {
	for _, c := range s {
		if c < 0x20 || c == 0x7f || c == ';' {
			return true
		}
	}
	return false
}

// stringsLastIndexByte returns the index of the last occurrence of c in s, or -1.
func stringsLastIndexByte(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}
