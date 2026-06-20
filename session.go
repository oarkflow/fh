package fasthttp

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"sync"
	"time"
)

type Session struct {
	ID        string
	Data      map[string]any
	CreatedAt time.Time
	ExpiresAt time.Time
	mu        sync.RWMutex
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
	if s.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(s.ExpiresAt)
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
	secret     []byte
	maxAge     time.Duration
	httpOnly   bool
	secure     bool
	sameSite   SameSite
	path       string
}

// SessionOption configures a SessionManager.
type SessionOption func(*SessionManager)

func SessionCookieName(name string) SessionOption {
	return func(m *SessionManager) { m.cookieName = name }
}

func SessionSecret(secret []byte) SessionOption {
	return func(m *SessionManager) { m.secret = secret }
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

func SessionSameSite(s SameSite) SessionOption {
	return func(m *SessionManager) { m.sameSite = s }
}

func SessionPath(p string) SessionOption {
	return func(m *SessionManager) { m.path = p }
}

func NewSessionManager(store SessionStore, opts ...SessionOption) *SessionManager {
	m := &SessionManager{
		store:      store,
		cookieName: "session_id",
		maxAge:     7 * 24 * time.Hour,
		httpOnly:   true,
		secure:     true,
		sameSite:   SameSiteLax,
		path:       "/",
	}
	for _, opt := range opts {
		opt(m)
	}
	if len(m.secret) == 0 {
		b := make([]byte, 32)
		rand.Read(b)
		m.secret = b
	}
	return m
}

func generateSessionID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// CookieName returns the configured cookie name.
func (m *SessionManager) CookieName() string { return m.cookieName }

// Store returns the underlying session store.
func (m *SessionManager) Store() SessionStore { return m.store }

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
func (m *SessionManager) Get(ctx *Ctx) *Session {
	raw := ctx.GetCookie(m.cookieName)
	if raw == "" {
		return m.NewSession()
	}

	id := raw
	if len(m.secret) > 0 {
		i := stringsLastIndexByte(raw, '.')
		if i < 0 {
			return m.NewSession()
		}
		sig, err := base64.RawURLEncoding.DecodeString(raw[i+1:])
		if err != nil {
			return m.NewSession()
		}
		mac := hmac.New(sha256.New, m.secret)
		mac.Write([]byte(raw[:i]))
		if !hmac.Equal(sig, mac.Sum(nil)) {
			return m.NewSession()
		}
		id = raw[:i]
	}

	session, err := m.store.Get(id)
	if err != nil || session == nil || session.Expired() {
		if session != nil {
			m.store.Delete(session.ID)
		}
		return m.NewSession()
	}
	return session
}

// Save persists the session to the store and sets the session cookie on the response.
func (m *SessionManager) Save(ctx *Ctx, session *Session) error {
	session.ExpiresAt = time.Now().Add(m.maxAge)
	if err := m.store.Set(session); err != nil {
		return err
	}
	val := session.ID
	if len(m.secret) > 0 {
		mac := hmac.New(sha256.New, m.secret)
		mac.Write([]byte(val))
		sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		val = val + "." + sig
	}
	ctx.SetCookie(&Cookie{
		Name:     m.cookieName,
		Value:    val,
		Path:     m.path,
		MaxAge:   int(m.maxAge.Seconds()),
		HttpOnly: m.httpOnly,
		Secure:   m.secure,
		SameSite: m.sameSite,
	})
	return nil
}

// Destroy removes the session from the store and clears the session cookie.
func (m *SessionManager) Destroy(ctx *Ctx, session *Session) error {
	if err := m.store.Delete(session.ID); err != nil {
		return err
	}
	ctx.SetCookie(&Cookie{
		Name:     m.cookieName,
		Value:    "",
		Path:     m.path,
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
func (m *SessionManager) Regenerate(ctx *Ctx, session *Session) error {
	m.store.Delete(session.ID)
	session.ID = generateSessionID()
	session.CreatedAt = time.Now()
	return m.Save(ctx, session)
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
