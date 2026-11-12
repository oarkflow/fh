// Package deviceauth implements device-bound, challenge-based authentication.
// It is a custom protocol: applications must protect enrollment and recovery,
// use HTTPS, and operate a shared atomic replay guard across server replicas.
package deviceauth

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var (
	ErrInvalid      = errors.New("deviceauth: invalid authentication")
	ErrCapacity     = errors.New("deviceauth: capacity exceeded")
	ErrDeviceExists = errors.New("deviceauth: device already enrolled")
)

const (
	HeaderProof         = "X-Device-Proof"
	HeaderTime          = "X-Device-Time"
	HeaderNonce         = "X-Device-Nonce"
	defaultChallengeTTL = 2 * time.Minute
	defaultSessionTTL   = 15 * time.Minute
)

// DeviceStore holds enrolled public keys. Implementations must be concurrency safe.
type DeviceStore interface {
	Get(deviceID string) (subject string, key ed25519.PublicKey, ok bool, err error)
	// Put must insert only; it must not silently replace an existing device key.
	Put(deviceID, subject string, key ed25519.PublicKey) error
	Delete(deviceID string) error
}

// ReplayGuard atomically admits a proof nonce once across all application nodes.
// A process-local implementation is suitable only for a single node.
type ReplayGuard interface {
	Use(key string, expires time.Time) (bool, error)
}

// ChallengeRecord is short-lived state used to finish one enrollment or login.
type ChallengeRecord struct {
	Value   []byte    `json:"value"`
	Subject string    `json:"subject"`
	Device  string    `json:"device"`
	Enroll  bool      `json:"enroll"`
	Expires time.Time `json:"expires"`
}

// ChallengeStore creates and consumes challenges atomically. Use shared storage
// when requests may be handled by different replicas.
type ChallengeStore interface {
	Create(id string, record ChallengeRecord, ttl time.Duration) error
	Take(id string) (ChallengeRecord, bool, error)
}

type Config struct {
	SigningKey    []byte // server session signing key; at least 32 random bytes
	Devices       DeviceStore
	Replay        ReplayGuard
	Challenges    ChallengeStore
	SessionTTL    time.Duration
	ChallengeTTL  time.Duration
	Clock         func() time.Time
	MaxBodyBytes  int64
	MaxChallenges int
	// CheckDeviceEachRequest trades an online DeviceStore read per request for
	// immediate device deletion semantics. The default uses the bounded signed
	// session lease and applies deletion when the lease expires.
	CheckDeviceEachRequest bool
}

type Manager struct {
	cfg Config
	key []byte
}

func New(cfg Config) (*Manager, error) {
	if len(cfg.SigningKey) < 32 {
		return nil, errors.New("deviceauth: signing key must contain at least 32 bytes")
	}
	if cfg.Devices == nil || cfg.Replay == nil {
		return nil, errors.New("deviceauth: device store and shared replay guard are required")
	}
	if g, ok := cfg.Replay.(*MemoryReplayGuard); ok && g.Clock == nil {
		g.Clock = cfg.Clock
	}
	if g, ok := cfg.Replay.(*KVReplayGuard); ok && g.Clock == nil {
		g.Clock = cfg.Clock
	}
	if cfg.MaxChallenges == 0 {
		cfg.MaxChallenges = 100_000
	}
	if cfg.MaxChallenges < 1 {
		return nil, errors.New("deviceauth: challenge capacity must be positive")
	}
	if cfg.Challenges == nil {
		memoryChallenges := NewMemoryChallengeStore(cfg.MaxChallenges)
		memoryChallenges.Clock = cfg.Clock
		cfg.Challenges = memoryChallenges
	}
	if g, ok := cfg.Challenges.(*KVChallengeStore); ok && g.Clock == nil {
		g.Clock = cfg.Clock
	}
	if cfg.SessionTTL == 0 {
		cfg.SessionTTL = defaultSessionTTL
	}
	if cfg.ChallengeTTL == 0 {
		cfg.ChallengeTTL = defaultChallengeTTL
	}
	if cfg.SessionTTL < time.Minute || cfg.SessionTTL > time.Hour || cfg.ChallengeTTL < 15*time.Second || cfg.ChallengeTTL > 10*time.Minute {
		return nil, errors.New("deviceauth: invalid session or challenge lifetime")
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = 1 << 20
	}
	if cfg.MaxBodyBytes < 0 || cfg.MaxBodyBytes > 16<<20 {
		return nil, errors.New("deviceauth: body limit must be between 1 byte and 16 MiB")
	}
	key := append([]byte(nil), cfg.SigningKey...)
	cfg.SigningKey = nil
	return &Manager{cfg: cfg, key: key}, nil
}

func (m *Manager) now() time.Time {
	if m.cfg.Clock != nil {
		return m.cfg.Clock()
	}
	return time.Now()
}

// BeginEnrollment must be called only after the application has authorized the
// user to add a device (for example, through an existing authenticated flow).
func (m *Manager) BeginEnrollment(subject, deviceID string) (challengeID, challengeValue string, err error) {
	return m.begin(subject, deviceID, true)
}

// BeginLogin starts a device challenge. The caller should apply account/IP rate limits.
func (m *Manager) BeginLogin(deviceID string) (challengeID, challengeValue string, err error) {
	subject, _, ok, e := m.cfg.Devices.Get(deviceID)
	if e != nil || !ok {
		return "", "", ErrInvalid
	}
	return m.begin(subject, deviceID, false)
}

// BeginLoginForUser starts login for an explicit account and device pair. The
// device must already belong to subject. Callers should return generic errors
// for unknown users and devices to avoid account enumeration.
func (m *Manager) BeginLoginForUser(subject, deviceID string) (challengeID, challengeValue string, err error) {
	if strings.TrimSpace(subject) == "" || len(subject) > 256 || strings.TrimSpace(deviceID) == "" || len(deviceID) > 128 {
		return "", "", ErrInvalid
	}
	storedSubject, _, ok, e := m.cfg.Devices.Get(deviceID)
	if e != nil || !ok || storedSubject != subject {
		return "", "", ErrInvalid
	}
	return m.begin(subject, deviceID, false)
}

func (m *Manager) begin(subject, deviceID string, enroll bool) (string, string, error) {
	if strings.TrimSpace(subject) == "" || len(subject) > 256 || strings.TrimSpace(deviceID) == "" || len(deviceID) > 128 {
		return "", "", ErrInvalid
	}
	idRaw := make([]byte, 24)
	val := make([]byte, 32)
	if _, e := rand.Read(idRaw); e != nil {
		return "", "", e
	}
	if _, e := rand.Read(val); e != nil {
		return "", "", e
	}
	id := base64.RawURLEncoding.EncodeToString(idRaw)
	expires := m.now().Add(m.cfg.ChallengeTTL)
	err := m.cfg.Challenges.Create(id, ChallengeRecord{Value: val, Subject: subject, Device: deviceID, Enroll: enroll, Expires: expires}, m.cfg.ChallengeTTL)
	if err != nil {
		return "", "", err
	}
	return id, base64.RawURLEncoding.EncodeToString(val), nil
}

// FinishEnrollment binds a public key after verifying possession of its private key.
// Signature input is the decoded 32-byte challenge; the caller is responsible for
// authenticating the account before starting enrollment.
func (m *Manager) FinishEnrollment(challengeID, signatureB64 string, publicKey ed25519.PublicKey) error {
	if len(publicKey) != ed25519.PublicKeySize || len(challengeID) != 32 || len(signatureB64) != 86 {
		return ErrInvalid
	}
	c, e := m.consumeChallenge(challengeID, true)
	if e != nil {
		return e
	}
	sig, e := base64.RawURLEncoding.DecodeString(signatureB64)
	if e != nil || !ed25519.Verify(publicKey, challengeMessage(c.Value), sig) {
		return ErrInvalid
	}
	return m.cfg.Devices.Put(c.Device, c.Subject, append(ed25519.PublicKey(nil), publicKey...))
}

// FinishLogin verifies the enrolled key's challenge signature and issues a signed session.
func (m *Manager) FinishLogin(challengeID, signatureB64 string) (string, error) {
	if len(challengeID) != 32 || len(signatureB64) != 86 {
		return "", ErrInvalid
	}
	c, e := m.consumeChallenge(challengeID, false)
	if e != nil {
		return "", e
	}
	subject, pub, ok, e := m.cfg.Devices.Get(c.Device)
	if e != nil || !ok || subject != c.Subject {
		return "", ErrInvalid
	}
	sig, e := base64.RawURLEncoding.DecodeString(signatureB64)
	if e != nil || !ed25519.Verify(pub, challengeMessage(c.Value), sig) {
		return "", ErrInvalid
	}
	claims := sessionClaims{Subject: c.Subject, Device: c.Device, Key: base64.RawURLEncoding.EncodeToString(pub), Expires: m.now().Add(m.cfg.SessionTTL).Unix(), Issued: m.now().Unix()}
	b, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(b)
	mac := hmac.New(sha256.New, m.key)
	mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

type sessionClaims struct {
	Subject string `json:"sub"`
	Device  string `json:"dev"`
	Key     string `json:"key"`
	Expires int64  `json:"exp"`
	Issued  int64  `json:"iat"`
}

func (m *Manager) consumeChallenge(id string, enroll bool) (ChallengeRecord, error) {
	if len(id) != 32 {
		return ChallengeRecord{}, ErrInvalid
	}
	c, ok, err := m.cfg.Challenges.Take(id)
	if err != nil {
		return ChallengeRecord{}, err
	}
	if !ok || c.Enroll != enroll || !c.Expires.After(m.now()) {
		return ChallengeRecord{}, ErrInvalid
	}
	return c, nil
}

// Middleware validates a short-lived session and a request-specific Ed25519 proof.
// Proof bytes are base64url(signature) over CanonicalRequest. The request body is
// bounded and restored before the handler. The subject is placed in request context.
func (m *Manager) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, token, err := m.parseSession(r.Header.Get("Authorization"))
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		pubBytes, err := base64.RawURLEncoding.DecodeString(claims.Key)
		if err != nil || len(pubBytes) != ed25519.PublicKeySize {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		pub := ed25519.PublicKey(pubBytes)
		if m.cfg.CheckDeviceEachRequest {
			sub, storedKey, ok, storeErr := m.cfg.Devices.Get(claims.Device)
			if storeErr != nil || !ok || sub != claims.Subject || !hmac.Equal(storedKey, pub) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		var bodyReader io.Reader = http.NoBody
		if r.Body != nil {
			bodyReader = r.Body
		}
		body, err := io.ReadAll(io.LimitReader(bodyReader, m.cfg.MaxBodyBytes+1))
		if err != nil || int64(len(body)) > m.cfg.MaxBodyBytes {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		if r.Body != nil {
			r.Body.Close()
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		ts, err := parseUnix(r.Header.Get(HeaderTime))
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		now := m.now()
		if d := now.Sub(time.Unix(ts, 0)); d > 30*time.Second || d < -30*time.Second {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		nonce := r.Header.Get(HeaderNonce)
		if len(nonce) < 16 || len(nonce) > 128 || strings.ContainsAny(nonce, "\r\n\x00") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		sig, err := base64.RawURLEncoding.DecodeString(r.Header.Get(HeaderProof))
		if err != nil || !ed25519.Verify(pub, []byte(CanonicalRequest(r, body, token, ts, nonce)), sig) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		nonceHash := sha256.Sum256([]byte(claims.Device + ":" + nonce))
		used, err := m.cfg.Replay.Use(hex.EncodeToString(nonceHash[:]), now.Add(time.Minute))
		if err != nil {
			http.Error(w, "authentication temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		if !used {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), subjectContextKey{}, claims.Subject)
		ctx = context.WithValue(ctx, deviceContextKey{}, claims.Device)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type subjectContextKey struct{}
type deviceContextKey struct{}

// Subject returns the authenticated account subject from a request context.
func Subject(ctx context.Context) (string, bool) {
	s, ok := ctx.Value(subjectContextKey{}).(string)
	return s, ok && s != ""
}

// DeviceID returns the enrolled device identifier associated with an
// authenticated request.
func DeviceID(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(deviceContextKey{}).(string)
	return id, ok && id != ""
}

func challengeMessage(value []byte) []byte {
	return append([]byte("FH-DEVICE-AUTH-CHALLENGE-V1\n"), value...)
}

func (m *Manager) parseSession(header string) (sessionClaims, string, error) {
	if !strings.HasPrefix(header, "Device ") || len(header) > 4096 {
		return sessionClaims{}, "", ErrInvalid
	}
	token := strings.TrimPrefix(header, "Device ")
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return sessionClaims{}, "", ErrInvalid
	}
	sig, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return sessionClaims{}, "", ErrInvalid
	}
	mac := hmac.New(sha256.New, m.key)
	mac.Write([]byte(parts[0]))
	if subtle.ConstantTimeCompare(sig, mac.Sum(nil)) != 1 {
		return sessionClaims{}, "", ErrInvalid
	}
	b, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil {
		return sessionClaims{}, "", ErrInvalid
	}
	var c sessionClaims
	if json.Unmarshal(b, &c) != nil || c.Subject == "" || c.Device == "" || len(c.Key) != 43 || c.Expires <= m.now().Unix() || c.Expires-c.Issued > int64(time.Hour.Seconds()) || c.Issued > m.now().Add(time.Minute).Unix() {
		return sessionClaims{}, "", ErrInvalid
	}
	return c, token, nil
}

// CanonicalRequest defines the exact request data signed by clients.
func CanonicalRequest(r *http.Request, body []byte, token string, timestamp int64, nonce string) string {
	h := sha256.Sum256(body)
	return fmt.Sprintf("FH-DEVICE-AUTH-V1\n%s\n%s\n%s\n%s\n%d\n%s\n%s", r.Method, r.URL.RequestURI(), r.Host, hex.EncodeToString(h[:]), timestamp, nonce, token)
}

func parseUnix(s string) (int64, error) {
	if s == "" || len(s) > 12 {
		return 0, ErrInvalid
	}
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, ErrInvalid
		}
		n = n*10 + int64(c-'0')
	}
	return n, nil
}
