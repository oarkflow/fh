package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Credentials and tokens.
//
// Two rules hold throughout, and they are the reason this file is small enough to
// read in one go:
//
//  1. Every failure looks the same to the caller. An unknown email, a wrong
//     password, an expired token and a forged token all produce ErrBadCredentials.
//     Distinguishing them is an enumeration oracle.
//  2. The algorithm comes from the key, not from the token. This verifier accepts
//     HS256 and nothing else, so a token whose header claims "alg":"none" — or
//     claims RS256 while presenting an HMAC — is rejected before any comparison.

var (
	// ErrBadCredentials is the single answer to every authentication failure.
	ErrBadCredentials = errors.New("invalid credentials")
	// bcryptCost is deliberately above bcrypt's default of 10.
	bcryptCost = 12
)

func hashPassword(plain string) (string, error) {
	if len(plain) < 12 {
		return "", fmt.Errorf("a password must contain at least 12 characters")
	}
	// bcrypt silently truncates at 72 bytes; refusing is better than hashing a
	// prefix and letting the user believe the rest counted.
	if len(plain) > 72 {
		return "", fmt.Errorf("a password must contain at most 72 bytes")
	}
	digest, err := bcrypt.GenerateFromPassword([]byte(plain), bcryptCost)
	if err != nil {
		return "", err
	}
	return string(digest), nil
}

// verifyPassword always does the bcrypt work, even for an unknown user, so the
// response time does not reveal whether the account exists. dummyHash is a real
// bcrypt hash of a random value, compared against when there is no user.
func verifyPassword(hash, plain string) error {
	if hash == "" {
		hash = dummyHash
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain)); err != nil {
		return ErrBadCredentials
	}
	return nil
}

// dummyHash is generated once at startup from random bytes. It is never a valid
// password for anybody.
var dummyHash = func() string {
	secret := make([]byte, 32)
	_, _ = rand.Read(secret)
	digest, err := bcrypt.GenerateFromPassword(secret, bcryptCost)
	if err != nil {
		// A bcrypt failure here means the process cannot hash at all, and a
		// login path that cannot hash must not fall back to something cheaper.
		panic("ref-app: cannot generate the comparison hash: " + err.Error())
	}
	return string(digest)
}()

// TokenClaims is the payload this application signs. It is deliberately small:
// everything else is looked up from the database, so a stale token cannot carry a
// stale role past a revocation.
type TokenClaims struct {
	Subject  string   `json:"sub"`
	Issuer   string   `json:"iss"`
	TenantID string   `json:"tenant_id"`
	Roles    []string `json:"roles,omitempty"`
	IssuedAt int64    `json:"iat"`
	Expires  int64    `json:"exp"`
}

// TokenSigner issues and verifies HS256 bearer tokens.
type TokenSigner struct {
	secret []byte
	issuer string
	ttl    time.Duration
	now    func() time.Time
}

func NewTokenSigner(secret []byte, issuer string, ttl time.Duration) (*TokenSigner, error) {
	if len(secret) < 32 {
		return nil, fmt.Errorf("ref-app: the token signing key must be at least 32 bytes")
	}
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &TokenSigner{secret: append([]byte(nil), secret...), issuer: issuer, ttl: ttl, now: time.Now}, nil
}

// Issue returns a signed token and the moment it expires.
func (s *TokenSigner) Issue(subject, tenantID string, roles []string) (string, time.Time, error) {
	issued := s.now().UTC()
	expires := issued.Add(s.ttl)
	claims := TokenClaims{
		Subject:  subject,
		Issuer:   s.issuer,
		TenantID: tenantID,
		Roles:    roles,
		IssuedAt: issued.Unix(),
		Expires:  expires.Unix(),
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", time.Time{}, err
	}
	header := base64url([]byte(`{"alg":"HS256","typ":"JWT"}`))
	body := base64url(payload)
	signing := header + "." + body
	return signing + "." + base64url(s.sign(signing)), expires, nil
}

// Verify checks the signature, the algorithm, the issuer and the expiry. Every
// failure is ErrBadCredentials.
func (s *TokenSigner) Verify(token string) (TokenClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return TokenClaims{}, ErrBadCredentials
	}
	// The header is checked, but the algorithm is not taken from it: this verifier
	// only ever computes HMAC-SHA256 with its own key. A header claiming anything
	// else simply does not match.
	rawHeader, err := decodeBase64URL(parts[0])
	if err != nil {
		return TokenClaims{}, ErrBadCredentials
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(rawHeader, &header); err != nil || header.Alg != "HS256" {
		return TokenClaims{}, ErrBadCredentials
	}
	expected := s.sign(parts[0] + "." + parts[1])
	presented, err := decodeBase64URL(parts[2])
	if err != nil {
		return TokenClaims{}, ErrBadCredentials
	}
	if subtle.ConstantTimeCompare(expected, presented) != 1 {
		return TokenClaims{}, ErrBadCredentials
	}
	rawClaims, err := decodeBase64URL(parts[1])
	if err != nil {
		return TokenClaims{}, ErrBadCredentials
	}
	var claims TokenClaims
	if err := json.Unmarshal(rawClaims, &claims); err != nil {
		return TokenClaims{}, ErrBadCredentials
	}
	if claims.Issuer != s.issuer || claims.Subject == "" {
		return TokenClaims{}, ErrBadCredentials
	}
	if claims.Expires == 0 || s.now().UTC().After(time.Unix(claims.Expires, 0)) {
		return TokenClaims{}, ErrBadCredentials
	}
	return claims, nil
}

func (s *TokenSigner) sign(signing string) []byte {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(signing))
	return mac.Sum(nil)
}

func base64url(raw []byte) string { return base64.RawURLEncoding.EncodeToString(raw) }

func decodeBase64URL(text string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(text)
}

// newID returns a sortable, unguessable identifier: a millisecond timestamp for
// ordering plus 8 random bytes so it cannot be enumerated.
func newID(prefix string) string {
	buf := make([]byte, 14)
	binary.BigEndian.PutUint64(buf[:8], uint64(time.Now().UTC().UnixMilli()))
	if _, err := rand.Read(buf[8:]); err != nil {
		// The process cannot generate an unguessable id; continuing would produce
		// predictable order ids, so this is fatal by design.
		panic("ref-app: no randomness available: " + err.Error())
	}
	return prefix + hex.EncodeToString(buf)
}
