package gateway

// Local JWT revocation snapshots. This is designed for incident response
// and works without accepting any token-provided key URLs or network calls.
// It is NOT a replacement for IdP introspection when immediate global revocation
// across all gateway nodes is required.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

var ErrRevoked = errors.New("gateway: token revoked or revocation state unavailable")

type RevocationFile struct {
	RevokedSubjects    []string         `json:"revoked_subjects"`
	RevokedJTI         []string         `json:"revoked_jti"`
	RevokedTokenSHA256 []string         `json:"revoked_token_sha256"`
	SubjectNotBefore   map[string]int64 `json:"subject_not_before"`
}

type revocations struct {
	at        time.Time
	subjects  map[string]struct{}
	jtIs      map[string]struct{}
	hashes    map[[32]byte]struct{}
	notBefore map[string]int64
}

// RevocationStore atomically replaces the full incident-response list. It
// fails closed once the last valid snapshot is too old (e.g. failed reloads).
type RevocationStore struct {
	current atomic.Pointer[revocations]
	MaxAge  time.Duration
	Clock   func() time.Time
}

func (r *RevocationStore) now() time.Time {
	if r.Clock != nil {
		return r.Clock()
	}
	return time.Now()
}

func (r *RevocationStore) Reload(path string) error {
	if r == nil || path == "" {
		return ErrRevoked
	}
	b, err := os.ReadFile(path)
	if err != nil || len(b) > 256<<10 {
		return ErrRevoked
	}
	var doc RevocationFile
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if dec.Decode(&doc) != nil {
		return ErrRevoked
	}
	var trailing any
	if dec.Decode(&trailing) != io.EOF {
		return ErrRevoked
	}
	if len(doc.RevokedSubjects)+len(doc.RevokedJTI)+len(doc.RevokedTokenSHA256)+len(doc.SubjectNotBefore) > 10000 {
		return ErrRevoked
	}
	snap := &revocations{at: r.now(), subjects: make(map[string]struct{}), jtIs: make(map[string]struct{}), hashes: make(map[[32]byte]struct{}), notBefore: make(map[string]int64)}
	for _, s := range doc.RevokedSubjects {
		if len(s) == 0 || len(s) > 256 {
			return ErrRevoked
		}
		snap.subjects[s] = struct{}{}
	}
	for _, s := range doc.RevokedJTI {
		if len(s) == 0 || len(s) > 256 {
			return ErrRevoked
		}
		snap.jtIs[s] = struct{}{}
	}
	for _, s := range doc.RevokedTokenSHA256 {
		if len(s) != 64 {
			return ErrRevoked
		}
		b, err := hex.DecodeString(s)
		if err != nil {
			return ErrRevoked
		}
		var h [32]byte
		copy(h[:], b)
		snap.hashes[h] = struct{}{}
	}
	for s, ts := range doc.SubjectNotBefore {
		if len(s) == 0 || len(s) > 256 || ts <= 0 {
			return ErrRevoked
		}
		snap.notBefore[s] = ts
	}
	r.current.Store(snap)
	return nil
}

// Check accepts only a previously verified JWT. Always authenticate first;
// payload parsing here is deliberately not an independent JWT verifier.
func (r *RevocationStore) Check(req *http.Request, subject string) error {
	if r == nil {
		return ErrRevoked
	}
	snap := r.current.Load()
	maxAge := r.MaxAge
	if maxAge == 0 {
		maxAge = 24 * time.Hour
	}
	now := r.now()
	if maxAge < time.Second || maxAge > 7*24*time.Hour || snap == nil || snap.at.After(now) || now.Sub(snap.at) > maxAge {
		return ErrRevoked
	}
	if _, ok := snap.subjects[subject]; ok {
		return ErrRevoked
	}
	token := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
	if len(token) > 8192 || token == "" {
		return ErrRevoked
	}
	if _, ok := snap.hashes[sha256.Sum256([]byte(token))]; ok {
		return ErrRevoked
	}
	pieces := strings.Split(token, ".")
	if len(pieces) != 3 {
		return ErrRevoked
	}
	raw, err := base64.RawURLEncoding.DecodeString(pieces[1])
	if err != nil || len(raw) > 8192 {
		return ErrRevoked
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var claims map[string]any
	if dec.Decode(&claims) != nil {
		return ErrRevoked
	}
	if id, ok := claims["jti"].(string); ok {
		if _, seen := snap.jtIs[id]; seen {
			return ErrRevoked
		}
	}
	if ts, ok := snap.notBefore[subject]; ok {
		iat, valid := claimTime(claims, "iat", true)
		if !valid || iat < ts {
			return ErrRevoked
		}
	}
	return nil
}

func (r *RevocationStore) Wrap(base Authenticate) Authenticate {
	return func(req *http.Request) (string, error) {
		if base == nil {
			return "", ErrRevoked
		}
		subject, err := base(req)
		if err != nil {
			return "", err
		}
		if err = r.Check(req, subject); err != nil {
			return "", err
		}
		return subject, nil
	}
}
