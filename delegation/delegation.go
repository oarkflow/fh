// Package delegation binds a browser-gateway principal to a single encrypted
// MIRA channel using a signed, short-lived service assertion. The backend must
// verify this BEFORE interpreting any application payload on gateway channels.
package delegation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh/protocol"
)

var ErrInvalid = errors.New("delegation: invalid or replayed identity assertion")

const tag = "MIRA-T/1 verified gateway delegation v1"
const acknowledgement = "MIRA-GATEWAY-PRINCIPAL-ACCEPTED-v1"
const maxProof = 2048

type Proof struct {
	Version   uint8  `json:"version"`
	Audience  string `json:"audience"`
	Subject   string `json:"subject"`
	Session   uint64 `json:"session"`
	Channel   uint64 `json:"channel"`
	Issued    int64  `json:"issued"`
	Expires   int64  `json:"expires"`
	Nonce     string `json:"nonce"`
	Signature string `json:"signature"`
	KeyID     string `json:"kid,omitempty"`
}

func appendString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}
func signedBytes(p Proof, nonce []byte) []byte {
	domain := tag
	if p.Version == 2 {
		domain = "MIRA-T/1 verified gateway delegation v2"
	}
	b := appendString(nil, domain)
	b = append(b, p.Version)
	if p.Version == 2 {
		b = appendString(b, p.KeyID)
	}
	b = appendString(b, p.Audience)
	b = appendString(b, p.Subject)
	b = binary.BigEndian.AppendUint64(b, p.Session)
	b = binary.BigEndian.AppendUint64(b, p.Channel)
	b = binary.BigEndian.AppendUint64(b, uint64(p.Issued))
	b = binary.BigEndian.AppendUint64(b, uint64(p.Expires))
	return append(b, nonce...)
}
func validIdentity(s string) bool {
	if len(s) == 0 || len(s) > 256 || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}
func Issue(priv ed25519.PrivateKey, audience, subject string, sid, cid uint64, now time.Time) ([]byte, error) {
	return issue(priv, "", audience, subject, sid, cid, now)
}

// IssueNamed signs v2 assertions with an explicit, cryptographically bound key ID.
func IssueNamed(priv ed25519.PrivateKey, keyID, audience, subject string, sid, cid uint64, now time.Time) ([]byte, error) {
	if !validKeyID(keyID) {
		return nil, ErrInvalid
	}
	return issue(priv, keyID, audience, subject, sid, cid, now)
}

func issue(priv ed25519.PrivateKey, keyID, audience, subject string, sid, cid uint64, now time.Time) ([]byte, error) {
	if len(priv) != ed25519.PrivateKeySize || !validIdentity(subject) || !validIdentity(audience) || sid == 0 || cid == 0 {
		return nil, ErrInvalid
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	p := Proof{Version: 1, Audience: audience, Subject: subject, Session: sid, Channel: cid, Issued: now.Unix(), Expires: now.Add(25 * time.Second).Unix(), Nonce: base64.RawURLEncoding.EncodeToString(random[:])}
	if keyID != "" {
		p.Version = 2
		p.KeyID = keyID
	}
	p.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, signedBytes(p, random[:])))
	b, err := json.Marshal(p)
	if len(b) > maxProof {
		return nil, ErrInvalid
	}
	return b, err
}

type Verifier struct {
	Audience  string
	PublicKey ed25519.PublicKey
	keyring   atomic.Pointer[publicRing]
	Clock     func() time.Time
	mu        sync.Mutex
	used      map[[32]byte]time.Time
}

func (v *Verifier) Verify(payload []byte, sid, cid uint64) (string, error) {
	if v == nil || !validIdentity(v.Audience) || len(payload) == 0 || len(payload) > maxProof || sid == 0 || cid == 0 {
		return "", ErrInvalid
	}
	var p Proof
	if json.Unmarshal(payload, &p) != nil || (p.Version != 1 && p.Version != 2) || p.Audience != v.Audience || !validIdentity(p.Subject) || p.Session != sid || p.Channel != cid {
		return "", ErrInvalid
	}
	nonce, err := base64.RawURLEncoding.DecodeString(p.Nonce)
	if err != nil || len(nonce) != 16 {
		return "", ErrInvalid
	}
	var key ed25519.PublicKey
	if p.Version == 2 && validKeyID(p.KeyID) {
		if ring := v.keyring.Load(); ring != nil {
			key = ring.keys[p.KeyID]
		}
	} else if p.Version == 1 && p.KeyID == "" {
		key = v.PublicKey
	}
	sig, err := base64.RawURLEncoding.DecodeString(p.Signature)
	if err != nil || len(sig) != ed25519.SignatureSize || len(key) != ed25519.PublicKeySize || !ed25519.Verify(key, signedBytes(p, nonce), sig) {
		return "", ErrInvalid
	}
	now := time.Now()
	if v.Clock != nil {
		now = v.Clock()
	}
	if p.Expires <= p.Issued || p.Expires-p.Issued > 30 || now.Unix() < p.Issued-5 || now.Unix() >= p.Expires {
		return "", ErrInvalid
	}
	hash := sha256.Sum256(signedBytes(p, nonce))
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.used == nil {
		v.used = make(map[[32]byte]time.Time)
	}
	if _, seen := v.used[hash]; seen {
		return "", ErrInvalid
	}
	if len(v.used) >= 4096 {
		for key, expiry := range v.used {
			if !expiry.After(now) {
				delete(v.used, key)
			}
		}
		if len(v.used) >= 4096 {
			return "", ErrInvalid
		}
	}
	v.used[hash] = time.Unix(p.Expires, 0)
	return p.Subject, nil
}

// ProveNamed is the key-identified variant of Prove used by rotating gateway keys.
func ProveNamed(ctx context.Context, c protocol.Channel, sid uint64, priv ed25519.PrivateKey, kid, audience, subject string) error {
	if c == nil {
		return ErrInvalid
	}
	b, err := IssueNamed(priv, kid, audience, subject, sid, c.ID(), time.Now())
	if err != nil {
		return err
	}
	if _, err = c.Send(ctx, b); err != nil {
		return err
	}
	resp, err := c.Receive(ctx)
	if err != nil {
		return err
	}
	if string(resp.Payload) != acknowledgement {
		return ErrInvalid
	}
	return nil
}

// Prove performs a mandatory challenge acceptance on the already encrypted
// backend channel before forwarding any browser application messages.
func Prove(ctx context.Context, c protocol.Channel, sid uint64, priv ed25519.PrivateKey, audience, subject string) error {
	if c == nil {
		return ErrInvalid
	}
	b, err := Issue(priv, audience, subject, sid, c.ID(), time.Now())
	if err != nil {
		return err
	}
	if _, err = c.Send(ctx, b); err != nil {
		return err
	}
	resp, err := c.Receive(ctx)
	if err != nil {
		return err
	}
	if string(resp.Payload) != acknowledgement {
		return ErrInvalid
	}
	return nil
}

// Accept consumes and verifies exactly one proof from a gateway-owned
// channel. Call this only for explicitly authorized gateway service identities.
func (v *Verifier) Accept(ctx context.Context, c protocol.Channel, sid uint64) (string, error) {
	if c == nil {
		return "", ErrInvalid
	}
	m, err := c.Receive(ctx)
	if err != nil {
		return "", err
	}
	name, err := v.Verify(m.Payload, sid, c.ID())
	if err != nil {
		return "", err
	}
	if _, err = c.Send(ctx, []byte(acknowledgement)); err != nil {
		return "", err
	}
	return name, nil
}

func LoadPrivateKey(path string) (ed25519.PrivateKey, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("delegation: signing key must be owner-readable only (0600)")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 8193))
	if err != nil || len(raw) > 8192 {
		return nil, ErrInvalid
	}
	block, rest := pem.Decode(raw)
	if block == nil || block.Type != "PRIVATE KEY" || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, ErrInvalid
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ed, ok := key.(ed25519.PrivateKey)
	if !ok || len(ed) != ed25519.PrivateKeySize {
		return nil, ErrInvalid
	}
	return ed, nil
}
