package delegation

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/oarkflow/fh/protocol"
)

type publicRing struct{ keys map[string]ed25519.PublicKey }

type KeyReference struct {
	ID   string `json:"id"`
	File string `json:"file"`
}
type KeyringConfig struct {
	Active string         `json:"active,omitempty"`
	Keys   []KeyReference `json:"keys"`
}

func validKeyID(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

func loadConfig(path string) (KeyringConfig, error) {
	var cfg KeyringConfig
	b, err := os.ReadFile(path)
	if err != nil || len(b) > 16384 {
		return cfg, ErrInvalid
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if dec.Decode(&cfg) != nil || len(cfg.Keys) == 0 || len(cfg.Keys) > 8 {
		return cfg, ErrInvalid
	}
	var trailing any
	if dec.Decode(&trailing) != io.EOF {
		return cfg, ErrInvalid
	}
	seen := map[string]bool{}
	for _, k := range cfg.Keys {
		if !validKeyID(k.ID) || seen[k.ID] || !filepath.IsAbs(k.File) {
			return cfg, ErrInvalid
		}
		seen[k.ID] = true
	}
	return cfg, nil
}

func loadPublic(path string) (ed25519.PublicKey, error) {
	b, err := os.ReadFile(path)
	if err != nil || len(b) > 8192 {
		return nil, ErrInvalid
	}
	block, rest := pem.Decode(b)
	if block == nil || block.Type != "PUBLIC KEY" || strings.TrimSpace(string(rest)) != "" {
		return nil, ErrInvalid
	}
	p, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ed, ok := p.(ed25519.PublicKey)
	if !ok || len(ed) != ed25519.PublicKeySize {
		return nil, ErrInvalid
	}
	return ed, nil
}

// ReplaceKeys copies the complete trust set atomically. Trust is never updated
// using a partially parsed or partly loaded configuration.
func (v *Verifier) ReplaceKeys(keys map[string]ed25519.PublicKey) error {
	if v == nil || len(keys) == 0 || len(keys) > 8 {
		return ErrInvalid
	}
	next := &publicRing{keys: make(map[string]ed25519.PublicKey, len(keys))}
	for kid, p := range keys {
		if !validKeyID(kid) || len(p) != ed25519.PublicKeySize {
			return ErrInvalid
		}
		next.keys[kid] = append(ed25519.PublicKey(nil), p...)
	}
	v.keyring.Store(next)
	return nil
}

func (v *Verifier) ReloadKeys(path string) error {
	cfg, err := loadConfig(path)
	if err != nil {
		return err
	}
	keys := make(map[string]ed25519.PublicKey, len(cfg.Keys))
	for _, k := range cfg.Keys {
		p, err := loadPublic(k.File)
		if err != nil {
			return err
		}
		keys[k.ID] = p
	}
	return v.ReplaceKeys(keys)
}

type privateRing struct {
	active string
	key    ed25519.PrivateKey
}

// RotatingSigner signs each NEW channel with the current key. Existing signed
// channel assertions remain verifiable while their key remains in the server ring.
// It does not copy private key material into logs or responses.
type RotatingSigner struct{ current atomic.Pointer[privateRing] }

func (s *RotatingSigner) Reload(path string) error {
	if s == nil {
		return ErrInvalid
	}
	cfg, err := loadConfig(path)
	if err != nil || !validKeyID(cfg.Active) {
		return ErrInvalid
	}
	for _, k := range cfg.Keys {
		if k.ID != cfg.Active {
			continue
		}
		priv, err := LoadPrivateKey(k.File)
		if err != nil {
			return err
		}
		s.current.Store(&privateRing{active: k.ID, key: priv})
		return nil
	}
	return errors.New("delegation: active signing key missing")
}

func (s *RotatingSigner) Prove(ctx context.Context, c protocol.Channel, sid uint64, audience, subject string) error {
	if s == nil {
		return ErrInvalid
	}
	v := s.current.Load()
	if v == nil {
		return ErrInvalid
	}
	return ProveNamed(ctx, c, sid, v.key, v.active, audience, subject)
}
