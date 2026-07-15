package securetransport

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"time"

	"github.com/oarkflow/fh"
	protocol "github.com/oarkflow/fh/pkg/securetransport"
)

// GenerateServerPrivateKey creates a persistent X25519 private key. Store the
// returned 32 bytes in a KMS/HSM or secret manager; never commit them to source.
func GenerateServerPrivateKey() ([]byte, error) {
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), key.Bytes()...), nil
}

func EncodeServerPrivateKey(key []byte) (string, error) {
	if len(key) != protocol.X25519KeySize {
		return "", errors.New("secure transport: X25519 private key must be 32 bytes")
	}
	return base64.RawURLEncoding.EncodeToString(key), nil
}

func DecodeServerPrivateKey(value string) ([]byte, error) {
	key, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(key) != protocol.X25519KeySize {
		return nil, errors.New("secure transport: invalid base64url X25519 private key")
	}
	return key, nil
}

// ServerPublicKey derives the public X25519 pin from a server private key.
func ServerPublicKey(privateKey []byte) ([protocol.X25519KeySize]byte, error) {
	var out [protocol.X25519KeySize]byte
	if len(privateKey) != protocol.X25519KeySize {
		return out, errors.New("secure transport: X25519 private key must be 32 bytes")
	}
	keyBytes := append([]byte(nil), privateKey...)
	defer clear(keyBytes)
	key, err := ecdh.X25519().NewPrivateKey(keyBytes)
	if err != nil {
		return out, errors.New("secure transport: invalid X25519 private key")
	}
	copy(out[:], key.PublicKey().Bytes())
	return out, nil
}

func SessionFromContext(c fh.Ctx) (SessionInfo, bool) {
	if c == nil {
		return SessionInfo{}, false
	}
	session, ok := c.Locals(LocalSession).(SessionInfo)
	return session, ok
}

func DeviceIDFromContext(c fh.Ctx) (protocol.ID16, bool) {
	if c == nil {
		return protocol.ID16{}, false
	}
	id, ok := c.Locals(LocalDevice).(protocol.ID16)
	return id, ok
}

func RequestIDFromContext(c fh.Ctx) (protocol.ID16, bool) {
	if c == nil {
		return protocol.ID16{}, false
	}
	id, ok := c.Locals(LocalRequest).(protocol.ID16)
	return id, ok
}

// RevokeDevice invalidates the device and every active session bound to it.
func (t *Transport) RevokeDevice(id protocol.ID16) error {
	if err := t.cfg.DeviceStore.Revoke(id, time.Now()); err != nil {
		return err
	}
	return t.cfg.SessionStore.DeleteByDevice(id)
}
