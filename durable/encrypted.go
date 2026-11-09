package durable

// EncryptedStore is a self-contained at-rest confidentiality layer for the
// durable WAL. It uses versioned AES-256-GCM envelopes and a keyring with an
// independently derived HMAC nonce key. Idempotent retries produce identical
// ciphertext; changing the authenticated payload produces a new nonce.
// AAD binds the ciphertext to the authenticated principal. WAL metadata such as
// principal and cursor remains readable; whole-disk encryption is still advised.

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"

	"github.com/oarkflow/fh/protocol"
)

const encryptedVersion byte = 1

var ErrEncryption = errors.New("durable: ciphertext authentication failed or unknown key")

type atRestKey struct {
	aead     cipher.AEAD
	nonceKey [32]byte
}

// EncryptedStore owns only the encryption keyring. Closing the underlying
// Store is the caller's responsibility. Keys are copied into private buffers;
// callers should take care with key management and process memory safety.
type EncryptedStore struct {
	base   *Store
	mu     sync.RWMutex
	active uint16
	keys   map[uint16]atRestKey
}

func NewEncryptedStore(base *Store, active uint16, raw map[uint16][]byte) (*EncryptedStore, error) {
	if base == nil || len(raw) == 0 || len(raw) > 16 {
		return nil, errors.New("durable: storage/keyring required")
	}
	e := &EncryptedStore{base: base, active: active, keys: make(map[uint16]atRestKey, len(raw))}
	for id, key := range raw {
		if len(key) != 32 {
			return nil, errors.New("durable: at-rest encryption keys must be 32 bytes")
		}
		cryptKey := hmacSHA256(key, []byte("MIRA encrypted WAL v1 key"))
		nonceKey := hmacSHA256(key, []byte("MIRA encrypted WAL v1 nonce"))
		block, err := aes.NewCipher(cryptKey[:])
		if err != nil {
			return nil, err
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}
		e.keys[id] = atRestKey{aead: aead, nonceKey: nonceKey}
	}
	if _, ok := e.keys[active]; !ok {
		return nil, errors.New("durable: active key ID missing")
	}
	return e, nil
}
func hmacSHA256(key, msg []byte) [32]byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(msg)
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}
func (s *EncryptedStore) Rotate(active uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[active]; !ok {
		return errors.New("durable: missing key ID")
	}
	s.active = active
	return nil
}
func principalAAD(p protocol.Principal) []byte {
	b, _ := json.Marshal(p)
	return append([]byte("MIRA WAL AAD v1:"), b...)
}
func (s *EncryptedStore) encrypt(p protocol.Principal, id string, m protocol.Message) (protocol.Message, error) {
	s.mu.RLock()
	keyID := s.active
	s.mu.RUnlock()
	return s.encryptWithKey(p, id, m, keyID)
}
func (s *EncryptedStore) encryptWithKey(p protocol.Principal, id string, m protocol.Message, keyID uint16) (protocol.Message, error) {
	s.mu.RLock()
	k := s.keys[keyID]
	s.mu.RUnlock()
	nonce := make([]byte, 12)
	if id != "" {
		// Domain-separated deterministic nonce for idempotent retry. Include all
		// fields that change ciphertext. A cryptographic key is necessary here:
		// never derive an AES-GCM nonce from an unkeyed message hash alone.
		input, _ := json.Marshal(struct {
			P  protocol.Principal
			ID string
			M  protocol.Message
		}{p, id, m})
		digest := hmacSHA256(k.nonceKey[:], input)
		copy(nonce, digest[:12])
	} else if _, err := rand.Read(nonce); err != nil {
		return protocol.Message{}, err
	}
	envelope := make([]byte, 0, 3+12+len(m.Payload)+k.aead.Overhead())
	envelope = append(envelope, encryptedVersion, 0, 0)
	binary.BigEndian.PutUint16(envelope[1:3], keyID)
	envelope = append(envelope, nonce...)
	envelope = k.aead.Seal(envelope, nonce, m.Payload, principalAAD(p))
	m.Payload = envelope
	return m, nil
}
func (s *EncryptedStore) decrypt(p protocol.Principal, m protocol.Message) (protocol.Message, error) {
	b := m.Payload
	if len(b) < 3+12+16 || b[0] != encryptedVersion {
		return protocol.Message{}, ErrEncryption
	}
	keyID := binary.BigEndian.Uint16(b[1:3])
	s.mu.RLock()
	k, ok := s.keys[keyID]
	s.mu.RUnlock()
	if !ok {
		return protocol.Message{}, ErrEncryption
	}
	out, err := k.aead.Open(nil, b[3:15], b[15:], principalAAD(p))
	if err != nil {
		return protocol.Message{}, ErrEncryption
	}
	m.Payload = out
	return m, nil
}
func (s *EncryptedStore) AppendIdempotent(ctx context.Context, p protocol.Principal, id string, m protocol.Message) (uint64, error) {
	ciphered, err := s.encrypt(p, id, m)
	if err != nil {
		return 0, err
	}
	cursor, err := s.base.AppendIdempotent(ctx, p, id, ciphered)
	if err != ErrConflict || id == "" {
		return cursor, err
	}
	// A rotated key must not break idempotent replay of an operation that was
	// persisted under a historical key. Reconstruct old ciphertext deterministically.
	s.mu.RLock()
	ids := make([]uint16, 0, len(s.keys))
	for k := range s.keys {
		if k != s.active {
			ids = append(ids, k)
		}
	}
	s.mu.RUnlock()
	for _, old := range ids {
		prior, e := s.encryptWithKey(p, id, m, old)
		if e != nil {
			return 0, e
		}
		cursor, e = s.base.AppendIdempotent(ctx, p, id, prior)
		if e == nil {
			return cursor, nil
		}
		if e != ErrConflict {
			return 0, e
		}
	}
	return 0, ErrConflict
}
func (s *EncryptedStore) Append(ctx context.Context, p protocol.Principal, m protocol.Message) (uint64, error) {
	return s.AppendIdempotent(ctx, p, "", m)
}
func (s *EncryptedStore) ReadRecordsAfter(ctx context.Context, p protocol.Principal, cursor uint64, max int) ([]Entry, error) {
	rows, err := s.base.ReadRecordsAfter(ctx, p, cursor, max)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		decoded, err := s.decrypt(p, rows[i].Message)
		if err != nil {
			return nil, err
		}
		rows[i].Message = decoded
	}
	return rows, nil
}
func (s *EncryptedStore) ReadAfter(ctx context.Context, p protocol.Principal, cursor uint64, max int) ([]protocol.Message, error) {
	rows, err := s.ReadRecordsAfter(ctx, p, cursor, max)
	if err != nil {
		return nil, err
	}
	out := make([]protocol.Message, len(rows))
	for i, row := range rows {
		out[i] = row.Message
	}
	return out, nil
}
func (s *EncryptedStore) Acknowledge(ctx context.Context, p protocol.Principal, cursor uint64) error {
	return s.base.Acknowledge(ctx, p, cursor)
}
func (s *EncryptedStore) ReplayCursor(p protocol.Principal) (uint64, error) {
	return s.base.ReplayCursor(p)
}
