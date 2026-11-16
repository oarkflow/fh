package durable

// EncryptedStore is a self-contained at-rest confidentiality layer for the
// durable WAL. It uses versioned AES-256-GCM envelopes and a keyring with an
// independently derived HMAC nonce key. Idempotent retries produce identical
// ciphertext; changing the authenticated payload produces a new nonce.
// Version 2 AAD binds the ciphertext to the principal and message identifiers;
// version 1 remains readable for migration. WAL cursors and other record
// metadata remain visible; whole-disk encryption is still advised.

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
	"sort"
	"sync"

	"github.com/oarkflow/fh/protocol"
)

const encryptedVersion byte = 2

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
	closed bool
}

func NewEncryptedStore(base *Store, active uint16, raw map[uint16][]byte) (*EncryptedStore, error) {
	if base == nil || len(raw) == 0 || len(raw) > 16 {
		return nil, errors.New("durable: storage/keyring required")
	}
	e := &EncryptedStore{base: base, active: active, keys: make(map[uint16]atRestKey, len(raw))}
	initialized := false
	defer func() {
		if !initialized {
			_ = e.Close()
		}
	}()
	for id, key := range raw {
		if len(key) != 32 {
			return nil, errors.New("durable: at-rest encryption keys must be 32 bytes")
		}
		cryptKey := hmacSHA256(key, []byte("MIRA encrypted WAL v1 key"))
		nonceKey := hmacSHA256(key, []byte("MIRA encrypted WAL v1 nonce"))
		block, err := aes.NewCipher(cryptKey[:])
		clear(cryptKey[:])
		if err != nil {
			clear(nonceKey[:])
			return nil, err
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			clear(nonceKey[:])
			return nil, err
		}
		e.keys[id] = atRestKey{aead: aead, nonceKey: nonceKey}
		clear(nonceKey[:])
	}
	if _, ok := e.keys[active]; !ok {
		return nil, errors.New("durable: active key ID missing")
	}
	initialized = true
	return e, nil
}

// Close clears the in-memory nonce keyring. The caller owns and must close the
// underlying Store separately, after stopping concurrent Store operations.
func (s *EncryptedStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	for id, key := range s.keys {
		clear(key.nonceKey[:])
		s.keys[id] = key
	}
	clear(s.keys)
	s.keys = nil
	s.active = 0
	s.closed = true
	return nil
}

func (s *EncryptedStore) checkOpen() error {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	return nil
}

func hmacSHA256(key, msg []byte) [32]byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(msg)
	var result [32]byte
	sum := mac.Sum(nil)
	copy(result[:], sum)
	clear(sum)
	return result
}
func (s *EncryptedStore) Rotate(active uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
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
func messageAAD(p protocol.Principal, m protocol.Message) []byte {
	principal, _ := json.Marshal(p)
	var metadata [16]byte
	binary.BigEndian.PutUint64(metadata[:8], m.ChannelID)
	binary.BigEndian.PutUint64(metadata[8:], m.MessageID)
	aad := make([]byte, 0, len("MIRA WAL AAD v2:")+len(metadata)+len(principal))
	aad = append(aad, "MIRA WAL AAD v2:"...)
	aad = append(aad, metadata[:]...)
	return append(aad, principal...)
}
func (s *EncryptedStore) encrypt(p protocol.Principal, id string, m protocol.Message) (protocol.Message, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return protocol.Message{}, ErrClosed
	}
	keyID := s.active
	s.mu.RUnlock()
	return s.encryptWithKey(p, id, m, keyID)
}
func (s *EncryptedStore) encryptWithKey(p protocol.Principal, id string, m protocol.Message, keyID uint16) (protocol.Message, error) {
	return s.encryptWithKeyVersion(p, id, m, keyID, encryptedVersion)
}
func (s *EncryptedStore) encryptWithKeyVersion(p protocol.Principal, id string, m protocol.Message, keyID uint16, version byte) (protocol.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return protocol.Message{}, ErrClosed
	}
	k := s.keys[keyID]
	if k.aead == nil {
		return protocol.Message{}, ErrEncryption
	}
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
		defer clear(input)
		digest := hmacSHA256(k.nonceKey[:], input)
		copy(nonce, digest[:12])
		clear(digest[:])
	} else if _, err := rand.Read(nonce); err != nil {
		return protocol.Message{}, err
	}
	envelope := make([]byte, 0, 3+12+len(m.Payload)+k.aead.Overhead())
	envelope = append(envelope, version, 0, 0)
	binary.BigEndian.PutUint16(envelope[1:3], keyID)
	envelope = append(envelope, nonce...)
	aad := messageAAD(p, m)
	if version == 1 {
		aad = principalAAD(p)
	}
	envelope = k.aead.Seal(envelope, nonce, m.Payload, aad)
	m.Payload = envelope
	return m, nil
}
func (s *EncryptedStore) decrypt(p protocol.Principal, m protocol.Message) (protocol.Message, error) {
	b := m.Payload
	if len(b) < 3+12+16 || (b[0] != 1 && b[0] != encryptedVersion) {
		return protocol.Message{}, ErrEncryption
	}
	keyID := binary.BigEndian.Uint16(b[1:3])
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return protocol.Message{}, ErrClosed
	}
	k, ok := s.keys[keyID]
	if !ok || k.aead == nil {
		return protocol.Message{}, ErrEncryption
	}
	var aad []byte
	if b[0] == 1 {
		aad = principalAAD(p)
	} else {
		aad = messageAAD(p, m)
	}
	out, err := k.aead.Open(nil, b[3:15], b[15:], aad)
	if err != nil {
		return protocol.Message{}, ErrEncryption
	}
	m.Payload = out
	return m, nil
}
func (s *EncryptedStore) AppendIdempotent(ctx context.Context, p protocol.Principal, id string, m protocol.Message) (uint64, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	if err := validatePrincipal(p); err != nil {
		return 0, err
	}
	if len(id) > 256 {
		return 0, errors.New("durable: idempotency key too long")
	}
	if len(m.Payload) > 1<<20 {
		return 0, ErrLimit
	}
	ciphered, err := s.encrypt(p, id, m)
	if err != nil {
		return 0, err
	}
	cursor, err := s.base.AppendIdempotent(ctx, p, id, ciphered)
	if err != ErrConflict || id == "" {
		return cursor, err
	}
	// Reconstruct deterministic ciphertext using retained keys so idempotent
	// retries survive key rotation. Try v2 first, then v1 records written before
	// principal and message identifiers were added to AAD.
	s.mu.RLock()
	ids := make([]uint16, 0, len(s.keys))
	for k := range s.keys {
		ids = append(ids, k)
	}
	s.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, version := range []byte{encryptedVersion, 1} {
		for _, old := range ids {
			prior, e := s.encryptWithKeyVersion(p, id, m, old, version)
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
	}
	return 0, ErrConflict
}
func (s *EncryptedStore) Append(ctx context.Context, p protocol.Principal, m protocol.Message) (uint64, error) {
	return s.AppendIdempotent(ctx, p, "", m)
}
func (s *EncryptedStore) ReadRecordsAfter(ctx context.Context, p protocol.Principal, cursor uint64, max int) ([]Entry, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	rows, err := s.base.ReadRecordsAfter(ctx, p, cursor, max)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		decoded, err := s.decrypt(p, rows[i].Message)
		if err != nil {
			// This batch is not returned to the caller. Clear plaintext from
			// earlier rows and ciphertext copied from the base store before
			// releasing the partially decoded slice to the garbage collector.
			for j := range rows {
				clear(rows[j].Message.Payload)
			}
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
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.base.Acknowledge(ctx, p, cursor)
}
func (s *EncryptedStore) ReplayCursor(p protocol.Principal) (uint64, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	return s.base.ReplayCursor(p)
}
