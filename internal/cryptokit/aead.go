// Package cryptokit provides AEAD packet protection after an externally
// authenticated handshake. THIS PACKAGE DOES NOT IMPLEMENT A HANDSHAKE.
package cryptokit

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// TrafficKeys are derived independently for both directions and key epochs.
type TrafficKeys struct {
	Key [32]byte
	IV  [12]byte
}

// DeriveTrafficKeys uses RFC 5869 HKDF-Extract and HKDF-Expand with SHA-256.
// inputSecret MUST be produced by an authenticated key exchange, not by a password.
// Context includes protocol version, connection ID, epoch and a direction label.
func DeriveTrafficKeys(inputSecret []byte, context []byte) (TrafficKeys, error) {
	if len(inputSecret) < 32 {
		return TrafficKeys{}, errors.New("secret must be >=32 bytes")
	}
	salt := []byte("MIRA-T/1 hkdf extract salt")
	extract := hmac.New(sha256.New, salt)
	extract.Write(inputSecret)
	prk := extract.Sum(nil)
	info := append([]byte("MIRA-T/1 traffic keys "), context...)
	okm := make([]byte, 44)
	var t []byte
	for i, pos := byte(1), 0; pos < len(okm); i++ {
		h := hmac.New(sha256.New, prk)
		h.Write(t)
		h.Write(info)
		h.Write([]byte{i})
		t = h.Sum(nil)
		pos += copy(okm[pos:], t)
	}
	var out TrafficKeys
	copy(out.Key[:], okm[:32])
	copy(out.IV[:], okm[32:])
	return out, nil
}

type Protector struct {
	aead cipher.AEAD
	iv   [12]byte
}

func NewProtector(k TrafficKeys) (*Protector, error) {
	block, err := aes.NewCipher(k.Key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Protector{aead: aead, iv: k.IV}, nil
}

func (p *Protector) nonce(packetNumber uint64) [12]byte {
	nonce := p.iv
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], packetNumber)
	for i := 0; i < 8; i++ {
		nonce[4+i] ^= encoded[i]
	}
	return nonce
}

// Seal appends a 16-byte tag. Callers must NEVER encrypt two different
// plaintexts with the same packet number under the same traffic key.
func (p *Protector) Seal(dst, plaintext, header []byte, packetNumber uint64) []byte {
	nonce := p.nonce(packetNumber)
	return p.aead.Seal(dst, nonce[:], plaintext, header)
}

func (p *Protector) Open(dst, ciphertext, header []byte, packetNumber uint64) ([]byte, error) {
	nonce := p.nonce(packetNumber)
	return p.aead.Open(dst, nonce[:], ciphertext, header)
}
