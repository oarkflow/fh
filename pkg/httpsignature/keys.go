package httpsignature

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
)

func GenerateKey() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// EncodePrivateKey encodes the 32-byte Ed25519 seed as unpadded base64url.
func EncodePrivateKey(key ed25519.PrivateKey) (string, error) {
	if len(key) != ed25519.PrivateKeySize {
		return "", errors.New("http signature: invalid Ed25519 private key")
	}
	return base64.RawURLEncoding.EncodeToString(key.Seed()), nil
}

func DecodePrivateKey(value string) (ed25519.PrivateKey, error) {
	seed, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, errors.New("http signature: private key must be a base64url Ed25519 seed")
	}
	key := ed25519.NewKeyFromSeed(seed)
	for i := range seed {
		seed[i] = 0
	}
	return key, nil
}

func EncodePublicKey(key ed25519.PublicKey) (string, error) {
	if len(key) != ed25519.PublicKeySize {
		return "", errors.New("http signature: invalid Ed25519 public key")
	}
	return base64.RawURLEncoding.EncodeToString(key), nil
}

func DecodePublicKey(value string) (ed25519.PublicKey, error) {
	key, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("http signature: public key must be a base64url Ed25519 key")
	}
	return ed25519.PublicKey(key), nil
}
