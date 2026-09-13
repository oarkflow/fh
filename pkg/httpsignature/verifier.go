package httpsignature

import (
	"crypto/ed25519"
	"errors"
	"time"
)

type KeyResolver func(keyID string) (ed25519.PublicKey, bool)

type Verifier struct {
	Label       string
	KeyID       string
	PublicKey   ed25519.PublicKey
	ResolveKey  KeyResolver
	ClockSkew   time.Duration
	MaxValidity time.Duration
	Now         func() time.Time
}

// ResponseMessage contains the exact HTTP values needed by this response
// signature profile. It is useful for clients, such as WASM, that do not use
// net/http for network I/O.
type ResponseMessage struct {
	Status         int
	ContentDigest  string
	ContentType    string
	SignatureInput string
	Signature      string
}

// VerifyMessage verifies already-extracted HTTP response fields and never
// returns authenticated content to the caller on failure.
func (v Verifier) VerifyMessage(method, targetURI string, response ResponseMessage, body []byte, expectedNonce string) error {
	if method == "" || targetURI == "" || !validNonce(expectedNonce) {
		return ErrPolicy
	}
	label := defaultString(v.Label, DefaultLabel)
	rawParams, params, err := parseSignatureInput(response.SignatureInput, label)
	if err != nil {
		return err
	}
	if params.Nonce != expectedNonce {
		return ErrNonce
	}
	if v.KeyID != "" && params.KeyID != v.KeyID {
		return ErrUnsupported
	}
	now := time.Now()
	if v.Now != nil {
		now = v.Now()
	}
	skew := v.ClockSkew
	if skew <= 0 {
		skew = 30 * time.Second
	}
	maxValidity := v.MaxValidity
	if maxValidity <= 0 {
		maxValidity = 2 * time.Minute
	}
	if params.Created > now.Add(skew).Unix() || params.Expires < now.Add(-skew).Unix() || time.Duration(params.Expires-params.Created)*time.Second > maxValidity {
		return ErrExpired
	}
	digest := response.ContentDigest
	if err := VerifyContentDigest(digest, body); err != nil {
		return err
	}
	base, err := SignatureBase(response.Status, digest, response.ContentType, method, targetURI, rawParams)
	if err != nil {
		return err
	}
	signature, err := parseSignature(response.Signature, label)
	if err != nil {
		return err
	}
	key := v.PublicKey
	if v.ResolveKey != nil {
		var ok bool
		key, ok = v.ResolveKey(params.KeyID)
		if !ok {
			return ErrUnsupported
		}
	}
	if !verify(key, base, signature) {
		return ErrSignature
	}
	return nil
}

func IsVerificationError(err error) bool {
	return errors.Is(err, ErrMalformed) || errors.Is(err, ErrPolicy) || errors.Is(err, ErrSignature) || errors.Is(err, ErrDigest) || errors.Is(err, ErrExpired) || errors.Is(err, ErrNonce) || errors.Is(err, ErrUnsupported)
}
