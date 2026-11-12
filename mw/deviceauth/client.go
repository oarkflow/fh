package deviceauth

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"io"
	"net/http"
	"strconv"
	"time"
)

// SignRequest attaches a session and its device proof to req. The body must be
// readable from req.Body; it is buffered up to maxBodyBytes so the bytes signed
// are exactly the bytes sent. This helper uses only standard Go APIs and can be
// compiled to js/wasm. Keep private-key storage outside the module.
func SignRequest(req *http.Request, body []byte, session string, privateKey ed25519.PrivateKey, now time.Time, maxBodyBytes int64) error {
	if req == nil || req.URL == nil || len(privateKey) != ed25519.PrivateKeySize || session == "" || maxBodyBytes < 0 || int64(len(body)) > maxBodyBytes {
		return ErrInvalid
	}
	if now.IsZero() {
		now = time.Now()
	}
	if req.Host == "" {
		req.Host = req.URL.Host
	}
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	nonce := base64.RawURLEncoding.EncodeToString(raw)
	stamp := now.Unix()
	canonical := CanonicalRequest(req, body, session, stamp, nonce)
	signature := ed25519.Sign(privateKey, []byte(canonical))
	req.Header.Set("Authorization", "Device "+session)
	req.Header.Set(HeaderTime, strconv.FormatInt(stamp, 10))
	req.Header.Set(HeaderNonce, nonce)
	req.Header.Set(HeaderProof, base64.RawURLEncoding.EncodeToString(signature))
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	return nil
}

// SignChallenge returns the base64url-encoded signature expected by
// FinishEnrollment or FinishLogin.
func SignChallenge(challenge string, privateKey ed25519.PrivateKey) (string, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return "", ErrInvalid
	}
	value, err := base64.RawURLEncoding.DecodeString(challenge)
	if err != nil || len(value) != 32 {
		return "", ErrInvalid
	}
	return base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, challengeMessage(value))), nil
}
