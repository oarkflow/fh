package gateway

// A deliberately narrow, offline-pinned JWT verifier for HTTP Bearer access.
// This is not an OIDC discovery client. The operator provisions the public
// keys from a trusted IdP and rotates them explicitly (kid -> public key).
// No "none", HMAC-vs-RSA confusion, dynamic jku/x5u, or unsigned claims.

import (
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

var ErrJWT = errors.New("gateway: invalid access token")

type JWTKey struct {
	KeyID     string
	Algorithm string
	PublicKey crypto.PublicKey
}
type JWTVerifier struct {
	Issuer    string
	Audience  string
	Keys      map[string]JWTKey
	Clock     func() time.Time
	ClockSkew time.Duration
}

func ParseJWTPublicKeyPEM(pemBytes []byte, algorithm string) (crypto.PublicKey, error) {
	b, rest := pem.Decode(pemBytes)
	if b == nil || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, ErrJWT
	}
	var key crypto.PublicKey
	var err error
	switch b.Type {
	case "PUBLIC KEY":
		key, err = x509.ParsePKIXPublicKey(b.Bytes)
	case "RSA PUBLIC KEY":
		key, err = x509.ParsePKCS1PublicKey(b.Bytes)
	default:
		return nil, ErrJWT
	}
	if err != nil {
		return nil, err
	}
	switch algorithm {
	case "EdDSA":
		if k, ok := key.(ed25519.PublicKey); ok && len(k) == ed25519.PublicKeySize {
			return k, nil
		}
	case "RS256":
		if k, ok := key.(*rsa.PublicKey); ok && k.N.BitLen() >= 2048 {
			return k, nil
		}
	}
	return nil, errors.New("gateway: incompatible JWT algorithm or weak key")
}

func (v *JWTVerifier) Validate() error {
	if v == nil || v.Issuer == "" || v.Audience == "" || len(v.Keys) == 0 {
		return errors.New("gateway: issuer, audience and pinned signing keys required")
	}
	for kid, k := range v.Keys {
		if kid == "" || k.KeyID != kid {
			return ErrJWT
		}
		switch k.Algorithm {
		case "EdDSA":
			p, ok := k.PublicKey.(ed25519.PublicKey)
			if !ok || len(p) != ed25519.PublicKeySize {
				return ErrJWT
			}
		case "RS256":
			p, ok := k.PublicKey.(*rsa.PublicKey)
			if !ok || p.N.BitLen() < 2048 {
				return ErrJWT
			}
		default:
			return ErrJWT
		}
	}
	if v.ClockSkew < 0 || v.ClockSkew > time.Minute {
		return ErrJWT
	}
	return nil
}

func decodeSegment(s string, dst any) error {
	if len(s) > 8192 {
		return ErrJWT
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return ErrJWT
	}
	if err = json.Unmarshal(raw, dst); err != nil {
		return ErrJWT
	}
	return nil
}

func validAudience(value any, audience string) bool {
	switch a := value.(type) {
	case string:
		return a == audience
	case []any:
		for _, x := range a {
			if x == audience {
				return true
			}
		}
	}
	return false
}

func claimTime(m map[string]any, name string, required bool) (int64, bool) {
	n, ok := m[name]
	if !ok {
		return 0, !required
	}
	f, ok := n.(json.Number)
	if !ok {
		return 0, false
	}
	i, err := f.Int64()
	if err != nil || i < 0 {
		return 0, false
	}
	return i, true
}

// Authenticate validates an access JWT and returns its subject. The signing
// identity is verified independently on every request; no token claims are
// trusted before its signature and issuer/audience/time checks succeed.
func (v *JWTVerifier) Authenticate(r *http.Request) (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	authorization := r.Header.Get("Authorization")
	if len(authorization) > 8192 || !strings.HasPrefix(authorization, "Bearer ") || strings.Count(authorization, " ") != 1 {
		return "", ErrJWT
	}
	token := strings.TrimPrefix(authorization, "Bearer ")
	parts := strings.Split(token, ".")
	if len(parts) != 3 || len(token) > 8192 {
		return "", ErrJWT
	}
	var hdr struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
		Type      string `json:"typ"`
	}
	if err := decodeSegment(parts[0], &hdr); err != nil {
		return "", err
	}
	key, ok := v.Keys[hdr.KeyID]
	if !ok || key.Algorithm != hdr.Algorithm || (hdr.Type != "" && hdr.Type != "JWT" && hdr.Type != "at+jwt") {
		return "", ErrJWT
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", ErrJWT
	}
	content := []byte(parts[0] + "." + parts[1])
	switch key.Algorithm {
	case "EdDSA":
		if !ed25519.Verify(key.PublicKey.(ed25519.PublicKey), content, signature) {
			return "", ErrJWT
		}
	case "RS256":
		hash := sha256.Sum256(content)
		if rsa.VerifyPKCS1v15(key.PublicKey.(*rsa.PublicKey), crypto.SHA256, hash[:], signature) != nil {
			return "", ErrJWT
		}
	default:
		return "", ErrJWT
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", ErrJWT
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var claims map[string]any
	if err := dec.Decode(&claims); err != nil {
		return "", ErrJWT
	}
	iss, ok := claims["iss"].(string)
	if !ok || iss != v.Issuer || !validAudience(claims["aud"], v.Audience) {
		return "", ErrJWT
	}
	sub, ok := claims["sub"].(string)
	if !ok || len(sub) == 0 || len(sub) > 256 {
		return "", ErrJWT
	}
	exp, ok := claimTime(claims, "exp", true)
	if !ok {
		return "", ErrJWT
	}
	nbf, ok := claimTime(claims, "nbf", false)
	if !ok {
		return "", ErrJWT
	}
	iat, ok := claimTime(claims, "iat", false)
	if !ok {
		return "", ErrJWT
	}
	now := time.Now()
	if v.Clock != nil {
		now = v.Clock()
	}
	skew := int64(v.ClockSkew.Seconds())
	unix := now.Unix()
	if unix >= exp+skew || (nbf != 0 && unix+skew < nbf) || (iat != 0 && unix+skew < iat) || (iat != 0 && exp <= iat) {
		return "", ErrJWT
	}
	return sub, nil
}

func (v *JWTVerifier) String() string {
	return fmt.Sprintf("JWTVerifier(issuer=%q,audience=%q,keys=%d)", v.Issuer, v.Audience, len(v.Keys))
}
