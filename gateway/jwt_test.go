package gateway

import (
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func makeToken(t *testing.T, alg, kid string, claims map[string]any, sign func([]byte) []byte) string {
	t.Helper()
	enc := func(v any) string { b, _ := json.Marshal(v); return base64.RawURLEncoding.EncodeToString(b) }
	msg := enc(map[string]any{"alg": alg, "kid": kid, "typ": "JWT"}) + "." + enc(claims)
	return msg + "." + base64.RawURLEncoding.EncodeToString(sign([]byte(msg)))
}
func TestJWTSignatureClaimsAndRotation(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	verifier := &JWTVerifier{Issuer: "https://auth.example.test", Audience: "mira-gateway", Keys: map[string]JWTKey{"2026": {KeyID: "2026", Algorithm: "EdDSA", PublicKey: pub}}, Clock: func() time.Time { return time.Unix(1800000000, 0) }}
	claims := map[string]any{"iss": verifier.Issuer, "aud": []string{verifier.Audience}, "sub": "alice", "exp": 1800000060, "iat": 1799999940}
	sign := func(b []byte) []byte { return ed25519.Sign(priv, b) }
	token := makeToken(t, "EdDSA", "2026", claims, sign)
	req := httptest.NewRequest("GET", "https://example.test/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	sub, err := verifier.Authenticate(req)
	if err != nil || sub != "alice" {
		t.Fatalf("valid token %q: %v", sub, err)
	}
	checks := []struct {
		name   string
		mutate func() string
	}{
		{"expired", func() string {
			m := map[string]any{}
			for k, v := range claims {
				m[k] = v
			}
			m["exp"] = 1799999000
			return makeToken(t, "EdDSA", "2026", m, sign)
		}},
		{"wrong-audience", func() string {
			m := map[string]any{}
			for k, v := range claims {
				m[k] = v
			}
			m["aud"] = "other"
			return makeToken(t, "EdDSA", "2026", m, sign)
		}},
		{"wrong-issuer", func() string {
			m := map[string]any{}
			for k, v := range claims {
				m[k] = v
			}
			m["iss"] = "attacker"
			return makeToken(t, "EdDSA", "2026", m, sign)
		}},
		{"unsigned", func() string { return makeToken(t, "none", "2026", claims, func([]byte) []byte { return nil }) }},
		{"unknown-kid", func() string { return makeToken(t, "EdDSA", "2025", claims, sign) }},
		{"signature", func() string { return token[:len(token)-2] + "aA" }},
	}
	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			req.Header.Set("Authorization", "Bearer "+tc.mutate())
			if _, err := verifier.Authenticate(req); err == nil {
				t.Fatal("accepted invalid token")
			}
		})
	}
	delete(verifier.Keys, "2026")
	req.Header.Set("Authorization", "Bearer "+token)
	if _, err := verifier.Authenticate(req); err == nil {
		t.Fatal("revoked key accepted")
	}
}
func TestJWT_RS256(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	v := &JWTVerifier{Issuer: "issuer", Audience: "audience", Keys: map[string]JWTKey{"rsa": {KeyID: "rsa", Algorithm: "RS256", PublicKey: &rsaKey.PublicKey}}}
	token := makeToken(t, "RS256", "rsa", map[string]any{"iss": "issuer", "aud": "audience", "sub": "bob", "exp": time.Now().Add(time.Minute).Unix()}, func(b []byte) []byte {
		hash := sha256.Sum256(b)
		s, e := rsa.SignPKCS1v15(rand.Reader, rsaKey, crypto.SHA256, hash[:])
		if e != nil {
			t.Fatal(e)
		}
		return s
	})
	req := httptest.NewRequest("GET", "https://example.test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	if sub, err := v.Authenticate(req); err != nil || sub != "bob" {
		t.Fatalf("RSA verify: %q %v", sub, err)
	}
}
