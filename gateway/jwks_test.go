package gateway

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestRotatingJWKSRotationStalenessAndFailure(t *testing.T) {
	now := time.Unix(1800000000, 0)
	pub1, priv1, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub2, priv2, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	selected := 1
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		current := selected
		mu.Unlock()
		if current == 0 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		key, kid := pub1, "k1"
		if current == 2 {
			key, kid = pub2, "k2"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
			"kid": kid, "kty": "OKP", "alg": "EdDSA", "crv": "Ed25519", "use": "sig", "x": base64.RawURLEncoding.EncodeToString(key),
		}}})
	}))
	defer server.Close()
	r, err := NewRotatingJWKS(JWKSOptions{Issuer: "https://issuer.example", Audience: "mira", URL: server.URL, HTTPClient: server.Client(), Clock: func() time.Time { mu.Lock(); defer mu.Unlock(); return now }, RefreshInterval: time.Second, MaxStaleness: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "https://gateway.example", nil)
	token := func(kid string, priv ed25519.PrivateKey) string {
		return makeToken(t, "EdDSA", kid, map[string]any{"iss": "https://issuer.example", "aud": "mira", "sub": "alice", "exp": 1800000100}, func(data []byte) []byte { return ed25519.Sign(priv, data) })
	}
	req.Header.Set("Authorization", "Bearer "+token("k1", priv1))
	if _, err := r.Authenticate(req); err == nil {
		t.Fatal("uninitialized keys accepted")
	}
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sub, err := r.Authenticate(req); err != nil || sub != "alice" {
		t.Fatalf("first signing key: %q %v", sub, err)
	}
	mu.Lock()
	selected = 2
	mu.Unlock()
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Authenticate(req); err == nil {
		t.Fatal("retired key still accepted")
	}
	req.Header.Set("Authorization", "Bearer "+token("k2", priv2))
	if _, err := r.Authenticate(req); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	selected = 0
	mu.Unlock()
	if err := r.Refresh(context.Background()); err == nil {
		t.Fatal("broken upstream accepted")
	}
	if _, err := r.Authenticate(req); err != nil {
		t.Fatalf("should allow bounded outage: %v", err)
	}
	mu.Lock()
	now = now.Add(4 * time.Second)
	mu.Unlock()
	if _, err := r.Authenticate(req); err == nil {
		t.Fatal("stale signing keys accepted")
	}
}

func TestJWKSRejectsUnsafeSourcesAndKeys(t *testing.T) {
	for _, url := range []string{"http://example.test/keys", "file:///etc/passwd", "https://user:secret@example.test/keys", "https://example.test/keys#fragment"} {
		if _, err := NewRotatingJWKS(JWKSOptions{Issuer: "issuer", Audience: "aud", URL: url}); err == nil {
			t.Fatalf("accepted unsafe endpoint %q", url)
		}
	}
	for _, bad := range []string{
		`{"keys":[]}`,
		`{"keys":[{"kid":"x","kty":"oct","alg":"HS256","k":"abc"}]}`,
		`{"keys":[{"kid":"same","kty":"OKP","alg":"EdDSA","crv":"Ed25519","x":"AA"},{"kid":"same"}]}`,
	} {
		if _, err := parseJWKS([]byte(bad)); err == nil {
			t.Fatalf("accepted weak keys %s", bad)
		}
	}
	destination := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"keys":[]}`)) }))
	defer destination.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, http.StatusFound) }))
	defer redirect.Close()
	opts := JWKSOptions{Issuer: "issuer", Audience: "aud", URL: redirect.URL, HTTPClient: redirect.Client()}
	r, _ := NewRotatingJWKS(opts)
	if err := r.Refresh(context.Background()); err == nil {
		t.Fatal("followed a redirect")
	}
}

func TestOIDCDiscoveryPinning(t *testing.T) {
	var issuer string
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			w.WriteHeader(404)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": issuer, "jwks_uri": issuer + "/keys"})
	}))
	defer s.Close()
	issuer = s.URL
	got, err := DiscoverOIDCJWKS(context.Background(), issuer, s.Client())
	if err != nil || got != issuer+"/keys" {
		t.Fatalf("discovery: %q %v", got, err)
	}
	_, err = DiscoverOIDCJWKS(context.Background(), issuer+"/other", s.Client())
	if err == nil {
		t.Fatal("accepted mismatched issuer")
	}
}
