package gateway

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func revocationTestToken(t *testing.T, now time.Time) (*JWTVerifier, string, string) {
	t.Helper()
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	verifier := &JWTVerifier{Issuer: "issuer", Audience: "aud", Keys: map[string]JWTKey{"kid": {KeyID: "kid", Algorithm: "EdDSA", PublicKey: pub}}, Clock: func() time.Time { return now }}
	mk := func(jti string) string {
		return makeToken(t, "EdDSA", "kid", map[string]any{"iss": "issuer", "aud": "aud", "sub": "alice", "jti": jti, "iat": now.Add(-time.Minute).Unix(), "exp": now.Add(time.Hour).Unix()}, func(data []byte) []byte { return ed25519.Sign(private, data) })
	}
	return verifier, mk("jwt-a"), mk("jwt-b")
}
func revocationFile(t *testing.T, path string, contents RevocationFile) {
	t.Helper()
	b, _ := json.Marshal(contents)
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}
func testAuthorized(t *testing.T, auth Authenticate, token string, want bool) {
	t.Helper()
	req := httptest.NewRequest("GET", "https://example.test/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	_, err := auth(req)
	if (err == nil) != want {
		t.Fatalf("authorization want=%v err=%v", want, err)
	}
}
func TestRevocationReloadFailClosedAndJTI(t *testing.T) {
	now := time.Unix(1800000000, 0)
	verifier, a, b := revocationTestToken(t, now)
	dir := t.TempDir()
	path := filepath.Join(dir, "revoked.json")
	r := &RevocationStore{Clock: func() time.Time { return now }, MaxAge: time.Minute}
	auth := r.Wrap(verifier.Authenticate)
	testAuthorized(t, auth, a, false)
	revocationFile(t, path, RevocationFile{RevokedJTI: []string{"jwt-a"}})
	if err := r.Reload(path); err != nil {
		t.Fatal(err)
	}
	testAuthorized(t, auth, a, false)
	testAuthorized(t, auth, b, true)
	sum := sha256.Sum256([]byte(b))
	revocationFile(t, path, RevocationFile{RevokedTokenSHA256: []string{hex.EncodeToString(sum[:])}})
	if err := r.Reload(path); err != nil {
		t.Fatal(err)
	}
	testAuthorized(t, auth, a, true)
	testAuthorized(t, auth, b, false)
	revocationFile(t, path, RevocationFile{SubjectNotBefore: map[string]int64{"alice": now.Unix()}})
	if err := r.Reload(path); err != nil {
		t.Fatal(err)
	}
	testAuthorized(t, auth, a, false)
	testAuthorized(t, auth, b, false)
	revocationFile(t, path, RevocationFile{RevokedSubjects: []string{"alice"}})
	if err := r.Reload(path); err != nil {
		t.Fatal(err)
	}
	testAuthorized(t, auth, a, false)
	if err := os.WriteFile(path, []byte(`{"unrecognized":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.Reload(path); err == nil {
		t.Fatal("accepted invalid revocation file")
	}
	testAuthorized(t, auth, a, false)
	r.Clock = func() time.Time { return now.Add(2 * time.Minute) }
	// Auth JWTVerifier uses the original clock, so this proves independent
	// expiry of revocation state, even while the JWT remains otherwise valid.
	testAuthorized(t, auth, a, false)
}
func TestRevocationConcurrentReload(t *testing.T) {
	now := time.Unix(1800000000, 0)
	verifier, a, _ := revocationTestToken(t, now)
	path := filepath.Join(t.TempDir(), "revoked.json")
	revocationFile(t, path, RevocationFile{})
	store := &RevocationStore{Clock: func() time.Time { return now }}
	if err := store.Reload(path); err != nil {
		t.Fatal(err)
	}
	auth := store.Wrap(verifier.Authenticate)
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if n == 0 {
					if err := store.Reload(path); err != nil {
						t.Error(err)
					}
					continue
				}
				req := httptest.NewRequest("GET", "https://example.test/", nil)
				req.Header.Set("Authorization", "Bearer "+a)
				if _, err := auth(req); err != nil {
					t.Error(err)
					return
				}
			}
		}(n)
	}
	wg.Wait()
}
