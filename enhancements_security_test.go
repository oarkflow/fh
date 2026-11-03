package fh

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIdempotencyAbortAndProcessingTimeout(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fh-idem-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "idempotency.jsonl")
	store, err := OpenIdempotencyStore(dbPath, 24*time.Hour, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key := "test-key-1"
	reqHash := "hash-1"

	// 1. Begin request
	decision, _, err := store.Begin(key, reqHash, "POST", "/charge")
	if err != nil || decision != idemNew {
		t.Fatalf("expected idemNew, got decision=%v err=%v", decision, err)
	}

	// 2. Immediate duplicate returns idemProcessing
	decision, _, err = store.Begin(key, reqHash, "POST", "/charge")
	if err != nil || decision != idemProcessing {
		t.Fatalf("expected idemProcessing, got decision=%v err=%v", decision, err)
	}

	// 3. Abort releases the lock
	if err := store.Abort(key, reqHash); err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// 4. Retry immediately succeeds as idemNew
	decision, _, err = store.Begin(key, reqHash, "POST", "/charge")
	if err != nil || decision != idemNew {
		t.Fatalf("expected idemNew after Abort, got decision=%v err=%v", decision, err)
	}

	// 5. Test processingTimeout expiry (simulating a crash where Abort was never called)
	time.Sleep(70 * time.Millisecond)
	decision, _, err = store.Begin(key, reqHash, "POST", "/charge")
	if err != nil || decision != idemNew {
		t.Fatalf("expected idemNew after processingTimeout expired, got decision=%v err=%v", decision, err)
	}
}

func TestCertPinningWithTargetHost(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "api.example.com"},
		DNSNames:     []string{"api.example.com", "other.example.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatal(err)
	}

	spkiHash := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	pin := base64.StdEncoding.EncodeToString(spkiHash[:])

	cfg := &CertPinningConfig{
		Pins: map[string][]string{
			"api.example.com": {pin},
		},
	}

	// Valid host with valid pin passes
	verifier := cfg.VerifierForHost("api.example.com")
	if err := verifier([][]byte{certDER}, [][]*x509.Certificate{{cert}}); err != nil {
		t.Fatalf("expected pin match for api.example.com, got: %v", err)
	}

	// Target host not in pin list is rejected
	verifierMismatch := cfg.VerifierForHost("unpinned.example.com")
	if err := verifierMismatch([][]byte{certDER}, [][]*x509.Certificate{{cert}}); err == nil {
		t.Fatal("expected failure for unpinned.example.com")
	}
}

func TestClientSecurityBlocksRedirectSSRF(t *testing.T) {
	client := NewClient(ClientConfig{
		Security: ClientSecurity{
			Strict:          true,
			RequireHTTPS:    true,
			AllowLocalhost:  false,
			AllowPrivateIPs: false,
		},
	})
	defer client.Close()

	// Redirect to localhost must fail
	localRedirectReq, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:8080/admin", nil)
	via := []*http.Request{{URL: &url.URL{Scheme: "https", Host: "api.example.com"}}}

	if err := client.checkRedirect(localRedirectReq, via); err == nil {
		t.Fatal("expected redirect to 127.0.0.1 to be rejected by security policy")
	}

	// Redirect to non-HTTPS must fail
	httpRedirectReq, _ := http.NewRequest(http.MethodGet, "http://api.example.com/login", nil)
	if err := client.checkRedirect(httpRedirectReq, via); err == nil {
		t.Fatal("expected redirect to plain HTTP to be rejected by RequireHTTPS")
	}
}
