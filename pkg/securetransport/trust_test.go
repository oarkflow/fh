package securetransport

import (
	"encoding/base64"
	"errors"
	"testing"
)

func TestParseTrustBundleAndRuntimeMatch(t *testing.T) {
	transport := [X25519KeySize]byte{1, 2, 3}
	response := [Ed25519PublicSize]byte{4, 5, 6}
	bundle, err := ParseTrustBundle(
		"https://api.example.test/",
		base64.RawURLEncoding.EncodeToString(transport[:]),
		"transport-2026-01",
		base64.RawURLEncoding.EncodeToString(response[:]),
		"response-2026-01",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bundle.Matches("https://api.example.test", transport, "transport-2026-01", response, "response-2026-01") {
		t.Fatal("matching runtime pins were rejected")
	}
	response[0] ^= 1
	if bundle.Matches("https://api.example.test", transport, "transport-2026-01", response, "response-2026-01") {
		t.Fatal("modified response signing key matched trusted bundle")
	}
}

func TestParseTrustBundleFailsClosed(t *testing.T) {
	if _, err := ParseTrustBundle("", "", "", "", ""); !errors.Is(err, ErrTrustUnavailable) {
		t.Fatalf("empty bundle error=%v", err)
	}
	if _, err := ParseTrustBundle("https://api.example.test", "missing", "", "", ""); err == nil {
		t.Fatal("partial trust bundle was accepted")
	}
	keyBytes := make([]byte, 32)
	keyBytes[0] = 1
	key := base64.RawURLEncoding.EncodeToString(keyBytes)
	if _, err := ParseTrustBundle("http://api.example.test", key, "transport", key, "response"); err == nil {
		t.Fatal("non-TLS production trust origin was accepted")
	}
}
