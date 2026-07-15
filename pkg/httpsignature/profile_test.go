package httpsignature

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

func signedFixture(t *testing.T) (ed25519.PublicKey, *http.Request, *http.Response, []byte, string) {
	t.Helper()
	publicKey, privateKey, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	nonce, err := NewNonce()
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodGet, "https://api.example.test/items?limit=2", nil)
	body := []byte(`{"items":[1,2]}`)
	params := Parameters{Created: 1_700_000_000, Expires: 1_700_000_090, Nonce: nonce, KeyID: "key-1", Alg: Algorithm, Tag: DefaultTag}
	digest, input, signature, err := SignResponse(privateKey, DefaultLabel, params, 200, "application/json", request.Method, request.URL.String(), body)
	if err != nil {
		t.Fatal(err)
	}
	response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}
	response.Header.Set(HeaderContentDigest, digest)
	response.Header.Set(HeaderSignatureInput, input)
	response.Header.Set(HeaderSignature, signature)
	response.Header.Set("Content-Type", "application/json")
	return publicKey, request, response, body, nonce
}

func TestAcceptSignatureRoundTrip(t *testing.T) {
	nonce, _ := NewNonce()
	field, err := FormatAcceptSignature(DefaultLabel, nonce, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseAcceptSignature(field, DefaultLabel)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Nonce != nonce || parsed.KeyID != "key-1" || parsed.Alg != Algorithm || parsed.Tag != DefaultTag {
		t.Fatalf("unexpected parsed signature request: %#v", parsed)
	}
	if _, err := ParseAcceptSignature(field+`;unknown="x"`, DefaultLabel); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unknown parameter error=%v", err)
	}
}

func TestVerifierAcceptsValidResponseAndRejectsTampering(t *testing.T) {
	publicKey, request, response, body, nonce := signedFixture(t)
	verifier := Verifier{
		KeyID: "key-1", PublicKey: publicKey,
		ClockSkew: time.Second, MaxValidity: 2 * time.Minute,
		Now: func() time.Time { return time.Unix(1_700_000_010, 0) },
	}
	if err := verifier.Verify(request, response, body, nonce); err != nil {
		t.Fatal(err)
	}
	if err := verifier.Verify(request, response, []byte(`{"items":[9]}`), nonce); !errors.Is(err, ErrDigest) {
		t.Fatalf("body tamper error=%v", err)
	}
	response.StatusCode = http.StatusCreated
	if err := verifier.Verify(request, response, body, nonce); !errors.Is(err, ErrSignature) {
		t.Fatalf("status tamper error=%v", err)
	}
	response.StatusCode = http.StatusOK
	response.Header.Set("Content-Type", "text/plain")
	if err := verifier.Verify(request, response, body, nonce); !errors.Is(err, ErrSignature) {
		t.Fatalf("content-type tamper error=%v", err)
	}
	response.Header.Set("Content-Type", "application/json")
	request.URL.Path = "/different"
	if err := verifier.Verify(request, response, body, nonce); !errors.Is(err, ErrSignature) {
		t.Fatalf("target tamper error=%v", err)
	}
	if err := verifier.Verify(request, response, body, "AAAAAAAAAAAAAAAAAAAAAA"); !errors.Is(err, ErrNonce) {
		t.Fatalf("nonce tamper error=%v", err)
	}
}

func TestVerifierRejectsSignatureAndLifetimeTampering(t *testing.T) {
	publicKey, request, response, body, nonce := signedFixture(t)
	verifier := Verifier{
		KeyID: "key-1", PublicKey: publicKey,
		ClockSkew: time.Second, MaxValidity: 2 * time.Minute,
		Now: func() time.Time { return time.Unix(1_700_000_010, 0) },
	}
	badSignature, err := FormatSignature(DefaultLabel, make([]byte, ed25519.SignatureSize))
	if err != nil {
		t.Fatal(err)
	}
	response.Header.Set(HeaderSignature, badSignature)
	if err := verifier.Verify(request, response, body, nonce); !errors.Is(err, ErrSignature) {
		t.Fatalf("signature tamper error=%v", err)
	}

	_, request, response, body, nonce = signedFixture(t)
	verifier.Now = func() time.Time { return time.Unix(1_700_001_000, 0) }
	if err := verifier.Verify(request, response, body, nonce); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired signature error=%v", err)
	}
}

func TestKeyEncodingRoundTrip(t *testing.T) {
	publicKey, privateKey, _ := GenerateKey()
	privateValue, _ := EncodePrivateKey(privateKey)
	decodedPrivate, err := DecodePrivateKey(privateValue)
	if err != nil || !bytes.Equal(decodedPrivate, privateKey) {
		t.Fatal("private key round trip failed")
	}
	publicValue, _ := EncodePublicKey(publicKey)
	decodedPublic, err := DecodePublicKey(publicValue)
	if err != nil || !bytes.Equal(decodedPublic, publicKey) {
		t.Fatal("public key round trip failed")
	}
}
