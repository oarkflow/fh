package httpclient

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/httpsignature"
)

func signedFixture(t *testing.T) (ed25519.PublicKey, *http.Request, *http.Response, []byte, string) {
	t.Helper()
	publicKey, privateKey, err := httpsignature.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	nonce, err := httpsignature.NewNonce()
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodGet, "https://api.example.test/items?limit=2", nil)
	body := []byte(`{"items":[1,2]}`)
	params := httpsignature.Parameters{Created: 1_700_000_000, Expires: 1_700_000_090, Nonce: nonce, KeyID: "key-1", Alg: httpsignature.Algorithm, Tag: httpsignature.DefaultTag}
	digest, input, signature, err := httpsignature.SignResponse(privateKey, httpsignature.DefaultLabel, params, 200, "application/json", request.Method, request.URL.String(), body)
	if err != nil {
		t.Fatal(err)
	}
	response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}
	response.Header.Set(httpsignature.HeaderContentDigest, digest)
	response.Header.Set(httpsignature.HeaderSignatureInput, input)
	response.Header.Set(httpsignature.HeaderSignature, signature)
	response.Header.Set("Content-Type", "application/json")
	return publicKey, request, response, body, nonce
}

func TestVerifierAcceptsValidResponseAndRejectsTampering(t *testing.T) {
	publicKey, request, response, body, nonce := signedFixture(t)
	verifier := httpsignature.Verifier{
		KeyID: "key-1", PublicKey: publicKey,
		ClockSkew: time.Second, MaxValidity: 2 * time.Minute,
		Now: func() time.Time { return time.Unix(1_700_000_010, 0) },
	}
	if err := Verify(verifier, request, response, body, nonce); err != nil {
		t.Fatal(err)
	}
	if err := Verify(verifier, request, response, []byte(`{"items":[9]}`), nonce); !errors.Is(err, httpsignature.ErrDigest) {
		t.Fatalf("body tamper error=%v", err)
	}
	response.StatusCode = http.StatusCreated
	if err := Verify(verifier, request, response, body, nonce); !errors.Is(err, httpsignature.ErrSignature) {
		t.Fatalf("status tamper error=%v", err)
	}
	response.StatusCode = http.StatusOK
	response.Header.Set("Content-Type", "text/plain")
	if err := Verify(verifier, request, response, body, nonce); !errors.Is(err, httpsignature.ErrSignature) {
		t.Fatalf("content-type tamper error=%v", err)
	}
	response.Header.Set("Content-Type", "application/json")
	request.URL.Path = "/different"
	if err := Verify(verifier, request, response, body, nonce); !errors.Is(err, httpsignature.ErrSignature) {
		t.Fatalf("target tamper error=%v", err)
	}
	if err := Verify(verifier, request, response, body, "AAAAAAAAAAAAAAAAAAAAAA"); !errors.Is(err, httpsignature.ErrNonce) {
		t.Fatalf("nonce tamper error=%v", err)
	}
}

func TestVerifierRejectsSignatureAndLifetimeTampering(t *testing.T) {
	publicKey, request, response, body, nonce := signedFixture(t)
	verifier := httpsignature.Verifier{
		KeyID: "key-1", PublicKey: publicKey,
		ClockSkew: time.Second, MaxValidity: 2 * time.Minute,
		Now: func() time.Time { return time.Unix(1_700_000_010, 0) },
	}
	badSignature, err := httpsignature.FormatSignature(httpsignature.DefaultLabel, make([]byte, ed25519.SignatureSize))
	if err != nil {
		t.Fatal(err)
	}
	response.Header.Set(httpsignature.HeaderSignature, badSignature)
	if err := Verify(verifier, request, response, body, nonce); !errors.Is(err, httpsignature.ErrSignature) {
		t.Fatalf("signature tamper error=%v", err)
	}

	_, request, response, body, nonce = signedFixture(t)
	verifier.Now = func() time.Time { return time.Unix(1_700_001_000, 0) }
	if err := Verify(verifier, request, response, body, nonce); !errors.Is(err, httpsignature.ErrExpired) {
		t.Fatalf("expired signature error=%v", err)
	}
}

func TestClientDoVerifiesSignedResponse(t *testing.T) {
	publicKey, privateKey, err := httpsignature.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		accept := request.Header.Get(httpsignature.HeaderAcceptSignature)
		parsed, err := httpsignature.ParseAcceptSignature(accept, httpsignature.DefaultLabel)
		if err != nil {
			t.Fatal(err)
		}
		body := []byte(`{"ok":true}`)
		params := httpsignature.Parameters{
			Created: 1_700_000_000, Expires: 1_700_000_090,
			Nonce: parsed.Nonce, KeyID: "key-1", Alg: httpsignature.Algorithm, Tag: httpsignature.DefaultTag,
		}
		digest, input, signature, err := httpsignature.SignResponse(privateKey, httpsignature.DefaultLabel, params, 200, "application/json", request.Method, request.URL.String(), body)
		if err != nil {
			t.Fatal(err)
		}
		header := make(http.Header)
		header.Set(httpsignature.HeaderContentDigest, digest)
		header.Set(httpsignature.HeaderSignatureInput, input)
		header.Set(httpsignature.HeaderSignature, signature)
		header.Set("Content-Type", "application/json")
		return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}
	client := Client{
		HTTPClient: server,
		Verifier: httpsignature.Verifier{
			KeyID: "key-1", PublicKey: publicKey,
			ClockSkew: time.Second, MaxValidity: 2 * time.Minute,
			Now: func() time.Time { return time.Unix(1_700_000_010, 0) },
		},
	}
	request, _ := http.NewRequest(http.MethodGet, "https://api.example.test/items", nil)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"ok":true}` {
		t.Fatalf("unexpected body: %s", body)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }
