package httpsignature

import (
	"bytes"
	"errors"
	"testing"
)

// Verifier.VerifyMessage's net/http-facing tests (constructing signed
// *http.Request/*http.Response fixtures) live in
// pkg/httpsignature/httpclient, alongside the net/http convenience wrappers
// they exercise — see the package doc there for why that split exists.

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

func TestSignRequestRoundTrip(t *testing.T) {
	publicKey, privateKey, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	nonce, err := NewNonce()
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"action":"transfer","amount":100}`)
	params := Parameters{
		Created: 1_700_000_000,
		Expires: 1_700_000_090,
		Nonce:   nonce,
		KeyID:   "client-device-key-1",
	}
	digest, input, sig, err := SignRequest(privateKey, DefaultLabel, params, "POST", "https://api.example.com/v1/transfer", "application/json", body)
	if err != nil {
		t.Fatalf("SignRequest failed: %v", err)
	}
	if digest == "" || input == "" || sig == "" {
		t.Fatal("expected non-empty signature fields")
	}
	if err := VerifyContentDigest(digest, body); err != nil {
		t.Fatalf("VerifyContentDigest failed: %v", err)
	}
	// Verify raw signature using public key and reconstructed base
	_, _, err = parseSignatureInput(input, DefaultLabel)
	if err == nil {
		t.Fatalf("expected parseSignatureInput to reject response params on request input")
	}
	// Extract raw parameter substring
	prefix := DefaultLabel + "="
	if !bytes.HasPrefix([]byte(input), []byte(prefix)) {
		t.Fatalf("malformed input: %s", input)
	}
	rawParams := input[len(prefix):]
	base, err := RequestSignatureBase("POST", "https://api.example.com/v1/transfer", digest, "application/json", rawParams)
	if err != nil {
		t.Fatalf("RequestSignatureBase failed: %v", err)
	}
	signatureBytes, err := parseSignature(sig, DefaultLabel)
	if err != nil {
		t.Fatalf("parseSignature failed: %v", err)
	}
	if !verify(publicKey, base, signatureBytes) {
		t.Fatal("signature verification failed")
	}
}
