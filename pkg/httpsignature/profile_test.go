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
