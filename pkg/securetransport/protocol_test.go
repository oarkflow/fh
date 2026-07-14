package securetransport

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"testing"
	"time"
)

func testKey(seed byte) [32]byte {
	var key [32]byte
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

func TestRequestRoundTripAndTamperDetection(t *testing.T) {
	key := testKey(7)
	sessionID, _ := NewID()
	requestID, _ := NewID()
	nonce, _ := NewAEADNonce()
	now := time.Now()
	env := RequestEnvelope{
		SessionID: sessionID,
		RequestID: requestID,
		Sequence:  41,
		IssuedAt:  now.UnixMilli(),
		ExpiresAt: now.Add(time.Minute).UnixMilli(),
		Nonce:     nonce,
	}
	payload := RequestPayload{
		ContentType: "application/json",
		Headers:     []Header{{Name: "authorization", Value: "Bearer secret"}, {Name: "x-workspace", Value: "nepal"}},
		Body:        []byte(`{"hello":"world"}`),
	}
	encoded, err := EncryptRequest(key, "POST", "/v1/items?a=1&a=2", env, payload, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decodedEnv, decodedPayload, err := DecryptRequest(key, "POST", "/v1/items?a=1&a=2", encoded, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if decodedEnv.Sequence != env.Sequence || !EqualID(decodedEnv.RequestID, requestID) {
		t.Fatalf("request metadata mismatch: %#v", decodedEnv)
	}
	if !bytes.Equal(decodedPayload.Body, payload.Body) || decodedPayload.ContentType != payload.ContentType {
		t.Fatalf("payload mismatch: %#v", decodedPayload)
	}
	if _, _, err := DecryptRequest(key, "POST", "/v1/items?a=2&a=1", encoded, Limits{}); err != ErrAuthentication {
		t.Fatalf("target tampering must fail authentication, got %v", err)
	}
	tampered := append([]byte(nil), encoded...)
	tampered[len(tampered)-1] ^= 0x80
	if _, _, err := DecryptRequest(key, "POST", "/v1/items?a=1&a=2", tampered, Limits{}); err != ErrAuthentication {
		t.Fatalf("ciphertext tampering must fail authentication, got %v", err)
	}
}

func TestResponseRoundTripAndRequestBinding(t *testing.T) {
	key := testKey(19)
	sessionID, _ := NewID()
	requestID, _ := NewID()
	nonce, _ := NewAEADNonce()
	now := time.Now()
	env := ResponseEnvelope{
		SessionID: sessionID,
		RequestID: requestID,
		Sequence:  9,
		IssuedAt:  now.UnixMilli(),
		ExpiresAt: now.Add(time.Minute).UnixMilli(),
		Nonce:     nonce,
	}
	payload := ResponsePayload{Status: 201, ContentType: "application/json", Headers: []Header{{Name: "etag", Value: `"abc"`}}, Body: []byte(`{"id":1}`)}
	encoded, err := EncryptResponse(key, 201, env, payload, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decodedEnv, decodedPayload, err := DecryptResponse(key, 201, encoded, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if !EqualID(decodedEnv.RequestID, requestID) || decodedPayload.Status != 201 || !bytes.Equal(decodedPayload.Body, payload.Body) {
		t.Fatalf("response mismatch: %#v %#v", decodedEnv, decodedPayload)
	}
	if _, _, err := DecryptResponse(key, 200, encoded, Limits{}); err != ErrAuthentication {
		t.Fatalf("status tampering must fail authentication, got %v", err)
	}
}

func TestHandshakeDerivationAndProof(t *testing.T) {
	curve := ecdh.X25519()
	client, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	server, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientShared, err := client.ECDH(server.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	serverShared, err := server.ECDH(client.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(clientShared, serverShared) {
		t.Fatal("X25519 shared secret mismatch")
	}
	clientHello := []byte("client-hello")
	serverHello := []byte("server-hello")
	clientKeys := DeriveSessionKeys(clientShared, clientHello, serverHello)
	serverKeys := DeriveSessionKeys(serverShared, clientHello, serverHello)
	if clientKeys != serverKeys {
		t.Fatal("derived session keys mismatch")
	}
	proof := ServerProof(serverKeys.ServerToClient, clientHello, serverHello)
	if !VerifyServerProof(clientKeys.ServerToClient, clientHello, serverHello, proof) {
		t.Fatal("valid server proof was rejected")
	}
	proof[0] ^= 1
	if VerifyServerProof(clientKeys.ServerToClient, clientHello, serverHello, proof) {
		t.Fatal("tampered server proof was accepted")
	}
}

func TestRejectsDangerousProtectedHeaders(t *testing.T) {
	for _, name := range []string{"host", "content-length", "transfer-encoding", "connection", "x-fh-envelope"} {
		if ValidProtectedHeader(name, "value") {
			t.Fatalf("dangerous header %q was accepted", name)
		}
	}
	if !ValidProtectedHeader("authorization", "Bearer token") {
		t.Fatal("authorization header should be protectable")
	}
	if ValidProtectedHeader("x-test", "ok\r\nInjected: yes") {
		t.Fatal("header injection was accepted")
	}
}
