package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	httpsig "github.com/oarkflow/fh/pkg/httpsignature"
)

func TestHardenedTransactionsEndToEnd(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "hardened-tx-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	clientPub, clientPriv, err := httpsig.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	serverPub, serverPriv, err := httpsig.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	_ = serverPub

	cfg := ServerConfig{
		ClientPublicKey:          clientPub,
		ServerPrivateKey:         serverPriv,
		KeyID:                    "test-tx-key",
		DataDir:                  tmpDir,
		IdempotencyProcessingTTL: time.Second,
	}

	app, err := setupApp(cfg)
	if err != nil {
		t.Fatalf("setupApp failed: %v", err)
	}

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Serve(listener) }()
	defer app.ShutdownWithTimeout(time.Second)

	serverAddr := listener.Addr().String()
	targetURI := "http://" + serverAddr + "/api/transactions"

	body := []byte(`{"action":"transfer","amount":500,"currency":"USD"}`)
	now := time.Now()
	nonce, _ := httpsig.NewNonce()
	params := httpsig.Parameters{
		Created: now.Unix(),
		Expires: now.Add(60 * time.Second).Unix(),
		Nonce:   nonce,
		KeyID:   "client-ed25519-key",
	}

	digest, sigInput, sig, err := httpsig.SignRequest(clientPriv, httpsig.DefaultLabel, params, "POST", targetURI, "application/json", body)
	if err != nil {
		t.Fatalf("SignRequest failed: %v", err)
	}

	httpClient := &http.Client{Timeout: 3 * time.Second}

	// 1. Send valid signed request with Idempotency-Key
	req, _ := http.NewRequest(http.MethodPost, targetURI, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(httpsig.HeaderContentDigest, digest)
	req.Header.Set(httpsig.HeaderSignatureInput, sigInput)
	req.Header.Set(httpsig.HeaderSignature, sig)
	req.Header.Set("Idempotency-Key", "tx-order-001")

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("transaction request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, string(respBytes))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result["status"] != "processed" {
		t.Fatalf("unexpected result: %#v", result)
	}

	// 2. Replay with identical idempotency key should succeed and be served from idempotency store
	reqReplay, _ := http.NewRequest(http.MethodPost, targetURI, bytes.NewReader(body))
	reqReplay.Header.Set("Content-Type", "application/json")
	reqReplay.Header.Set(httpsig.HeaderContentDigest, digest)
	reqReplay.Header.Set(httpsig.HeaderSignatureInput, sigInput)
	reqReplay.Header.Set(httpsig.HeaderSignature, sig)
	reqReplay.Header.Set("Idempotency-Key", "tx-order-001")

	respReplay, err := httpClient.Do(reqReplay)
	if err != nil {
		t.Fatalf("replay request failed: %v", err)
	}
	defer respReplay.Body.Close()
	if respReplay.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on replay, got %d", respReplay.StatusCode)
	}
	if respReplay.Header.Get("X-Idempotency-Replayed") != "true" {
		t.Fatalf("expected X-Idempotency-Replayed header on replayed request")
	}

	// 3. Request with tampered digest must be rejected
	reqTampered, _ := http.NewRequest(http.MethodPost, targetURI, bytes.NewReader([]byte(`{"tampered":true}`)))
	reqTampered.Header.Set("Content-Type", "application/json")
	reqTampered.Header.Set(httpsig.HeaderContentDigest, digest) // mismatched digest
	reqTampered.Header.Set(httpsig.HeaderSignatureInput, sigInput)
	reqTampered.Header.Set(httpsig.HeaderSignature, sig)
	reqTampered.Header.Set("Idempotency-Key", "tx-order-tampered")

	respTampered, err := httpClient.Do(reqTampered)
	if err != nil {
		t.Fatalf("tampered request failed: %v", err)
	}
	defer respTampered.Body.Close()
	if respTampered.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for tampered digest, got %d", respTampered.StatusCode)
	}
}

func TestBuildHardenedOutboundClientPolicy(t *testing.T) {
	client := BuildHardenedOutboundClient("api.example.com", map[string][]string{
		"api.example.com": {"fake-pin-hash"},
	})
	defer client.Close()

	if client == nil {
		t.Fatal("expected non-nil client")
	}

	// Strict security requires HTTPS and blocks local IP
	_, err := client.R().Get(t.Context(), "http://127.0.0.1:8080/secret")
	if err == nil {
		t.Fatal("expected error for plaintext local IP")
	}
}
