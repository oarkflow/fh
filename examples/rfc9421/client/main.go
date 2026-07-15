package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	protocol "github.com/oarkflow/fh/pkg/httpsignature"
)

func main() {
	requestURL := flag.String("url", "http://127.0.0.1:8081/api/message", "signed endpoint URL")
	publicValue := flag.String("public-key", os.Getenv("FH_RFC9421_PUBLIC_KEY"), "trusted base64url Ed25519 public key")
	keyID := flag.String("key-id", "response-signing-2026-01", "expected RFC 9421 keyid")
	flag.Parse()
	if strings.TrimSpace(*publicValue) == "" {
		log.Fatal("-public-key or FH_RFC9421_PUBLIC_KEY is required")
	}
	publicKey, err := protocol.DecodePublicKey(*publicValue)
	if err != nil {
		log.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, *requestURL, nil)
	if err != nil {
		log.Fatal(err)
	}
	client := protocol.Client{
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Verifier: protocol.Verifier{
			KeyID:       *keyID,
			PublicKey:   publicKey,
			ClockSkew:   30 * time.Second,
			MaxValidity: 2 * time.Minute,
		},
		MaxBodySize: 1 << 20,
	}
	response, err := client.Do(request)
	if err != nil {
		log.Fatalf("response rejected: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("verified status=%d\nSignature-Input: %s\nbody: %s\n", response.StatusCode, response.Header.Get(protocol.HeaderSignatureInput), body)
}
