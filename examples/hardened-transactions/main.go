package main

import (
	"crypto/ed25519"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/oarkflow/fh"
	httpsig "github.com/oarkflow/fh/pkg/httpsignature"
)

type ServerConfig struct {
	Addr                     string
	ClientPublicKey          ed25519.PublicKey
	ServerPrivateKey         ed25519.PrivateKey
	KeyID                    string
	DataDir                  string
	IdempotencyProcessingTTL time.Duration
}

func setupApp(cfg ServerConfig) (*fh.App, error) {
	app := fh.NewProduction()

	// Reliability subsystem with deadlock-free idempotency
	reliability, err := fh.NewReliability(fh.ReliabilityConfig{
		Enabled:                      true,
		DataDir:                      cfg.DataDir,
		JournalEnabled:               true,
		IdempotencyEnabled:           true,
		IdempotencyTTL:               24 * time.Hour,
		IdempotencyProcessingTimeout: cfg.IdempotencyProcessingTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("setup reliability: %w", err)
	}
	app.Use(reliability.Middleware())

	// Verified Transaction Endpoint: requires valid RFC 9421 / RFC 9530 signature from client
	app.Post("/api/transactions", func(c fh.Ctx) error {
		sigInput := c.Get(httpsig.HeaderSignatureInput)
		signature := c.Get(httpsig.HeaderSignature)
		contentDigest := c.Get(httpsig.HeaderContentDigest)

		if sigInput == "" || signature == "" || contentDigest == "" {
			return c.Status(fh.StatusUnauthorized).JSON(fh.Map{
				"error": "missing_rfc9421_signature",
			})
		}

		body := c.Body()
		if err := httpsig.VerifyContentDigest(contentDigest, body); err != nil {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{
				"error": "content_digest_mismatch",
			})
		}

		// Verify RFC 9421 signature over @method, @target-uri, content-digest, content-type
		targetURI := c.BaseURL() + c.OriginalURL()
		if err := httpsig.VerifyRequest(cfg.ClientPublicKey, httpsig.DefaultLabel, c.Method(), targetURI, c.Get("Content-Type"), body, sigInput, signature); err != nil {
			return c.Status(fh.StatusUnauthorized).JSON(fh.Map{
				"error": "request_signature_verification_failed",
			})
		}

		return c.Status(fh.StatusOK).JSON(fh.Map{
			"status":    "processed",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"length":    len(body),
		})
	})

	return app, nil
}

// BuildHardenedOutboundClient demonstrates outbound client configuration with SSRF defense,
// redirect protection, and certificate pinning.
func BuildHardenedOutboundClient(allowedHost string, pins map[string][]string) *fh.Client {
	return fh.NewClient(fh.ClientConfig{
		Timeout:     5 * time.Second,
		DialTimeout: 2 * time.Second,
		Security: fh.ClientSecurity{
			Strict:          true,
			RequireHTTPS:    true,
			AllowLocalhost:  false,
			AllowPrivateIPs: false,
			AllowedHosts:    map[string]bool{allowedHost: true},
		},
		CertPinning: &fh.CertPinningConfig{
			Pins: pins,
		},
		Redirect: fh.RedirectPolicy{
			Max:                   3,
			SameHostOnly:          true,
			HTTPSOnly:             true,
			StripSensitiveHeaders: true,
		},
	})
}

func main() {
	genKeys := flag.Bool("generate-keys", false, "generate Ed25519 keypair for test configuration")
	listenAddr := flag.String("listen", "127.0.0.1:8443", "listening address")
	flag.Parse()

	if *genKeys {
		pub, priv, err := httpsig.GenerateKey()
		if err != nil {
			log.Fatal(err)
		}
		pubB64, _ := httpsig.EncodePublicKey(pub)
		privB64, _ := httpsig.EncodePrivateKey(priv)
		fmt.Printf("PUBLIC_KEY=%s\nPRIVATE_KEY=%s\n", pubB64, privB64)
		return
	}

	clientPub, _, _ := httpsig.GenerateKey()
	_, serverPriv, _ := httpsig.GenerateKey()

	cfg := ServerConfig{
		Addr:                     *listenAddr,
		ClientPublicKey:          clientPub,
		ServerPrivateKey:         serverPriv,
		KeyID:                    "hardened-tx-2026",
		DataDir:                  ".fh-hardened-tx",
		IdempotencyProcessingTTL: 30 * time.Second,
	}

	app, err := setupApp(cfg)
	if err != nil {
		log.Fatalf("failed to setup server: %v", err)
	}

	log.Printf("Starting hardened transaction server on %s", cfg.Addr)
	if err := app.Listen(cfg.Addr); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}
