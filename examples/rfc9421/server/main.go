package main

import (
	"crypto/ed25519"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	middleware "github.com/oarkflow/fh/mw/httpsignature"
	"github.com/oarkflow/fh/mw/security"
	protocol "github.com/oarkflow/fh/pkg/httpsignature"
)

func main() {
	generate := flag.Bool("generate-key", false, "print an Ed25519 private seed and public key")
	flag.Parse()
	if *generate {
		publicKey, privateKey, err := protocol.GenerateKey()
		if err != nil {
			log.Fatal(err)
		}
		privateValue, _ := protocol.EncodePrivateKey(privateKey)
		publicValue, _ := protocol.EncodePublicKey(publicKey)
		fmt.Printf("FH_RFC9421_PRIVATE_KEY=%s\nFH_RFC9421_PUBLIC_KEY=%s\n", privateValue, publicValue)
		return
	}

	addr := env("FH_RFC9421_ADDR", "127.0.0.1:8081")
	origin := strings.TrimRight(env("FH_RFC9421_ORIGIN", "http://"+addr), "/")
	keyID := env("FH_RFC9421_KEY_ID", "response-signing-2026-01")
	privateKey, ephemeral := loadPrivateKey()
	publicKey := privateKey.Public().(ed25519.PublicKey)
	publicValue, _ := protocol.EncodePublicKey(publicKey)
	if ephemeral {
		log.Printf("WARNING: ephemeral demonstration signing key; trusted public key is %s", publicValue)
	}

	app := fh.NewProduction()
	app.Use(security.New(security.Config{
		ContentSecurityPolicy:     "default-src 'self'; script-src 'self'; connect-src 'self'; style-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
		FrameDeny:                 true,
		ContentTypeNosniff:        true,
		XSSProtection:             "0",
		CrossOriginOpenerPolicy:   "same-origin",
		CrossOriginResourcePolicy: "same-origin",
		ReferrerPolicy:            "no-referrer",
		PermissionsPolicy:         "geolocation=(), microphone=(), camera=(), payment=(), usb=()",
	}))

	// Static bootstrap resources are intentionally outside the response-signing
	// middleware. In production the public key must come from a trusted build or
	// out-of-band configuration, not this demonstration endpoint.
	app.Static("/", "examples/rfc9421/public", fh.StaticConfig{CacheDuration: time.Second})
	app.Get("/demo-config.json", func(c fh.Ctx) error {
		c.Set("Cache-Control", "no-store")
		return c.JSON(fh.Map{"publicKey": publicValue, "keyID": keyID})
	})

	signer, err := middleware.New(middleware.Config{
		PrivateKey: privateKey,
		KeyID:      keyID,
		Origin:     origin,
		Validity:   90 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	app.Use(signer)
	app.Get("/api/message", func(c fh.Ctx) error {
		return c.JSON(fh.Map{
			"message": "This body and its response metadata are covered by RFC 9421",
			"issued":  time.Now().UTC().Format(time.RFC3339),
		})
	})

	log.Printf("RFC 9421 example listening on %s", addr)
	log.Fatal(app.Listen(addr))
}

func loadPrivateKey() (ed25519.PrivateKey, bool) {
	value := strings.TrimSpace(os.Getenv("FH_RFC9421_PRIVATE_KEY"))
	if value != "" {
		key, err := protocol.DecodePrivateKey(value)
		if err != nil {
			log.Fatal(err)
		}
		return key, false
	}
	_, key, err := protocol.GenerateKey()
	if err != nil {
		log.Fatal(err)
	}
	return key, true
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
