// Command http2 demonstrates all three HTTP/2 modes supported by fasthttp:
//
//  1. TLS + ALPN (h2)   — curl -k https://localhost:3001
//  2. h2c prior knowledge — curl --http2-prior-knowledge http://localhost:3000
//  3. h2c upgrade          — curl --http2 http://localhost:3000
//
// The same routes work identically in all three modes.
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/oarkflow/fh"
)

func main() {
	h2cAddr := flag.String("h2c", ":3000", "cleartext HTTP/1.1, h2c upgrade, and h2c prior-knowledge address")
	tlsAddr := flag.String("tls", ":3001", "TLS address (HTTP/2 negotiated with ALPN)")
	flag.Parse()

	// Two separate app instances are needed because fasthttp.App can only be
	// started once. Both share the same route definitions.
	routes := func(app *fh.App) {
		app.Get("/", func(c *fh.Ctx) error {
			return c.JSON(map[string]any{
				"protocol": string(c.Header.Proto),
				"message":  "Hello from fasthttp",
			})
		})
		app.Get("/headers", func(c *fh.Ctx) error {
			return c.JSON(map[string]string{
				"protocol": string(c.Header.Proto),
				"host":     string(c.Header.Host),
				"ua":       c.Get("User-Agent"),
			})
		})
		app.Post("/echo", func(c *fh.Ctx) error {
			c.Set("X-Echo-Bytes", strconv.Itoa(len(c.Body())))
			return c.Send(c.Body())
		})
		app.Get("/parallel/:id", func(c *fh.Ctx) error {
			return c.JSON(map[string]string{"id": c.Param("id"), "protocol": string(c.Header.Proto)})
		})
		app.Get("/large", func(c *fh.Ctx) error {
			size, _ := strconv.Atoi(c.Query("bytes"))
			if size <= 0 {
				size = 256 << 10
			}
			if size > 8<<20 {
				size = 8 << 20
			}
			return c.SendStream(&repeatingReader{remaining: size})
		})
		app.Get("/trailers", func(c *fh.Ctx) error {
			c.SetTrailer("X-Stream-Complete", "true")
			return c.Stream(func(w *fh.StreamWriter) error {
				_, err := fmt.Fprintln(w, "the completion marker is in an HTTP/2 trailing HEADERS frame")
				return err
			})
		})
		app.Get("/stream", func(c *fh.Ctx) error {
			return c.Stream(func(w *fh.StreamWriter) error {
				for i := 0; i < 10; i++ {
					if _, err := fmt.Fprintf(w, "chunk %d over %s\n", i+1, c.Header.Proto); err != nil {
						return err
					}
					time.Sleep(50 * time.Millisecond)
				}
				return nil
			})
		})
	}

	// ── 1. Cleartext HTTP — h2c (prior knowledge + upgrade) ────────────────

	appH2C := fh.New(fh.Config{
		ReadTimeout:          10 * time.Second,
		WriteTimeout:         10 * time.Second,
		IdleTimeout:          60 * time.Second,
		MaxRequestBodySize:   4 << 20,
		MaxConcurrentStreams: 128,
		MaxHeaderListSize:    64 << 10,
	})
	routes(appH2C)

	go func() {
		log.Printf("h2c server listening on %s", *h2cAddr)
		if err := appH2C.Listen(*h2cAddr); err != nil {
			log.Fatal(err)
		}
	}()

	// ── 2. TLS + ALPN — negotiates h2 automatically ───────────────────────

	appTLS := fh.New(fh.Config{
		ReadTimeout:          10 * time.Second,
		WriteTimeout:         10 * time.Second,
		IdleTimeout:          60 * time.Second,
		MaxRequestBodySize:   4 << 20,
		MaxConcurrentStreams: 128,
		MaxHeaderListSize:    64 << 10,
	})
	routes(appTLS)

	cert := generateSelfSignedCert()
	ln, err := tls.Listen("tcp", *tlsAddr, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		// fasthttp's ServeTLS sets NextProtos to []string{"h2", "http/1.1"}
		// when DisableHTTP2 is false. You can also set it manually:
		NextProtos: []string{"h2", "http/1.1"},
	})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		log.Printf("TLS server listening on %s", *tlsAddr)
		if err := appTLS.Serve(ln); err != nil {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Print("shutting down")
	_ = appH2C.ShutdownWithTimeout(5 * time.Second)
	_ = appTLS.ShutdownWithTimeout(5 * time.Second)
}

type repeatingReader struct{ remaining int }

func (r *repeatingReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	for i := range p {
		p[i] = 'x'
	}
	r.remaining -= len(p)
	return len(p), nil
}

// generateSelfSignedCert creates a temporary self-signed certificate for the
// TLS example. Replace this with a real certificate in production.
func generateSelfSignedCert() tls.Certificate {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		log.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		log.Fatal(err)
	}
	return cert
}
