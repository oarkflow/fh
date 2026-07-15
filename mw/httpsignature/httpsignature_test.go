package httpsignature

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	protocol "github.com/oarkflow/fh/pkg/httpsignature"
)

func TestSignedResponseIntegrationAndNonceReplay(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	origin := "http://" + listener.Addr().String()
	signer, err := New(Config{PrivateKey: privateKey, KeyID: "key-1", Origin: origin})
	if err != nil {
		t.Fatal(err)
	}
	app := fh.NewFast(fh.WithDisableHTTP2(true), fh.WithDisablePanicRecovery(true))
	app.Use(signer)
	// A downstream transform must run before signing. This guards against
	// signing bytes that are subsequently changed before transmission.
	app.Use(func(c fh.Ctx) error {
		c.AddBodyTransform(func(body []byte) ([]byte, error) {
			return append(body, '\n'), nil
		})
		return c.Next()
	})
	app.Get("/data", func(c fh.Ctx) error { return c.Type("application/json").SendString(`{"ok":true}`) })
	go func() { _ = app.Serve(listener) }()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })

	request, _ := http.NewRequest(http.MethodGet, origin+"/data", nil)
	client := protocol.Client{
		HTTPClient: &http.Client{Timeout: 3 * time.Second},
		Verifier:   protocol.Verifier{KeyID: "key-1", PublicKey: publicKey},
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || string(body) != "{\"ok\":true}\n" {
		t.Fatalf("status=%d body=%q", response.StatusCode, body)
	}
	if response.Header.Get(protocol.HeaderSignatureInput) == "" || response.Header.Get(protocol.HeaderSignature) == "" || response.Header.Get(protocol.HeaderContentDigest) == "" {
		t.Fatal("required RFC 9421 response fields are missing")
	}

	nonce, _ := protocol.NewNonce()
	accept, _ := protocol.FormatAcceptSignature(protocol.DefaultLabel, nonce, "key-1")
	do := func() int {
		req, _ := http.NewRequest(http.MethodGet, origin+"/data", nil)
		req.Header.Set(protocol.HeaderAcceptSignature, accept)
		resp, doErr := http.DefaultClient.Do(req)
		if doErr != nil {
			t.Fatal(doErr)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	if status := do(); status != http.StatusOK {
		t.Fatalf("first nonce status=%d", status)
	}
	if status := do(); status != http.StatusConflict {
		t.Fatalf("replayed nonce status=%d", status)
	}

	unsigned, err := http.Get(origin + "/data")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, unsigned.Body)
	unsigned.Body.Close()
	if unsigned.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing negotiation status=%d", unsigned.StatusCode)
	}
	if unsigned.Header.Get(protocol.HeaderSignature) != "" {
		t.Fatal("unnegotiated error unexpectedly included a signature")
	}
}

func TestRequiresNegotiatedSignature(t *testing.T) {
	_, privateKey, _ := protocol.GenerateKey()
	if _, err := New(Config{PrivateKey: privateKey, KeyID: "key-1", Origin: "https://api.example.test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{PrivateKey: privateKey, KeyID: "key-1", Origin: "http://api.example.test"}); err == nil {
		t.Fatal("non-loopback HTTP origin was accepted")
	}
}

func TestAllowedOriginSelectsRequestHost(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, _ := protocol.GenerateKey()
	port := listener.Addr().(*net.TCPAddr).Port
	canonical := "http://127.0.0.1:" + strconv.Itoa(port)
	alternative := "http://localhost:" + strconv.Itoa(port)
	signer, err := New(Config{
		PrivateKey: privateKey, KeyID: "key-1", Origin: canonical,
		AllowedOrigins: []string{alternative},
	})
	if err != nil {
		t.Fatal(err)
	}
	app := fh.NewFast(fh.WithDisableHTTP2(true), fh.WithDisablePanicRecovery(true))
	app.Use(signer)
	app.Get("/data", func(c fh.Ctx) error { return c.JSON(fh.Map{"ok": true}) })
	go func() { _ = app.Serve(listener) }()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })

	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
	}}
	request, _ := http.NewRequest(http.MethodGet, alternative+"/data", nil)
	client := protocol.Client{
		HTTPClient: &http.Client{Transport: transport, Timeout: 3 * time.Second},
		Verifier:   protocol.Verifier{KeyID: "key-1", PublicKey: publicKey},
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("alternative origin status=%d", response.StatusCode)
	}
}
