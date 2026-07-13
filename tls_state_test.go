package fh

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	stdhttp "net/http"
	"testing"
	"time"
)

func TestHTTP1TLSStateAvailableWhenHTTP2Disabled(t *testing.T) {
	cert := testTLSCertificate(t)
	app := NewFast(WithDisableHTTP2(true), WithDisablePanicRecovery(true))
	app.Get("/tls", func(c Ctx) error {
		state, ok := RequestTLSState(c)
		if !ok || state.Version < tls.VersionTLS12 {
			return c.Status(StatusInternalServerError).SendString("missing TLS state")
		}
		return c.SendString("tls-state-ok")
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = app.ServeTLS(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	}()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })

	transport := &stdhttp.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	defer transport.CloseIdleConnections()
	client := &stdhttp.Client{Transport: transport, Timeout: 3 * time.Second}
	resp, err := client.Get("https://" + ln.Addr().String() + "/tls")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProtoMajor != 1 || resp.StatusCode != StatusOK || string(body) != "tls-state-ok" {
		t.Fatalf("proto=%s status=%d body=%q", resp.Proto, resp.StatusCode, body)
	}
}

func TestNewServerTLSConfigSecureDefaults(t *testing.T) {
	cert := testTLSCertificate(t)
	cfg, err := NewServerTLSConfig(ServerTLSOptions{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("MinVersion=%x, want TLS 1.3", cfg.MinVersion)
	}
	if cfg.ClientAuth != tls.NoClientCert {
		t.Fatalf("ClientAuth=%v", cfg.ClientAuth)
	}
	if _, err := NewServerTLSConfig(ServerTLSOptions{Certificates: []tls.Certificate{cert}, RequireClientCertificate: true}); err == nil {
		t.Fatal("expected missing client CA error")
	}
}

func TestMutualTLSVerifiedChainAvailableToHandler(t *testing.T) {
	serverCert, clientCert, roots := testMutualTLSCertificates(t)
	serverTLS, err := NewServerTLSConfig(ServerTLSOptions{
		Certificates:             []tls.Certificate{serverCert},
		ClientCAs:                roots,
		RequireClientCertificate: true,
		MinVersion:               tls.VersionTLS12,
	})
	if err != nil {
		t.Fatal(err)
	}
	app := NewFast(WithDisableHTTP2(true), WithDisablePanicRecovery(true))
	app.Get("/mtls", func(c Ctx) error {
		state, ok := RequestTLSState(c)
		if !ok || len(state.VerifiedChains) == 0 || state.VerifiedChains[0][0].Subject.CommonName != "test-client" {
			return c.Status(StatusUnauthorized).SendString("missing verified client")
		}
		return c.SendString("verified")
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.ServeTLS(ln, serverTLS) }()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })
	transport := &stdhttp.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      roots,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS12,
	}}
	defer transport.CloseIdleConnections()
	client := &stdhttp.Client{Transport: transport, Timeout: 3 * time.Second}
	resp, err := client.Get("https://" + ln.Addr().String() + "/mtls")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != StatusOK || string(body) != "verified" {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
}

func testMutualTLSCertificates(t *testing.T) (server, client tls.Certificate, roots *x509.CertPool) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(100),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(serial int64, commonName string, usage x509.ExtKeyUsage) tls.Certificate {
		key, keyErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: commonName},
			NotBefore:    now.Add(-time.Hour),
			NotAfter:     now.Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		}
		if usage == x509.ExtKeyUsageServerAuth {
			template.DNSNames = []string{"localhost"}
			template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		}
		der, certErr := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if certErr != nil {
			t.Fatal(certErr)
		}
		return tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: key}
	}
	roots = x509.NewCertPool()
	roots.AddCert(ca)
	return issue(101, "localhost", x509.ExtKeyUsageServerAuth), issue(102, "test-client", x509.ExtKeyUsageClientAuth), roots
}
