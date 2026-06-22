package fh

import (
	"context"
	"crypto/tls"
	"net"
	"os/exec"
	"testing"
	"time"
)

func TestH2SpecIntegration(t *testing.T) {
	if _, err := exec.LookPath("h2spec"); err != nil {
		t.Skip("h2spec not installed; run: go install github.com/summerwind/h2spec/cmd/h2spec@latest")
	}

	cert := testTLSCertificate(t)
	app := New()
	app.Get("/", func(c *Ctx) error { return c.SendString("ok") })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = app.ServeTLS(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	}()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })

	addr := ln.Addr().String()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "h2spec",
		"-h", host,
		"-p", portStr,
		"-t",
		"-k",
		"-P", "/",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("h2spec output:\n%s", out)
		t.Fatalf("h2spec exited with error: %v", err)
	}
	t.Logf("h2spec output:\n%s", out)
}
