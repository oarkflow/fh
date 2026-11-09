package mtls_test

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/oarkflow/fh/mira/mtls"
)

func TestLoadErrors(t *testing.T) {
	dir := t.TempDir()
	if _, err := mtls.LoadServer(filepath.Join(dir, "no.pem"), filepath.Join(dir, "no.key"), filepath.Join(dir, "no.ca")); err == nil {
		t.Fatal("missing files must fail")
	}
	bad := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(bad, []byte("not a cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := mtls.LoadClient(bad, bad, bad, "x"); err == nil {
		t.Fatal("garbage PEM must fail")
	}
}

func TestConfigsAreStrict(t *testing.T) {
	// Use the repo's dev generator output layout via a tiny self-signed pair.
	dir := t.TempDir()
	cert, key := selfSigned(t)
	cp, kp := filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
	os.WriteFile(cp, cert, 0o600)
	os.WriteFile(kp, key, 0o600)
	srv, err := mtls.LoadServer(cp, kp, cp)
	if err != nil {
		t.Fatal(err)
	}
	if srv.ClientAuth != tls.RequireAndVerifyClientCert || srv.MinVersion != tls.VersionTLS13 {
		t.Fatal("server config must require verified client certs on TLS 1.3")
	}
	cli, err := mtls.LoadClient(cp, kp, cp, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if cli.ServerName != "localhost" || cli.MinVersion != tls.VersionTLS13 || cli.InsecureSkipVerify {
		t.Fatal("client config must verify the server on TLS 1.3")
	}
}
