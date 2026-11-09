package certconfig

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReloaderAtomicUpdateAndFailureRollback(t *testing.T) {
	dir := t.TempDir()
	cert := filepath.Join(dir, "server.pem")
	key := filepath.Join(dir, "server.key")
	ca := filepath.Join(dir, "root.pem")
	issue := func(serial int64) {
		t.Helper()
		k, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		tpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "test-server"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true}
		der, e := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
		if e != nil {
			t.Fatal(e)
		}
		keyDER, e := x509.MarshalECPrivateKey(k)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600); e != nil {
			t.Fatal(e)
		}
	}
	issue(101)
	reload, e := NewServerReloader(cert, key, ca)
	if e != nil {
		t.Fatal(e)
	}
	before, _ := x509.ParseCertificate(reload.Config().Certificates[0].Certificate[0])
	if before.SerialNumber.Int64() != 101 {
		t.Fatal("wrong initial certificate")
	}
	issue(202)
	if e = reload.Reload(); e != nil {
		t.Fatal(e)
	}
	after, _ := x509.ParseCertificate(reload.Config().Certificates[0].Certificate[0])
	if after.SerialNumber.Int64() != 202 {
		t.Fatal("not rotated")
	}
	if e = os.WriteFile(key, []byte("invalid key"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = reload.Reload(); e == nil {
		t.Fatal("invalid key accepted")
	}
	preserved, _ := x509.ParseCertificate(reload.Config().Certificates[0].Certificate[0])
	if preserved.SerialNumber.Int64() != 202 {
		t.Fatal("invalid reload replaced last-known-good")
	}
}
