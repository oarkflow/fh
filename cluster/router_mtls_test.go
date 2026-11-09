package cluster

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRoutedForwardingOverMutualTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	caKey, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "cluster root"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true}
	caDER, e := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if e != nil {
		t.Fatal(e)
	}
	caCert, e := x509.ParseCertificate(caDER)
	if e != nil {
		t.Fatal(e)
	}
	issue := func(cn string) tls.Certificate {
		t.Helper()
		priv, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		tpl := &x509.Certificate{SerialNumber: big.NewInt(int64(len(cn) + 100)), Subject: pkix.Name{CommonName: cn}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}}
		der, e := x509.CreateCertificate(rand.Reader, tpl, caCert, &priv.PublicKey, caKey)
		if e != nil {
			t.Fatal(e)
		}
		return tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: priv}
	}
	clientCert := issue("node-a")
	serverCert := issue("node-b")
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	store := NewMemoryStore()
	// One logical identity belongs to b despite preferred hash membership;
	// ownership is decided by the authoritative fence, not a routing hint.
	lease, e := store.Acquire(ctx, "identity-1", "node-b", 30*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	var seen Lease
	b := &Router{NodeID: "node-b", Members: []string{"node-b"}, Store: store, TTL: 3 * time.Second, Handler: func(_ context.Context, l Lease, op string, body []byte) ([]byte, error) {
		seen = l
		return append([]byte("node-b:"), body...), nil
	}}
	// AcquireManaged by b increments the fence to establish local authority.
	_ = store.Release(ctx, lease)
	if e := b.Acquire(ctx, "identity-1"); e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	b.Forwarder = &AuthenticatedForwarder{Peers: map[string]string{"node-a": "https://node-a.invalid"}}
	testServer := httptest.NewUnstartedServer(b.ForwardHandler())
	testServer.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS13}
	testServer.StartTLS()
	defer testServer.Close()
	a := &Router{NodeID: "node-a", Members: []string{"node-a", "node-b"}, Store: store, TTL: 3 * time.Second, Handler: func(context.Context, Lease, string, []byte) ([]byte, error) {
		t.Fatal("stale local handler invoked")
		return nil, nil
	}}
	a.Forwarder = &AuthenticatedForwarder{NodeID: "node-a", Store: store, Peers: map[string]string{"node-b": testServer.URL}, Client: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{clientCert}, RootCAs: pool, MinVersion: tls.VersionTLS13}}}}
	result, e := a.Route(ctx, "identity-1", "operation-007", []byte("payload"))
	if e != nil || !bytes.Equal(result, []byte("node-b:payload")) {
		t.Fatalf("forwarded result %q: %v", result, e)
	}
	current, e := store.Current(ctx, "identity-1")
	if e != nil || seen.Fence != current.Fence {
		t.Fatalf("unfenced handler %+v, current %+v", seen, current)
	}
	_ = b.Close()
	if _, e = a.Route(ctx, "identity-1", "operation-008", nil); e == nil {
		t.Fatal("forwarded stale owner")
	}
}
