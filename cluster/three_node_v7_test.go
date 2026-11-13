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

// Three independent mutually-authenticated forwarding endpoints share a
// simulated lease store. This is not a PostgreSQL partition/failover test.
func TestThreeNodeMutualTLSForwardAndFencedTakeover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "MIRA cluster CA"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, root, root, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	rootCert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(rootCert)
	issue := func(name string, serial int64) tls.Certificate {
		t.Helper()
		key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		issued, e := x509.CreateCertificate(rand.Reader, template, rootCert, &key.PublicKey, rootKey)
		if e != nil {
			t.Fatal(e)
		}
		return tls.Certificate{Certificate: [][]byte{issued, der}, PrivateKey: key}
	}
	store := NewMemoryStore()
	names := []string{"node-a", "node-b", "node-c"}
	certificates := map[string]tls.Certificate{}
	for i, name := range names {
		certificates[name] = issue(name, int64(i+10))
	}
	routers := map[string]*Router{}
	endpoints := map[string]*httptest.Server{}
	for _, name := range names {
		n := name
		routers[n] = &Router{NodeID: n, Members: []string{n}, Store: store, TTL: 3 * time.Second,
			Handler: func(_ context.Context, l Lease, op string, payload []byte) ([]byte, error) {
				return append([]byte(n+":"), payload...), nil
			},
		}
		routers[n].Forwarder = &AuthenticatedForwarder{Peers: map[string]string{"node-a": "https://placeholder", "node-b": "https://placeholder", "node-c": "https://placeholder"}}
		server := httptest.NewUnstartedServer(routers[n].ForwardHandler())
		server.TLS = &tls.Config{Certificates: []tls.Certificate{certificates[n]}, MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
		server.StartTLS()
		endpoints[n] = server
		defer server.Close()
	}
	for _, name := range names {
		peerURLs := map[string]string{}
		for id, server := range endpoints {
			peerURLs[id] = server.URL
		}
		routers[name].Forwarder = &AuthenticatedForwarder{NodeID: name, Store: store, Peers: peerURLs, Client: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{certificates[name]}, RootCAs: pool, MinVersion: tls.VersionTLS13}}}}
	}
	// Forward handlers captured their own server-side Forwarder snapshot at
	// construction, whose source allowlist contains all three identities.
	if err := routers["node-b"].Acquire(ctx, "tenant-one"); err != nil {
		t.Fatal(err)
	}
	initial, err := store.Current(ctx, "tenant-one")
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"node-a", "node-c"} {
		got, e := routers[source].Route(ctx, "tenant-one", "operation-pre-failover", []byte(source))
		if e != nil || !bytes.Equal(got, []byte("node-b:"+source)) {
			t.Fatalf("%s -> b: %q %v", source, got, e)
		}
	}
	if err := routers["node-b"].Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate verified membership change: the standby becomes owner after the
	// old authority is released; its monotonic fence must be strictly higher.
	if err := routers["node-c"].Acquire(ctx, "tenant-one"); err != nil {
		t.Fatal(err)
	}
	current, err := store.Current(ctx, "tenant-one")
	if err != nil || current.Fence <= initial.Fence || current.Owner != "node-c" {
		t.Fatalf("fencing takeover: before=%+v after=%+v err=%v", initial, current, err)
	}
	got, err := routers["node-a"].Route(ctx, "tenant-one", "operation-after-failover", []byte("ok"))
	if err != nil || string(got) != "node-c:ok" {
		t.Fatalf("post failover forward %q: %v", got, err)
	}
	// The old owner's stale fence can no longer authenticate a forwarded
	// operation; the new owner is authoritative.
	_, err = routers["node-a"].Forwarder.Forward(ctx, "node-b", initial, "operation-stale-owner", []byte("attack"))
	if err == nil {
		t.Fatalf("stale fence %d was accepted", initial.Fence)
	}
	for _, router := range routers {
		_ = router.Close()
	}
}
