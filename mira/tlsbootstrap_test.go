package mira

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"github.com/oarkflow/fh/protocol"
	"math/big"
	"net"
	"testing"
	"time"
)

func testingTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caCert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "MIRA test root"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDer, err := x509.CreateCertificate(rand.Reader, caCert, caCert, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDer)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	mint := func(cn string, usage x509.ExtKeyUsage) tls.Certificate {
		key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(len(cn) + 15)), Subject: pkix.Name{CommonName: cn}, DNSNames: []string{cn}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		der, e := x509.CreateCertificate(rand.Reader, leaf, ca, key.Public(), caKey)
		if e != nil {
			t.Fatal(e)
		}
		return tls.Certificate{Certificate: [][]byte{der, caDer}, PrivateKey: key}
	}
	sc := &tls.Config{Certificates: []tls.Certificate{mint("server.local", x509.ExtKeyUsageServerAuth)}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	cc := &tls.Config{Certificates: []tls.Certificate{mint("device-1", x509.ExtKeyUsageClientAuth)}, RootCAs: pool, ServerName: "server.local", ClientSessionCache: tls.NewLRUClientSessionCache(8)}
	return sc, cc
}
func TestTLSBootstrappedUDPFullDuplex(t *testing.T) {
	sc, cc := testingTLS(t)
	server, err := ListenTLS("127.0.0.1:0", "127.0.0.1:0", sc, Options{HandshakeTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	server.SetChannelAuthorizer(func(id string, c protocol.ChannelConfig) bool { return id == "device-1" })
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cli, err := DialTLS(ctx, server.TLSAddr().String(), server.Addr().String(), cc, Options{HandshakeTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal("TLS+UDP bootstrap", err)
	}
	defer cli.Close(context.Background())
	ss, err := server.Accept(ctx)
	if err != nil {
		t.Fatal("UDP path validation", err)
	}
	defer ss.Close(context.Background())
	if ss.Identity() != "device-1" {
		t.Fatalf("identity: %q", ss.Identity())
	}
	c, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	s, err := ss.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("custom MIRA UDP"), 3000)
	id, err := c.Send(ctx, payload)
	if err != nil {
		t.Fatal(err)
	}
	received, err := s.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received.Payload, payload) {
		t.Fatal("corrupted encrypted UDP message")
	}
	if _, err := c.AwaitReceipt(ctx, id, protocol.AckTransport); err != nil {
		t.Fatal(err)
	}
	push, err := ss.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	recv, err := cli.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := push.Send(ctx, []byte("server initiated")); err != nil {
		t.Fatal(err)
	}
	msg, err := recv.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Payload) != "server initiated" {
		t.Fatalf("server push %q", msg.Payload)
	}
}
func TestTLSRejectsUnauthenticatedClients(t *testing.T) {
	sc, cc := testingTLS(t)
	server, err := ListenTLS("127.0.0.1:0", "127.0.0.1:0", sc, Options{HandshakeTimeout: 600 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	bad := cc.Clone()
	bad.Certificates = nil
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = DialTLS(ctx, server.TLSAddr().String(), server.Addr().String(), bad, Options{HandshakeTimeout: 600 * time.Millisecond})
	if err == nil {
		t.Fatal("missing mTLS credential must fail")
	}
	bad = cc.Clone()
	bad.ServerName = "wrong.local"
	_, err = DialTLS(ctx, server.TLSAddr().String(), server.Addr().String(), bad, Options{HandshakeTimeout: 600 * time.Millisecond})
	if err == nil {
		t.Fatal("server name mismatch must fail")
	}
	// Plain UDP is not a supported client authentication path on this listener.
	_, err = Dial(ctx, server.Addr().String(), bytes.Repeat([]byte{5}, 32), Options{HandshakeTimeout: 200 * time.Millisecond})
	if err == nil {
		t.Fatal("legacy handshake accepted on TLS-only server")
	}
	if _, ok := server.Addr().(*net.UDPAddr); !ok {
		t.Fatal("expected native UDP")
	}
}
