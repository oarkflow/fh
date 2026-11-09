package delegation_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/oarkflow/fh/delegation"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
)

func certificates(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rootTemplate := &x509.Certificate{SerialNumber: big.NewInt(100), Subject: pkix.Name{CommonName: "delegation-test-CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, rootKey.Public(), rootKey)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(root)
	mint := func(name string, usage x509.ExtKeyUsage) tls.Certificate {
		private, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(int64(len(name) + 10)), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		leaf, e := x509.CreateCertificate(rand.Reader, template, root, private.Public(), rootKey)
		if e != nil {
			t.Fatal(e)
		}
		return tls.Certificate{Certificate: [][]byte{leaf, der}, PrivateKey: private}
	}
	return &tls.Config{Certificates: []tls.Certificate{mint("server.local", x509.ExtKeyUsageServerAuth)}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}, &tls.Config{Certificates: []tls.Certificate{mint("gateway-service", x509.ExtKeyUsageClientAuth)}, RootCAs: pool, ServerName: "server.local"}
}

func TestDelegationAcrossRealEncryptedUDPChannel(t *testing.T) {
	sc, cc := certificates(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	server, err := mira.ListenTLS("127.0.0.1:0", "127.0.0.1:0", sc, mira.Options{HandshakeTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	server.SetChannelAuthorizer(func(id string, _ protocol.ChannelConfig) bool { return id == "gateway-service" })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errors := make(chan error, 1)
	go func() {
		ss, e := server.Accept(ctx)
		if e != nil {
			errors <- e
			return
		}
		defer ss.Close(context.Background())
		ch, e := ss.AcceptChannel(ctx)
		if e != nil {
			errors <- e
			return
		}
		verifier := &delegation.Verifier{Audience: "mira-upstream", PublicKey: pub}
		subject, e := verifier.Accept(ctx, ch, ss.ID())
		if e != nil {
			errors <- e
			return
		}
		if subject != "alice" {
			errors <- delegation.ErrInvalid
			return
		}
		payload, e := ch.Receive(ctx)
		if e != nil {
			errors <- e
			return
		}
		id, e := ch.Send(ctx, append([]byte(subject+":"), payload.Payload...))
		if e == nil {
			_, e = ch.AwaitReceipt(ctx, id, protocol.AckTransport)
		}
		errors <- e
	}()
	cli, err := mira.DialTLS(ctx, server.TLSAddr().String(), server.Addr().String(), cc, mira.Options{HandshakeTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close(context.Background())
	ch, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	if err := delegation.Prove(ctx, ch, cli.ID(), priv, "mira-upstream", "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Send(ctx, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	response, err := ch.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Payload) != "alice:hello" {
		t.Fatalf("wrong bound principal: %s", response.Payload)
	}
	if err := <-errors; err != nil {
		t.Fatal(err)
	}
}

// The new key-identified v2 delegated proof must work over the authenticated
// native UDP channel, not merely through the local signature unit tests.
func TestNamedDelegationAcrossRealEncryptedUDPChannel(t *testing.T) {
	sc, cc := certificates(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	server, err := mira.ListenTLS("127.0.0.1:0", "127.0.0.1:0", sc, mira.Options{HandshakeTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	server.SetChannelAuthorizer(func(id string, _ protocol.ChannelConfig) bool { return id == "gateway-service" })
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		sess, e := server.Accept(ctx)
		if e != nil {
			done <- e
			return
		}
		defer sess.Close(context.Background())
		ch, e := sess.AcceptChannel(ctx)
		if e != nil {
			done <- e
			return
		}
		verifier := &delegation.Verifier{Audience: "mira-upstream"}
		if e = verifier.ReplaceKeys(map[string]ed25519.PublicKey{"v7-key": pub}); e != nil {
			done <- e
			return
		}
		subject, e := verifier.Accept(ctx, ch, sess.ID())
		if e != nil {
			done <- e
			return
		}
		if subject != "alice" {
			done <- delegation.ErrInvalid
			return
		}
		msg, e := ch.Receive(ctx)
		if e != nil {
			done <- e
			return
		}
		mid, e := ch.Send(ctx, append([]byte(subject+":"), msg.Payload...))
		if e == nil {
			_, e = ch.AwaitReceipt(ctx, mid, protocol.AckTransport)
		}
		done <- e
	}()
	cli, err := mira.DialTLS(ctx, server.TLSAddr().String(), server.Addr().String(), cc, mira.Options{HandshakeTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close(context.Background())
	ch, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	if err = delegation.ProveNamed(ctx, ch, cli.ID(), priv, "v7-key", "mira-upstream", "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err = ch.Send(ctx, []byte("rotated")); err != nil {
		t.Fatal(err)
	}
	reply, err := ch.Receive(ctx)
	if err != nil || string(reply.Payload) != "alice:rotated" {
		t.Fatalf("bound message %q: %v", reply.Payload, err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}
