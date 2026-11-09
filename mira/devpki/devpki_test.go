package devpki_test

import (
	"crypto/tls"
	"net"
	"testing"

	"github.com/oarkflow/fh/mira/devpki"
)

func TestMutualTLS13Handshake(t *testing.T) {
	ca, err := devpki.New()
	if err != nil {
		t.Fatal(err)
	}
	srvCfg, _ := ca.ServerTLS()
	cliCfg, _ := ca.ClientTLS("device-9")
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srvCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	peer := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		tc := c.(*tls.Conn)
		if tc.Handshake() == nil {
			peer <- tc.ConnectionState().PeerCertificates[0].Subject.CommonName
		}
	}()
	c, err := tls.Dial("tcp", ln.Addr().String(), cliCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.ConnectionState().Version != tls.VersionTLS13 {
		t.Fatal("TLS 1.3 expected")
	}
	if got := <-peer; got != "device-9" {
		t.Fatalf("server saw identity %q", got)
	}
}

func TestForeignCARejected(t *testing.T) {
	good, _ := devpki.New()
	evil, _ := devpki.New()
	srvCfg, _ := good.ServerTLS()
	ln, _ := tls.Listen("tcp", "127.0.0.1:0", srvCfg)
	defer ln.Close()
	go func() {
		if c, err := ln.Accept(); err == nil {
			c.(*tls.Conn).Handshake()
			c.Close()
		}
	}()
	cliCfg, _ := evil.ClientTLS("intruder")
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tc := tls.Client(c, cliCfg)
	if err := tc.Handshake(); err == nil {
		// TLS 1.3 reports a rejected client certificate on the first read.
		buf := make([]byte, 1)
		if _, err = tc.Read(buf); err == nil {
			t.Fatal("a certificate from a foreign CA must not be accepted")
		}
	}
}
