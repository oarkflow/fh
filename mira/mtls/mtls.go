// Package mtls loads production PEM credentials into the TLS 1.3 mutual-auth
// configurations that mira.ListenTLS and mira.DialTLS require. For hot
// certificate reload under systemd/SIGHUP see internal/certconfig use in
// cmd/mira-tls-server.
package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func pool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("mtls: %s contains no certificates", path)
	}
	return p, nil
}

// LoadServer returns a server config that requires client certificates signed
// by the CA in caFile.
func LoadServer(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	p, err := pool(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    p,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
	}, nil
}

// LoadClient returns a client config that presents certFile and verifies the
// server against caFile and serverName.
func LoadClient(certFile, keyFile, caFile, serverName string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	p, err := pool(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      p,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
	}, nil
}
