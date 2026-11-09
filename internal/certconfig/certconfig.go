package certconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

func Server(certPath, keyPath, caPath string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	roots, err := readRoots(caPath)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13}, nil
}
func Client(certPath, keyPath, caPath, serverName string) (*tls.Config, error) {
	if serverName == "" {
		return nil, errors.New("certificate server name required")
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	roots, err := readRoots(caPath)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: roots, ServerName: serverName, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, ClientSessionCache: tls.NewLRUClientSessionCache(64)}, nil
}
func readRoots(path string) (*x509.CertPool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(b) {
		return nil, errors.New("no valid CA certs")
	}
	return roots, nil
}
