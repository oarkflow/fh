package fh

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"sync/atomic"
)

// ServerTLSOptions builds a hardened server-side tls.Config without coupling
// the HTTP runtime to certificate provisioning. TLS 1.3 is the default; set
// MinVersion to tls.VersionTLS12 only when compatibility requires it.
type ServerTLSOptions struct {
	Certificates              []tls.Certificate
	GetCertificate            func(*tls.ClientHelloInfo) (*tls.Certificate, error)
	ClientCAs                 *x509.CertPool
	RequireClientCertificate  bool
	VerifyClientCertIfPresent bool
	MinVersion                uint16
	NextProtos                []string
	CurvePreferences          []tls.CurveID
}

// NewServerTLSConfig returns a validated, server-side TLS configuration.
func NewServerTLSConfig(opt ServerTLSOptions) (*tls.Config, error) {
	if len(opt.Certificates) == 0 && opt.GetCertificate == nil {
		return nil, errors.New("fh: TLS requires a certificate or GetCertificate callback")
	}
	if (opt.RequireClientCertificate || opt.VerifyClientCertIfPresent) && opt.ClientCAs == nil {
		return nil, errors.New("fh: client-certificate verification requires a client CA pool")
	}
	minVersion := opt.MinVersion
	if minVersion == 0 {
		minVersion = tls.VersionTLS13
	}
	if minVersion < tls.VersionTLS12 {
		return nil, errors.New("fh: TLS MinVersion must be TLS 1.2 or newer")
	}
	clientAuth := tls.NoClientCert
	switch {
	case opt.RequireClientCertificate:
		clientAuth = tls.RequireAndVerifyClientCert
	case opt.VerifyClientCertIfPresent:
		clientAuth = tls.VerifyClientCertIfGiven
	}
	curves := append([]tls.CurveID(nil), opt.CurvePreferences...)
	if len(curves) == 0 {
		curves = []tls.CurveID{tls.X25519, tls.CurveP256}
	}
	return &tls.Config{
		Certificates:     append([]tls.Certificate(nil), opt.Certificates...),
		GetCertificate:   opt.GetCertificate,
		ClientAuth:       clientAuth,
		ClientCAs:        opt.ClientCAs,
		MinVersion:       minVersion,
		NextProtos:       append([]string(nil), opt.NextProtos...),
		CurvePreferences: curves,
	}, nil
}

// CertificateReloader atomically swaps a PEM certificate/key pair. Existing
// TLS connections continue with their negotiated certificate; new handshakes
// observe the most recently loaded pair.
type CertificateReloader struct {
	certFile string
	keyFile  string
	current  atomic.Pointer[tls.Certificate]
}

// NewCertificateReloader loads the initial certificate pair.
func NewCertificateReloader(certFile, keyFile string) (*CertificateReloader, error) {
	r := &CertificateReloader{certFile: certFile, keyFile: keyFile}
	if err := r.Reload(); err != nil {
		return nil, err
	}
	return r, nil
}

// Reload validates and atomically publishes the files currently on disk.
func (r *CertificateReloader) Reload() error {
	if r == nil {
		return errors.New("fh: nil certificate reloader")
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return err
	}
	r.current.Store(&cert)
	return nil
}

// GetCertificate implements tls.Config.GetCertificate.
func (r *CertificateReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if r == nil {
		return nil, errors.New("fh: nil certificate reloader")
	}
	cert := r.current.Load()
	if cert == nil {
		return nil, errors.New("fh: no TLS certificate loaded")
	}
	return cert, nil
}
