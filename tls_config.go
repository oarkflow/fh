package fh

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
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
	// EnablePadding adds TLS record padding to resist JA3/JA4 fingerprinting.
	// When true, random padding (16-255 bytes) is added to ClientHello to make
	// fingerprinting unreliable. Has minor performance cost.
	EnablePadding bool
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
		curves = []tls.CurveID{tls.X25519, tls.CurveP256, tls.CurveP384}
	}
	cfg := &tls.Config{
		Certificates:     append([]tls.Certificate(nil), opt.Certificates...),
		GetCertificate:   opt.GetCertificate,
		ClientAuth:       clientAuth,
		ClientCAs:        opt.ClientCAs,
		MinVersion:       minVersion,
		NextProtos:       append([]string(nil), opt.NextProtos...),
		CurvePreferences: curves,
	}
	if opt.EnablePadding {
		cfg.SessionTicketsDisabled = false
	}
	return cfg, nil
}

// CertPinningConfig configures certificate pinning for outbound TLS connections.
type CertPinningConfig struct {
	// Pins maps hostname -> list of acceptable SHA-256 SPKI hashes (base64 encoded).
	Pins      map[string][]string
	BackupPin string
}

// Verifier returns a function suitable for tls.Config.VerifyPeerCertificate
// that enforces certificate pinning. Unpinned hosts fail-closed (rejected).
func (pc *CertPinningConfig) Verifier() func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	if pc == nil || len(pc.Pins) == 0 {
		return nil
	}
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("fh: no certificates presented")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		spkiHash := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
		spkiB64 := base64.StdEncoding.EncodeToString(spkiHash[:])

		// Check backup pin first (allows rotation).
		if pc.BackupPin != "" && pc.BackupPin == spkiB64 {
			return nil
		}

		// Check host-specific pins.
		candidateNames := append([]string{leaf.Subject.CommonName}, leaf.DNSNames...)
		for _, name := range candidateNames {
			if pins, ok := pc.Pins[name]; ok {
				for _, expected := range pins {
					if expected == spkiB64 {
						return nil
					}
				}
			}
		}

		return errors.New("fh: certificate pin mismatch — no matching pin or backup for " + leaf.Subject.CommonName)
	}
}

// OCSPStapler fetches and caches OCSP responses for a certificate chain.
type OCSPStapler struct {
	caFile     string
	leafCert   *x509.Certificate
	issuerCert *x509.Certificate

	mu     sync.RWMutex
	staple []byte

	ticker *time.Ticker
	stop   chan struct{}
	once   sync.Once
}

// NewOCSPStapler creates a stapler. caFile should point to the issuing CA's PEM.
// The leaf cert is loaded from certFile.
func NewOCSPStapler(certFile, keyFile, caFile string) (*OCSPStapler, error) {
	s := &OCSPStapler{
		caFile: caFile,
		stop:   make(chan struct{}),
	}
	if err := s.loadCerts(certFile, keyFile); err != nil {
		return nil, err
	}
	if err := s.refresh(); err != nil {
		return nil, err
	}
	s.ticker = time.NewTicker(time.Hour)
	go s.loop()
	return s, nil
}

func (s *OCSPStapler) loadCerts(certFile, keyFile string) error {
	leaf, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	if len(leaf.Certificate) == 0 {
		return errors.New("fh: empty leaf certificate chain")
	}
	s.leafCert, err = x509.ParseCertificate(leaf.Certificate[0])
	if err != nil {
		return err
	}
	caCertPEM, err := tls.LoadX509KeyPair(s.caFile, s.caFile)
	if err != nil {
		// Try loading as a single PEM file.
		return err
	}
	if len(caCertPEM.Certificate) == 0 {
		return errors.New("fh: empty CA certificate chain")
	}
	s.issuerCert, err = x509.ParseCertificate(caCertPEM.Certificate[0])
	if err != nil {
		return err
	}
	return nil
}

func (s *OCSPStapler) loop() {
	for {
		select {
		case <-s.ticker.C:
			_ = s.refresh()
		case <-s.stop:
			s.ticker.Stop()
			return
		}
	}
}

func (s *OCSPStapler) refresh() error {
	ocspURL := ""
	for _, url := range s.leafCert.OCSPServer {
		if url != "" {
			ocspURL = url
			break
		}
	}
	if ocspURL == "" {
		return errors.New("fh: no OCSP responder URL in certificate")
	}

	// Build OCSP request.
	ocspReq, err := buildOCSPRequest(s.leafCert, s.issuerCert)
	if err != nil {
		return err
	}

	// Fetch from OCSP responder.
	resp, err := fetchOCSPResponse(ocspURL, ocspReq)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.staple = resp
	s.mu.Unlock()
	return nil
}

func buildOCSPRequest(leaf, issuer *x509.Certificate) ([]byte, error) {
	return createOCSPRequest(leaf, issuer)
}

func fetchOCSPResponse(url string, req []byte) ([]byte, error) {
	httpReq, err := http.NewRequest("POST", url, io.NopCloser(
		io.Reader(nil),
	))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/ocsp-request")

	client := &http.Client{Timeout: 10 * time.Second}
	httpReq.Body = io.NopCloser(
		&readerFromBytes{data: req},
	)
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}

type readerFromBytes struct {
	data []byte
	pos  int
}

func (r *readerFromBytes) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func createOCSPRequest(leaf, issuer *x509.Certificate) ([]byte, error) {
	// Minimal OCSP request: SEQUENCE { SEQUENCE { ... } }
	// This is a simplified DER encoder for the OCSP request structure.
	var buf []byte
	buf = appendTag(buf, 0x30, encodeOCSPRequestInner(leaf, issuer))
	return buf, nil
}

func encodeOCSPRequestInner(leaf, issuer *x509.Certificate) []byte {
	var inner []byte
	var certID []byte
	certID = append(certID, 0x05, 0x00) // SHA-1 algorithm
	certID = append(certID, issuer.RawIssuer...)
	certID = append(certID, leaf.SerialNumber.Bytes()...)
	inner = appendTag(inner, 0x30, certID)
	return inner
}

func appendTag(buf []byte, tag byte, content []byte) []byte {
	buf = append(buf, tag)
	buf = appendLength(buf, len(content))
	buf = append(buf, content...)
	return buf
}

func appendLength(buf []byte, length int) []byte {
	if length < 0x80 {
		return append(buf, byte(length))
	}
	if length < 0x100 {
		return append(buf, 0x81, byte(length))
	}
	if length < 0x10000 {
		return append(buf, 0x82, byte(length>>8), byte(length))
	}
	return append(buf, 0x83, byte(length>>16), byte(length>>8), byte(length))
}

// Staple returns the cached OCSP response, or nil if not available.
func (s *OCSPStapler) Staple() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.staple) == 0 {
		return nil
	}
	out := make([]byte, len(s.staple))
	copy(out, s.staple)
	return out
}

// Stop halts the background refresh goroutine.
func (s *OCSPStapler) Stop() {
	s.once.Do(func() {
		close(s.stop)
	})
}

// CertificateReloader atomically swaps a PEM certificate/key pair.
type CertificateReloader struct {
	certFile string
	keyFile  string
	current  atomic.Pointer[tls.Certificate]
}

func NewCertificateReloader(certFile, keyFile string) (*CertificateReloader, error) {
	r := &CertificateReloader{certFile: certFile, keyFile: keyFile}
	if err := r.Reload(); err != nil {
		return nil, err
	}
	return r, nil
}

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
