package fh

import (
	"bytes"
	"crypto"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ocsp"
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
		if len(verifiedChains) == 0 {
			return errors.New("fh: certificate pinning requires normal CA and hostname verification")
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
	done   chan struct{}
	once   sync.Once
}

// NewOCSPStapler creates a stapler. caFile should point to the issuing CA's PEM.
// The leaf cert is loaded from certFile.
func NewOCSPStapler(certFile, keyFile, caFile string) (*OCSPStapler, error) {
	s := &OCSPStapler{
		caFile: caFile,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
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
		// caFile may be a PEM bundle without a private key.
		// Fall back to parsing it as a raw PEM certificate.
		pemData, readErr := os.ReadFile(s.caFile)
		if readErr != nil {
			return err
		}
		block, _ := pemDecode(pemData)
		if block == nil {
			return err
		}
		s.issuerCert, err = x509.ParseCertificate(block.Bytes)
		if err != nil {
			return err
		}
		return nil
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
	defer close(s.done)
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
	parsed, err := ocsp.ParseResponseForCert(resp, s.leafCert, s.issuerCert)
	if err != nil {
		return errors.New("fh: invalid OCSP response: " + err.Error())
	}
	if parsed.Status != ocsp.Good {
		return errors.New("fh: OCSP responder did not report certificate status good")
	}
	now := time.Now()
	if parsed.ThisUpdate.After(now.Add(5 * time.Minute)) {
		return errors.New("fh: OCSP response is not yet valid")
	}
	if !parsed.NextUpdate.IsZero() && !parsed.NextUpdate.After(now) {
		return errors.New("fh: OCSP response is expired")
	}

	s.mu.Lock()
	s.staple = resp
	s.mu.Unlock()
	return nil
}

func buildOCSPRequest(leaf, issuer *x509.Certificate) ([]byte, error) {
	return ocsp.CreateRequest(leaf, issuer, &ocsp.RequestOptions{Hash: crypto.SHA256})
}

func fetchOCSPResponse(url string, req []byte) ([]byte, error) {
	parsedURL, err := neturl.Parse(url)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		return nil, errors.New("fh: invalid OCSP responder URL")
	}
	httpReq, err := http.NewRequest("POST", url, bytes.NewReader(req))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/ocsp-request")

	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("fh: OCSP responder redirects are not allowed")
		},
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, errors.New("fh: OCSP responder returned " + resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(body) > 1<<20 {
		return nil, errors.New("fh: OCSP response exceeds 1 MiB")
	}
	return body, nil
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

// Stop halts the background refresh goroutine and waits for it to exit.
func (s *OCSPStapler) Stop() {
	s.once.Do(func() { close(s.stop) })
	<-s.done
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

func pemDecode(data []byte) (*pem.Block, []byte) {
	var block *pem.Block
	rest := data
	for {
		block, rest = pem.Decode(rest)
		if block == nil {
			return nil, nil
		}
		if block.Type == "CERTIFICATE" {
			return block, rest
		}
	}
}
