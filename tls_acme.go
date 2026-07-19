package fh

import (
	"crypto/tls"
	"errors"
	"net"

	"golang.org/x/crypto/acme/autocert"
)

// ACMEOptions configures automatic certificate issuance and renewal via an
// ACME CA (Let's Encrypt by default, through golang.org/x/crypto/acme/autocert
// — already a dependency of this module for OCSP stapling in tls_config.go).
//
// fh has no net/http dependency, so it deliberately supports only the
// tls-alpn-01 challenge type (RFC 8737): it needs no second listener, no
// port-80 HTTP handler, and no acme.ALPNProto plumbing beyond what
// Manager.TLSConfig already does. Environments that terminate TLS in front of
// fh cannot complete tls-alpn-01 and are out of scope — use a manually
// provisioned certificate with CertificateReloader instead in that case.
type ACMEOptions struct {
	// Domains is the exact set of hostnames this server is authorized to
	// request certificates for. Required.
	Domains []string
	// CacheDir persists issued certificates and account state to disk across
	// restarts. Required — without it, every restart re-issues certificates
	// and risks hitting the CA's rate limits.
	CacheDir string
	// Email is an optional contact address the CA may use to warn about
	// certificate problems.
	Email string
	// HostPolicy overrides the default autocert.HostWhitelist(Domains...)
	// policy, e.g. to allow subdomains dynamically.
	HostPolicy autocert.HostPolicy
}

// NewACMEManager builds an autocert.Manager from opt. The returned Manager's
// TLSConfig method is the intended way to obtain a *tls.Config for
// ServeTLS/ListenTLS; use ListenAutoTLS for the common case.
func NewACMEManager(opt ACMEOptions) (*autocert.Manager, error) {
	if len(opt.Domains) == 0 {
		return nil, errors.New("fh: ACMEOptions.Domains must not be empty")
	}
	if opt.CacheDir == "" {
		return nil, errors.New("fh: ACMEOptions.CacheDir is required")
	}
	hostPolicy := opt.HostPolicy
	if hostPolicy == nil {
		hostPolicy = autocert.HostWhitelist(opt.Domains...)
	}
	return &autocert.Manager{
		Cache:      autocert.DirCache(opt.CacheDir),
		HostPolicy: hostPolicy,
		Email:      opt.Email,
	}, nil
}

// ListenAutoTLS serves HTTPS on :443 with certificates issued and renewed
// automatically via ACME tls-alpn-01. It is the ACME counterpart to ListenTLS.
func (a *App) ListenAutoTLS(domains []string, cacheDir string) error {
	tlsConfig, err := a.autoTLSConfig(domains, cacheDir)
	if err != nil {
		return err
	}
	if a.cfg.Kernel.Enabled {
		return a.listenKernel(":https", tlsConfig)
	}
	ln, err := net.Listen("tcp", ":https")
	if err != nil {
		return err
	}
	return a.Serve(tls.NewListener(ln, tlsConfig))
}

// ListenAutoTLSWithGracefulShutdown is the graceful-shutdown counterpart to
// ListenAutoTLS, draining on SIGINT/SIGTERM exactly like
// ListenTLSWithGracefulShutdown.
func (a *App) ListenAutoTLSWithGracefulShutdown(domains []string, cacheDir string) error {
	tlsConfig, err := a.autoTLSConfig(domains, cacheDir)
	if err != nil {
		return err
	}
	if a.cfg.Kernel.Enabled {
		return a.runWithSignal(func() error { return a.listenKernel(":https", tlsConfig) })
	}
	ln, err := net.Listen("tcp", ":https")
	if err != nil {
		return err
	}
	return a.runWithSignal(func() error { return a.Serve(tls.NewListener(ln, tlsConfig)) })
}

func (a *App) autoTLSConfig(domains []string, cacheDir string) (*tls.Config, error) {
	mgr, err := NewACMEManager(ACMEOptions{Domains: domains, CacheDir: cacheDir})
	if err != nil {
		return nil, err
	}
	// mgr.TLSConfig() already sets GetCertificate and populates NextProtos
	// with "h2", "http/1.1", and the tls-alpn-01 ALPN identifier — since
	// prepareTLSConfig only fills NextProtos when empty, that selection is
	// preserved. MinVersion is forced to TLS 1.3 here (autocert itself leaves
	// it unset) so ListenAutoTLS matches ListenTLS's unconditional TLS-1.3
	// floor rather than only enforcing it when SecureByDefault is set.
	cfg := mgr.TLSConfig()
	cfg.MinVersion = tls.VersionTLS13
	return a.prepareTLSConfig(cfg)
}
