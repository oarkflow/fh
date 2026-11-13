package mtls

import (
	"context"
	"crypto/x509"
	"strings"

	"github.com/oarkflow/fh"
)

type ErrorHandler func(fh.Ctx, string) error
type Config struct {
	Required bool
	// AllowUnverified permits PeerCertificates when the TLS stack did not build
	// a verified chain. Leave false for secure deployments. It exists for
	// private-PKI applications that perform all verification in Verify.
	AllowUnverified bool
	AllowedSubjects []string
	AllowedIssuers  []string
	// Verify must perform full certificate validation when AllowUnverified is
	// enabled or certificates are supplied through WithPeerCertificates.
	Verify func(fh.Ctx, []*x509.Certificate) bool
	Error  ErrorHandler
	Next   func(fh.Ctx) bool
}
type peerCertificatesKey struct{}

// WithPeerCertificates supplies certificates from a trusted transport
// adapter. New still requires Config.Verify to accept these certificates;
// placing raw peer certificates in a context does not prove their identity.
func WithPeerCertificates(ctx context.Context, certs []*x509.Certificate) context.Context {
	return context.WithValue(ctx, peerCertificatesKey{}, certs)
}
func New(cfg Config) fh.HandlerFunc {
	if cfg.Error == nil {
		cfg.Error = func(c fh.Ctx, msg string) error {
			return c.Status(fh.StatusUnauthorized).JSON(fh.Map{"error": "mtls_invalid", "message": msg})
		}
	}
	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		certs, injected := c.Context().Value(peerCertificatesKey{}).([]*x509.Certificate)
		if state, ok := fh.RequestTLSState(c); ok {
			// TLS state is authoritative. Never fall back to adapter-injected
			// certificates when this connection presented no client cert.
			certs = nil
			switch {
			case len(state.VerifiedChains) > 0:
				certs = state.VerifiedChains[0]
			case len(state.PeerCertificates) > 0 && cfg.AllowUnverified && cfg.Verify != nil:
				certs = state.PeerCertificates
			case len(state.PeerCertificates) > 0:
				return cfg.Error(c, "client certificate is not verified")
			}
		} else if injected && len(certs) > 0 && cfg.Verify == nil {
			return cfg.Error(c, "injected client certificates require a verifier")
		}
		if len(certs) == 0 {
			if cfg.Required {
				return cfg.Error(c, "client certificate required")
			}
			return c.Next()
		}
		for _, cert := range certs {
			if cert == nil {
				return cfg.Error(c, "client certificate is invalid")
			}
		}
		if !allowed(certs[0].Subject.CommonName, cfg.AllowedSubjects) {
			return cfg.Error(c, "client subject denied")
		}
		if !allowed(certs[0].Issuer.CommonName, cfg.AllowedIssuers) {
			return cfg.Error(c, "client issuer denied")
		}
		if cfg.Verify != nil && !cfg.Verify(c, certs) {
			return cfg.Error(c, "client certificate rejected")
		}
		c.Locals("mtls_subject", certs[0].Subject.CommonName)
		return c.Next()
	}
}
func allowed(value string, allow []string) bool {
	if len(allow) == 0 {
		return true
	}
	for _, a := range allow {
		if strings.EqualFold(strings.TrimSpace(a), value) {
			return true
		}
	}
	return false
}
