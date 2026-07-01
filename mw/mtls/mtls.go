package mtls

import (
	"context"
	"crypto/x509"
	"github.com/oarkflow/fh"
	"strings"
)

type ErrorHandler func(fh.Ctx, string) error
type Config struct {
	Required        bool
	AllowedSubjects []string
	AllowedIssuers  []string
	Verify          func(fh.Ctx, []*x509.Certificate) bool
	Error           ErrorHandler
	Next            func(fh.Ctx) bool
}
type peerCertificatesKey struct{}

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
		certs, _ := c.Context().Value(peerCertificatesKey{}).([]*x509.Certificate)
		if len(certs) == 0 {
			if cfg.Required {
				return cfg.Error(c, "client certificate required")
			}
			return c.Next()
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
