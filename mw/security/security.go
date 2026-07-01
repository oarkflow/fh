package security

import (
	"crypto/rand"
	"encoding/base64"
	"github.com/oarkflow/fh"
	"strconv"
	"strings"
)

const CSPNonceLocalKey = "csp_nonce"

type Config struct {
	ContentSecurityPolicy           string
	ContentSecurityPolicyReportOnly string
	CSPNonce                        bool
	CSPNonceDirectives              []string
	HSTSMaxAge                      int
	HSTSIncludeSubDomains           bool
	HSTSPreload                     bool
	FrameDeny                       bool
	ContentTypeNosniff              bool
	XSSProtection                   string
	CrossOriginOpenerPolicy         string
	CrossOriginResourcePolicy       string
	CrossOriginEmbedderPolicy       string
	ReferrerPolicy                  string
	PermissionsPolicy               string
}

var defaultConfig = Config{HSTSMaxAge: 31536000, HSTSIncludeSubDomains: true, FrameDeny: true, ContentTypeNosniff: true, XSSProtection: "0", ReferrerPolicy: "no-referrer", CrossOriginOpenerPolicy: "same-origin", CrossOriginResourcePolicy: "same-origin", PermissionsPolicy: "geolocation=(), microphone=(), camera=(), payment=(), usb=(), magnetometer=(), accelerometer=(), gyroscope=(), interest-cohort=()"}

func New(config ...Config) fh.HandlerFunc {
	cfg := defaultConfig
	if len(config) > 0 {
		cfg = config[0]
	}
	var static [][2]string
	if cfg.ContentSecurityPolicyReportOnly != "" {
		static = append(static, [2]string{"Content-Security-Policy-Report-Only", cfg.ContentSecurityPolicyReportOnly})
	}
	if cfg.FrameDeny {
		static = append(static, [2]string{"X-Frame-Options", "DENY"})
	}
	if cfg.ContentTypeNosniff {
		static = append(static, [2]string{"X-Content-Type-Options", "nosniff"})
	}
	if cfg.XSSProtection != "" {
		static = append(static, [2]string{"X-XSS-Protection", cfg.XSSProtection})
	}
	if cfg.ReferrerPolicy != "" {
		static = append(static, [2]string{"Referrer-Policy", cfg.ReferrerPolicy})
	}
	if cfg.CrossOriginOpenerPolicy != "" {
		static = append(static, [2]string{"Cross-Origin-Opener-Policy", cfg.CrossOriginOpenerPolicy})
	}
	if cfg.CrossOriginResourcePolicy != "" {
		static = append(static, [2]string{"Cross-Origin-Resource-Policy", cfg.CrossOriginResourcePolicy})
	}
	if cfg.CrossOriginEmbedderPolicy != "" {
		static = append(static, [2]string{"Cross-Origin-Embedder-Policy", cfg.CrossOriginEmbedderPolicy})
	}
	if cfg.PermissionsPolicy != "" {
		static = append(static, [2]string{"Permissions-Policy", cfg.PermissionsPolicy})
	}
	if cfg.HSTSMaxAge > 0 {
		hsts := "max-age=" + strconv.Itoa(cfg.HSTSMaxAge)
		if cfg.HSTSIncludeSubDomains {
			hsts += "; includeSubDomains"
		}
		if cfg.HSTSPreload {
			hsts += "; preload"
		}
		static = append(static, [2]string{"Strict-Transport-Security", hsts})
	}
	return func(ctx fh.Ctx) error {
		for _, h := range static {
			ctx.Set(h[0], h[1])
		}
		if cfg.CSPNonce || cfg.ContentSecurityPolicy != "" {
			policy := cfg.ContentSecurityPolicy
			if cfg.CSPNonce {
				nonce, err := newNonce()
				if err != nil {
					return fh.InternalError(err)
				}
				ctx.Locals(CSPNonceLocalKey, nonce)
				policy = addNonce(policy, nonce, cfg.CSPNonceDirectives)
			}
			if policy != "" {
				ctx.Set("Content-Security-Policy", policy)
			}
		}
		return ctx.Next()
	}
}
func merge(base, o Config) Config {
	if o.ContentSecurityPolicy != "" {
		base.ContentSecurityPolicy = o.ContentSecurityPolicy
	}
	if o.ContentSecurityPolicyReportOnly != "" {
		base.ContentSecurityPolicyReportOnly = o.ContentSecurityPolicyReportOnly
	}
	if o.CSPNonce {
		base.CSPNonce = true
	}
	if o.CSPNonceDirectives != nil {
		base.CSPNonceDirectives = o.CSPNonceDirectives
	}
	if o.HSTSMaxAge != 0 {
		base.HSTSMaxAge = o.HSTSMaxAge
	}
	base.HSTSIncludeSubDomains = o.HSTSIncludeSubDomains || base.HSTSIncludeSubDomains
	base.HSTSPreload = o.HSTSPreload
	if o.FrameDeny {
		base.FrameDeny = true
	}
	if o.ContentTypeNosniff {
		base.ContentTypeNosniff = true
	}
	if o.XSSProtection != "" {
		base.XSSProtection = o.XSSProtection
	}
	if o.CrossOriginOpenerPolicy != "" {
		base.CrossOriginOpenerPolicy = o.CrossOriginOpenerPolicy
	}
	if o.CrossOriginResourcePolicy != "" {
		base.CrossOriginResourcePolicy = o.CrossOriginResourcePolicy
	}
	if o.CrossOriginEmbedderPolicy != "" {
		base.CrossOriginEmbedderPolicy = o.CrossOriginEmbedderPolicy
	}
	if o.ReferrerPolicy != "" {
		base.ReferrerPolicy = o.ReferrerPolicy
	}
	if o.PermissionsPolicy != "" {
		base.PermissionsPolicy = o.PermissionsPolicy
	}
	return base
}
func newNonce() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}
func addNonce(policy, nonce string, dirs []string) string {
	if policy == "" {
		policy = "default-src 'self'; script-src 'self'; style-src 'self'"
	}
	if len(dirs) == 0 {
		dirs = []string{"script-src", "style-src"}
	}
	parts := strings.Split(policy, ";")
	for i, p := range parts {
		name := strings.Fields(strings.TrimSpace(p))
		if len(name) == 0 {
			continue
		}
		for _, d := range dirs {
			if name[0] == d && !strings.Contains(p, "'nonce-") {
				parts[i] = strings.TrimSpace(p) + " 'nonce-" + nonce + "'"
			}
		}
	}
	return strings.Join(parts, "; ")
}
