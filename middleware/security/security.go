package security

import (
	"strconv"

	"github.com/oarkflow/fh"
)

type Config struct {
	ContentSecurityPolicy string
	HSTSMaxAge            int
	HSTSIncludeSubDomains bool
	FrameDeny             bool
	ContentTypeNosniff    bool
	XSSProtection         string
	ReferrerPolicy        string
	PermissionsPolicy     string
}

var defaultConfig = Config{
	HSTSMaxAge:            31536000,
	HSTSIncludeSubDomains: true,
	FrameDeny:             true,
	ContentTypeNosniff:    true,
	XSSProtection:         "0",
	ReferrerPolicy:        "no-referrer",
	PermissionsPolicy:     "geolocation=(), microphone=(), camera=(), payment=(), usb=(), magnetometer=(), accelerometer=(), gyroscope=(), interest-cohort=()",
}

func New(config ...Config) fh.HandlerFunc {
	cfg := defaultConfig
	if len(config) > 0 {
		cfg = config[0]
	}

	var static [][2]string
	if cfg.ContentSecurityPolicy != "" {
		static = append(static, [2]string{"Content-Security-Policy", cfg.ContentSecurityPolicy})
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
	if cfg.PermissionsPolicy != "" {
		static = append(static, [2]string{"Permissions-Policy", cfg.PermissionsPolicy})
	}
	if cfg.HSTSMaxAge > 0 {
		hsts := "max-age=" + strconv.Itoa(cfg.HSTSMaxAge)
		if cfg.HSTSIncludeSubDomains {
			hsts += "; includeSubDomains"
		}
		static = append(static, [2]string{"Strict-Transport-Security", hsts})
	}

	return func(ctx *fh.Ctx) error {
		for _, h := range static {
			ctx.Set(h[0], h[1])
		}
		return ctx.Next()
	}
}
