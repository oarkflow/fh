package fh

// defaultHardeningMiddleware returns a middleware that applies production
// security defaults. Static policy is resolved once here; the request path is
// a small fixed loop with no mode branches or allocations.
func defaultHardeningMiddleware(cfg Config) HandlerFunc {
	mode := cfg.Mode
	isProd := mode == ModeProduction || mode == ModeStrict || mode == ModeEnterprise || cfg.Compliance.Enabled

	if !isProd {
		return nil
	}

	strict := mode == ModeStrict || mode == ModeEnterprise || cfg.Compliance.Strict || cfg.SecureByDefault
	referrer := "strict-origin-when-cross-origin"
	if strict {
		referrer = "no-referrer"
	}
	headers := [][2]string{
		{"X-Content-Type-Options", "nosniff"},
		{"X-Frame-Options", "DENY"},
		{"Referrer-Policy", referrer},
		{"X-XSS-Protection", "0"},
		{"Permissions-Policy", "geolocation=(), microphone=(), camera=(), payment=(), usb=()"},
	}
	if cfg.SecureByDefault {
		hsts := "max-age=31536000; includeSubDomains"
		if cfg.HSTSPreload {
			hsts += "; preload"
		}
		headers = append(headers,
			[2]string{"Strict-Transport-Security", hsts},
			[2]string{"Cross-Origin-Opener-Policy", "same-origin"},
			[2]string{"Cross-Origin-Resource-Policy", "same-origin"},
		)
	}

	return func(c Ctx) error {
		if dc, ok := c.(*DefaultCtx); ok {
			for _, header := range headers {
				dc.Set(header[0], header[1])
			}
		} else {
			for _, header := range headers {
				c.Set(header[0], header[1])
			}
		}
		return c.Next()
	}
}
