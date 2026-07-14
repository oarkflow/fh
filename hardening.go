package fh

// defaultHardeningMiddleware returns a middleware that applies production
// security defaults. It is auto-prepended when Mode is production, strict,
// or enterprise. Returns nil for non-production modes.
func defaultHardeningMiddleware(cfg Config) HandlerFunc {
	mode := cfg.Mode
	isProd := mode == ModeProduction || mode == ModeStrict || mode == ModeEnterprise || cfg.Compliance.Enabled

	if !isProd {
		return nil
	}

	return func(c Ctx) error {
		dc, ok := c.(*DefaultCtx)
		if !ok {
			return c.Next()
		}

		if !hasCustomHeader(dc, "X-Content-Type-Options") {
			dc.Set("X-Content-Type-Options", "nosniff")
		}

		if !hasCustomHeader(dc, "X-Frame-Options") {
			dc.Set("X-Frame-Options", "DENY")
		}

		if !hasCustomHeader(dc, "Referrer-Policy") {
			if mode == ModeStrict || mode == ModeEnterprise || cfg.Compliance.Strict {
				dc.Set("Referrer-Policy", "no-referrer")
			} else {
				dc.Set("Referrer-Policy", "strict-origin-when-cross-origin")
			}
		}

		if !hasCustomHeader(dc, "X-XSS-Protection") {
			dc.Set("X-XSS-Protection", "0")
		}

		if !hasCustomHeader(dc, "Permissions-Policy") {
			dc.Set("Permissions-Policy", "geolocation=(), microphone=(), camera=(), payment=(), usb=()")
		}

		return c.Next()
	}
}

// hasCustomHeader reports whether a header with the given name has already
// been queued on the context via Set or Append.
func hasCustomHeader(dc *DefaultCtx, name string) bool {
	for i := 0; i < dc.chCount; i++ {
		if bytesEqualFold(dc.customHeaders[i].Key, []byte(name)) {
			return true
		}
	}
	for _, h := range dc.extraHeaders {
		if bytesEqualFold(h.Key, []byte(name)) {
			return true
		}
	}
	return false
}
