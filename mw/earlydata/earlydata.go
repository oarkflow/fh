// Package earlydata protects non-idempotent requests replayed as TLS early data.
package earlydata

import (
	"strings"

	"github.com/oarkflow/fh"
)

type Config struct {
	AllowMethods            []string
	AllowWithIdempotencyKey bool
	IdempotencyHeader       string
	Next                    func(fh.Ctx) bool
}

var DefaultConfig = Config{
	AllowMethods:            []string{"GET", "HEAD", "OPTIONS", "TRACE"},
	AllowWithIdempotencyKey: false,
	IdempotencyHeader:       "Idempotency-Key",
}

func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		if config[0].AllowMethods != nil {
			cfg.AllowMethods = config[0].AllowMethods
		}
		if config[0].IdempotencyHeader != "" {
			cfg.IdempotencyHeader = config[0].IdempotencyHeader
		}
		cfg.AllowWithIdempotencyKey = config[0].AllowWithIdempotencyKey
		cfg.Next = config[0].Next
	}
	allowed := make(map[string]struct{}, len(cfg.AllowMethods))
	for _, method := range cfg.AllowMethods {
		allowed[strings.ToUpper(method)] = struct{}{}
	}
	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		if c.Get("Early-Data") != "1" {
			return c.Next()
		}
		if _, ok := allowed[c.Method()]; ok {
			return c.Next()
		}
		if cfg.AllowWithIdempotencyKey && c.Get(cfg.IdempotencyHeader) != "" {
			return c.Next()
		}
		return fh.NewHTTPError(fh.StatusTooEarly, "TOO_EARLY", "Retry the request after the TLS handshake completes")
	}
}
