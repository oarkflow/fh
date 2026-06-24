package apikey

import (
	"crypto/hmac"

	"github.com/oarkflow/fh"
)

type LookupFunc func(ctx *fh.Ctx, key string) bool
type ErrorHandler func(ctx *fh.Ctx) error

type Config struct {
	Header string
	Query  string
	Keys   []string
	Lookup LookupFunc
	Next   func(*fh.Ctx) bool
	Error  ErrorHandler
}

func New(config Config) fh.HandlerFunc {
	if config.Header == "" {
		config.Header = "X-API-Key"
	}
	allowed := make([][]byte, 0, len(config.Keys))
	for _, k := range config.Keys {
		if k != "" {
			allowed = append(allowed, []byte(k))
		}
	}
	if config.Error == nil {
		config.Error = func(c *fh.Ctx) error {
			return fh.NewHTTPError(fh.StatusUnauthorized, "API_KEY_INVALID", "API key is missing or invalid")
		}
	}
	return func(c *fh.Ctx) error {
		if config.Next != nil && config.Next(c) {
			return c.Next()
		}
		key := c.Get(config.Header)
		if key == "" && config.Query != "" {
			key = c.Query(config.Query)
		}
		ok := false
		if key != "" && config.Lookup != nil {
			ok = config.Lookup(c, key)
		}
		if key != "" && !ok {
			kb := []byte(key)
			for _, want := range allowed {
				if hmac.Equal(kb, want) {
					ok = true
					break
				}
			}
		}
		if !ok {
			return config.Error(c)
		}
		return c.Next()
	}
}
