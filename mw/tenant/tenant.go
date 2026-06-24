package tenant

import "github.com/oarkflow/fh"

type Config struct {
	Header   string
	Required bool
	Next     func(fh.Ctx) bool
}

func New(cfg Config) fh.HandlerFunc {
	resolver := fh.TenantResolver(cfg.RequestHeader(), cfg.Required)
	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		return resolver(c)
	}
}
