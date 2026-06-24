package compliance

import "github.com/oarkflow/fh"

// New records compliance route metadata and can enforce authentication.
type Config struct {
	Security fh.RouteSecurityConfig
	Data     fh.DataPolicy
}

func New(cfg Config) fh.HandlerFunc {
	sec := fh.RouteSecurity(cfg.Security)
	return func(c fh.Ctx) error {
		if cfg.Data.Sensitivity != "" {
			c.Locals("fh.data_policy", cfg.Data)
		}
		return sec(c)
	}
}
