// Package policy groups cross-cutting route metadata with middleware policies.
package policy

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/apiversion"
)

type Config struct {
	Data    fh.DataPolicy
	Version apiversion.Config
}

// New records data policy metadata and applies API version policy.
func New(cfg Config) fh.HandlerFunc {
	version := apiversion.New(cfg.Version)
	return func(c fh.Ctx) error {
		if cfg.Data.Sensitivity != "" {
			c.Locals("fh.data_policy", cfg.Data)
		}
		if len(cfg.Version.Supported) > 0 || cfg.Version.Default != "" || len(cfg.Version.Deprecated) > 0 {
			return version(c)
		}
		return c.Next()
	}
}
