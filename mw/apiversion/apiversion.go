package apiversion

import (
	"strings"

	"github.com/oarkflow/fh"
)

type Config struct {
	Header     string
	Default    string
	Supported  []string
	Deprecated map[string]string
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.Header == "" {
		cfg.Header = "Accept-Version"
	}
	return func(c fh.Ctx) error {
		v := c.Get(cfg.Header)
		if v == "" {
			v = cfg.Default
		}
		if len(cfg.Supported) > 0 && !contains(cfg.Supported, v) {
			return fh.NewHTTPError(fh.StatusBadRequest, "API_VERSION_UNSUPPORTED", "API version is not supported")
		}
		c.Locals("api_version", v)
		if sunset := lookupDeprecated(cfg.Deprecated, v); sunset != "" {
			c.Set("Sunset", sunset)
			c.Set("Deprecation", "true")
		}
		return c.Next()
	}
}

func contains(values []string, target string) bool {
	for _, v := range values {
		if strings.EqualFold(v, target) {
			return true
		}
	}
	return false
}

// lookupDeprecated looks up v in the Deprecated map the same way contains
// matches Supported: case-insensitively. Version matching is
// case-insensitive throughout this middleware, so a client sending "V1"
// must see the same deprecation signal as one sending "v1"; an exact-only
// lookup here would silently swallow the Sunset/Deprecation headers for any
// deprecated version whose casing doesn't match the map key exactly.
func lookupDeprecated(m map[string]string, v string) string {
	if s, ok := m[v]; ok {
		return s
	}
	for k, s := range m {
		if strings.EqualFold(k, v) {
			return s
		}
	}
	return ""
}
