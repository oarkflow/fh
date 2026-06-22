package cors

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/oarkflow/fh"
)

type OriginStore interface {
	Allowed(origin string) bool
}

type Config struct {
	AllowOrigins  []string
	AllowMethods  []string
	AllowHeaders  []string
	ExposeHeaders []string

	AllowCredentials    bool
	AllowPrivateNetwork bool

	MaxAge          int
	PreflightStatus int

	OriginStore OriginStore

	Next func(ctx *fh.Ctx) bool
}

var DefaultConfig = Config{
	AllowOrigins: []string{"*"},
	AllowMethods: []string{
		"GET",
		"POST",
		"PUT",
		"PATCH",
		"DELETE",
		"OPTIONS",
	},
	AllowHeaders: []string{
		"Origin",
		"Content-Type",
		"Accept",
		"Authorization",
		"X-Request-ID",
	},
	ExposeHeaders: []string{
		"X-Request-ID",
	},
	MaxAge:          86400,
	PreflightStatus: 204,
}

func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		cfg = mergeConfig(cfg, config[0])
	}

	if cfg.OriginStore == nil {
		cfg.OriginStore = NewMemoryOriginStore(cfg.AllowOrigins...)
	}

	methods := strings.Join(cfg.AllowMethods, ", ")
	headers := strings.Join(cfg.AllowHeaders, ", ")
	exposeHeaders := strings.Join(cfg.ExposeHeaders, ", ")
	maxAge := strconv.Itoa(cfg.MaxAge)

	return func(ctx *fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(ctx) {
			return ctx.Next()
		}

		origin := ctx.Get("Origin")
		if origin == "" {
			return ctx.Next()
		}

		if !validOrigin(origin) {
			return ctx.Next()
		}

		allowed := cfg.OriginStore.Allowed(origin)
		if !allowed {
			return ctx.Next()
		}

		if cfg.AllowCredentials {
			ctx.Set("Access-Control-Allow-Origin", origin)
			ctx.Set("Access-Control-Allow-Credentials", "true")
			ctx.Append("Vary", "Origin")
		} else {
			if isWildcardStore(cfg.OriginStore) {
				ctx.Set("Access-Control-Allow-Origin", "*")
			} else {
				ctx.Set("Access-Control-Allow-Origin", origin)
				ctx.Append("Vary", "Origin")
			}
		}

		if exposeHeaders != "" {
			ctx.Set("Access-Control-Expose-Headers", exposeHeaders)
		}

		if isPreflight(ctx) {
			ctx.Append("Vary", "Access-Control-Request-Method")
			ctx.Append("Vary", "Access-Control-Request-Headers")
			requestedMethod := ctx.Get("Access-Control-Request-Method")
			if !containsFold(cfg.AllowMethods, requestedMethod) {
				return ctx.Status(403).SendString("CORS preflight method denied")
			}
			ctx.Set("Access-Control-Allow-Methods", methods)

			requestHeaders := ctx.Get("Access-Control-Request-Headers")
			if requestHeaders != "" && !headersAllowed(cfg.AllowHeaders, requestHeaders) {
				return ctx.Status(403).SendString("CORS preflight headers denied")
			}
			if requestHeaders != "" && len(cfg.AllowHeaders) == 1 && cfg.AllowHeaders[0] == "*" {
				ctx.Set("Access-Control-Allow-Headers", requestHeaders)
				ctx.Append("Vary", "Access-Control-Request-Headers")
			} else {
				ctx.Set("Access-Control-Allow-Headers", headers)
			}

			if cfg.AllowPrivateNetwork && strings.EqualFold(ctx.Get("Access-Control-Request-Private-Network"), "true") {
				ctx.Set("Access-Control-Allow-Private-Network", "true")
			}

			if cfg.MaxAge > 0 {
				ctx.Set("Access-Control-Max-Age", maxAge)
			}

			return ctx.SendStatus(cfg.PreflightStatus)
		}

		return ctx.Next()
	}
}

func mergeConfig(base Config, override Config) Config {
	if override.AllowOrigins != nil {
		base.AllowOrigins = override.AllowOrigins
	}
	if override.AllowMethods != nil {
		base.AllowMethods = override.AllowMethods
	}
	if override.AllowHeaders != nil {
		base.AllowHeaders = override.AllowHeaders
	}
	if override.ExposeHeaders != nil {
		base.ExposeHeaders = override.ExposeHeaders
	}
	if override.MaxAge != 0 {
		base.MaxAge = override.MaxAge
	}
	if override.PreflightStatus != 0 {
		base.PreflightStatus = override.PreflightStatus
	}
	if override.OriginStore != nil {
		base.OriginStore = override.OriginStore
	}
	if override.Next != nil {
		base.Next = override.Next
	}

	base.AllowCredentials = override.AllowCredentials
	base.AllowPrivateNetwork = override.AllowPrivateNetwork

	return base
}

func containsFold(values []string, want string) bool {
	for _, v := range values {
		if v == "*" || strings.EqualFold(strings.TrimSpace(v), strings.TrimSpace(want)) {
			return true
		}
	}
	return false
}
func headersAllowed(allowed []string, requested string) bool {
	if len(allowed) == 1 && allowed[0] == "*" {
		return true
	}
	for _, h := range strings.Split(requested, ",") {
		if !containsFold(allowed, h) {
			return false
		}
	}
	return true
}

func isPreflight(ctx *fh.Ctx) bool {
	m := ctx.Header.Method
	return len(m) == 7 &&
		m[0] == 'O' &&
		m[1] == 'P' &&
		m[2] == 'T' &&
		m[3] == 'I' &&
		m[4] == 'O' &&
		m[5] == 'N' &&
		m[6] == 'S' &&
		ctx.Get("Access-Control-Request-Method") != ""
}

func validOrigin(origin string) bool {
	if origin == "" {
		return false
	}

	for i := 0; i < len(origin); i++ {
		if origin[i] == '\r' || origin[i] == '\n' {
			return false
		}
	}

	u, err := url.Parse(origin)
	if err != nil {
		return false
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}

	return u.Host != ""
}

// -----------------------------------------------------------------------------
// Memory origin store
// -----------------------------------------------------------------------------

type MemoryOriginStore struct {
	wildcard bool
	exact    map[string]struct{}
	suffixes []string
}

func NewMemoryOriginStore(origins ...string) *MemoryOriginStore {
	s := &MemoryOriginStore{
		exact: make(map[string]struct{}, len(origins)),
	}

	for _, origin := range origins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}

		if origin == "*" {
			s.wildcard = true
			continue
		}

		// Allows patterns like:
		// https://*.example.com
		if strings.Contains(origin, "*.") {
			prefix := origin[:strings.Index(origin, "*.")]
			suffix := origin[strings.Index(origin, "*.")+1:]

			if prefix == "http://" || prefix == "https://" {
				s.suffixes = append(s.suffixes, prefix+suffix)
			}

			continue
		}

		s.exact[origin] = struct{}{}
	}

	return s
}

func (s *MemoryOriginStore) Allowed(origin string) bool {
	if s.wildcard {
		return true
	}

	if _, ok := s.exact[origin]; ok {
		return true
	}

	for _, suffix := range s.suffixes {
		if strings.HasSuffix(origin, suffix) && origin != suffix {
			return true
		}
	}

	return false
}

func isWildcardStore(store OriginStore) bool {
	s, ok := store.(*MemoryOriginStore)
	return ok && s.wildcard
}
