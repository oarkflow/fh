package override

import (
	"log/slog"
	"net/url"
	"strings"

	"github.com/oarkflow/fh"
)

type Config struct {
	Header     string
	QueryParam string
	FormParam  string

	// AllowedMethods is the set of methods a POST request is permitted to be
	// rewritten into. It is empty (disabled) by default: any caller that can
	// reach this handler can set the override header/param, so honoring it
	// silently lets that caller bypass method-based access controls (a WAF,
	// gateway ACL, or audit log) sitting in front of this server, which only
	// ever observes the original POST. Set this explicitly, and only after
	// confirming no perimeter control depends on the wire method.
	AllowedMethods []string
}

func New(config ...Config) fh.HandlerFunc {
	cfg := Config{
		Header:     "X-HTTP-Method-Override",
		QueryParam: "_method",
		FormParam:  "_method",
	}
	if len(config) > 0 {
		if config[0].Header != "" {
			cfg.Header = config[0].Header
		}
		if config[0].QueryParam != "" {
			cfg.QueryParam = config[0].QueryParam
		}
		if config[0].FormParam != "" {
			cfg.FormParam = config[0].FormParam
		}
		cfg.AllowedMethods = config[0].AllowedMethods
	}

	allowedSet := make(map[string]struct{}, len(cfg.AllowedMethods))
	for _, m := range cfg.AllowedMethods {
		allowedSet[strings.ToUpper(m)] = struct{}{}
	}
	if len(allowedSet) == 0 {
		slog.Warn("fh/mw/override: no AllowedMethods configured — method override is disabled; any client that can send a POST would otherwise be able to rewrite it into another method and bypass perimeter method-based access controls")
	} else {
		slog.Warn("fh/mw/override: method override enabled for methods " + strings.Join(cfg.AllowedMethods, ",") + " — any client sending POST can trigger these methods via the override header/param; ensure no WAF, gateway ACL, or audit log in front of this server relies on the raw wire method")
	}

	return func(c fh.Ctx) error {
		if c.Method() == "POST" {
			targetMethod := ""
			if cfg.Header != "" {
				targetMethod = c.Get(cfg.Header)
			}
			if targetMethod == "" && cfg.QueryParam != "" {
				targetMethod = c.Query(cfg.QueryParam)
			}
			if targetMethod == "" && cfg.FormParam != "" && len(c.Body()) > 0 {
				if values, err := url.ParseQuery(string(c.Body())); err == nil {
					targetMethod = values.Get(cfg.FormParam)
				}
			}

			if targetMethod != "" {
				targetMethod = strings.ToUpper(strings.TrimSpace(targetMethod))
				if _, ok := allowedSet[targetMethod]; ok {
					if dc, ok := c.(*fh.DefaultCtx); ok {
						dc.Header.Method = []byte(targetMethod)
						return c.Rewrite(c.Path())
					}
				}
			}
		}
		return c.Next()
	}
}
