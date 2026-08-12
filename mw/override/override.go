package override

import (
	"strings"

	"github.com/oarkflow/fh"
)

type Config struct {
	Header         string
	QueryParam     string
	FormParam      string
	AllowedMethods []string
}

func New(config ...Config) fh.HandlerFunc {
	cfg := Config{
		Header:         "X-HTTP-Method-Override",
		QueryParam:     "_method",
		FormParam:      "_method",
		AllowedMethods: []string{"PUT", "PATCH", "DELETE"},
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
		if len(config[0].AllowedMethods) > 0 {
			cfg.AllowedMethods = config[0].AllowedMethods
		}
	}

	allowedSet := make(map[string]struct{}, len(cfg.AllowedMethods))
	for _, m := range cfg.AllowedMethods {
		allowedSet[strings.ToUpper(m)] = struct{}{}
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
				bodyStr := string(c.Body())
				if strings.Contains(bodyStr, cfg.FormParam+"=") {
					for _, pair := range strings.Split(bodyStr, "&") {
						parts := strings.SplitN(pair, "=", 2)
						if len(parts) == 2 && parts[0] == cfg.FormParam {
							targetMethod = parts[1]
							break
						}
					}
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
