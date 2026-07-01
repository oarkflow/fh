package hostguard

import (
	"strings"

	"github.com/oarkflow/fh"
)

type RejectHandler func(fh.Ctx, string) error

type Config struct {
	Allowed    []string
	Denied     []string
	AllowEmpty bool
	Reject     RejectHandler
}

func New(cfg Config) fh.HandlerFunc {
	allowed := map[string]struct{}{}
	denied := map[string]struct{}{}
	for _, h := range cfg.Allowed {
		allowed[normalize(h)] = struct{}{}
	}
	for _, h := range cfg.Denied {
		denied[normalize(h)] = struct{}{}
	}
	if cfg.Reject == nil {
		cfg.Reject = func(c fh.Ctx, host string) error {
			return c.Status(fh.StatusMisdirectedRequest).JSON(fh.Map{"error": "host_not_allowed", "host": host})
		}
	}
	return func(c fh.Ctx) error {
		host := normalize(c.Hostname())
		if host == "" && cfg.AllowEmpty {
			return c.Next()
		}
		if _, bad := denied[host]; bad {
			return cfg.Reject(c, host)
		}
		if len(allowed) > 0 {
			if _, ok := allowed[host]; !ok {
				return cfg.Reject(c, host)
			}
		}
		return c.Next()
	}
}
func normalize(h string) string {
	h = strings.ToLower(strings.TrimSpace(h))
	if i := strings.LastIndexByte(h, ':'); i > -1 && !strings.Contains(h[i+1:], "]") {
		h = h[:i]
	}
	return h
}
