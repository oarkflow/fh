package conditional

import (
	"github.com/oarkflow/fh"
	"strings"
	"time"
)

type ETagFunc func(fh.Ctx) string
type LastModifiedFunc func(fh.Ctx) time.Time
type Config struct {
	ETag         ETagFunc
	LastModified LastModifiedFunc
	WeakCompare  bool
}

func New(cfg Config) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		if cfg.ETag != nil {
			tag := cfg.ETag(c)
			if tag != "" {
				if matchETag(c.Get("If-None-Match"), tag, cfg.WeakCompare) {
					c.Set("ETag", tag)
					return c.SendStatus(fh.StatusNotModified)
				}
				c.Set("ETag", tag)
			}
		}
		if cfg.LastModified != nil {
			lm := cfg.LastModified(c)
			if !lm.IsZero() {
				c.Set("Last-Modified", lm.UTC().Format(time.RFC1123))
				if ims := c.Get("If-Modified-Since"); ims != "" {
					if t, err := time.Parse(time.RFC1123, ims); err == nil && !lm.After(t) {
						return c.SendStatus(fh.StatusNotModified)
					}
				}
			}
		}
		return c.Next()
	}
}
func matchETag(header, tag string, weak bool) bool {
	if header == "" {
		return false
	}
	for _, p := range strings.Split(header, ",") {
		p = strings.TrimSpace(p)
		if p == "*" || p == tag {
			return true
		}
		if weak && stripWeak(p) == stripWeak(tag) {
			return true
		}
	}
	return false
}
func stripWeak(s string) string { return strings.TrimPrefix(strings.TrimSpace(s), "W/") }
