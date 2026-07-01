package etag

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/oarkflow/fh"
)

type Config struct {
	Weak    bool
	MinSize int
	Methods []string
}

func New(cfg Config) fh.HandlerFunc {
	allowed := map[string]struct{}{"GET": {}, "HEAD": {}}
	if len(cfg.Methods) > 0 {
		allowed = map[string]struct{}{}
		for _, m := range cfg.Methods {
			allowed[strings.ToUpper(m)] = struct{}{}
		}
	}
	return func(c fh.Ctx) error {
		if _, ok := allowed[strings.ToUpper(c.Method())]; !ok {
			return c.Next()
		}
		c.CaptureResponseBody()
		c.OnBeforeResponse(func(ctx fh.Ctx) error {
			body := ctx.ResponseBody()
			if len(body) < cfg.MinSize {
				return nil
			}
			sum := sha256.Sum256(body)
			tag := `"` + hex.EncodeToString(sum[:]) + `"`
			if cfg.Weak {
				tag = "W/" + tag
			}
			ctx.Set("ETag", tag)
			ctx.Set("Vary", "Accept-Encoding")
			if match(ctx.Get("If-None-Match"), tag) {
				ctx.Status(fh.StatusNotModified)
				return ctx.SendBytes(nil)
			}
			return nil
		})
		return c.Next()
	}
}
func match(v, tag string) bool {
	for _, p := range strings.Split(v, ",") {
		if strings.TrimSpace(p) == tag || strings.TrimSpace(p) == "*" {
			return true
		}
	}
	return false
}
