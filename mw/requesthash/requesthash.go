package requesthash

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/oarkflow/fh"
)

const (
	DefaultHeader = "X-Request-Body-SHA256"
	DefaultLocal  = "request_body_sha256"
)

type Config struct {
	Header    string
	LocalKey  string
	SkipEmpty bool
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.Header == "" {
		cfg.Header = DefaultHeader
	}
	if cfg.LocalKey == "" {
		cfg.LocalKey = DefaultLocal
	}
	return func(c fh.Ctx) error {
		body := c.BodyCopy()
		if cfg.SkipEmpty && len(body) == 0 {
			return c.Next()
		}
		sum := sha256.Sum256(body)
		digest := hex.EncodeToString(sum[:])
		c.Locals(cfg.LocalKey, digest)
		if cfg.Header != "" {
			c.Set(cfg.Header, digest)
		}
		return c.Next()
	}
}
