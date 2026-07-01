package slowlog

import (
	"time"

	"github.com/oarkflow/fh"
)

type Config struct {
	Threshold time.Duration
	Logger    fh.Logger
	Skip      func(fh.Ctx) bool
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.Threshold <= 0 {
		cfg.Threshold = 500 * time.Millisecond
	}
	return func(c fh.Ctx) error {
		if cfg.Skip != nil && cfg.Skip(c) {
			return c.Next()
		}
		start := time.Now()
		err := c.Next()
		dur := time.Since(start)
		if dur >= cfg.Threshold {
			logger := cfg.Logger
			if logger == nil && c.App() != nil {
				logger = c.App().Logger()
			}
			if logger != nil {
				logger.Warn("slow request", "method", c.Method(), "path", c.Path(), "status", c.StatusCode(), "latency", dur.String(), "request_id", c.Get(fh.HeaderRequestID))
			}
		}
		return err
	}
}
