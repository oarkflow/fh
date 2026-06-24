package idempotency

import "github.com/oarkflow/fh"

func New(key func(*fh.Ctx) string) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		if key != nil {
			if value := key(c); value != "" {
				c.Header.Set(fh.HeaderIdempotencyKey, value)
			}
		}
		return c.Next()
	}
}
