package bodylimit

import "github.com/oarkflow/fh"

type Config struct {
	Limit int
	Next  func(*fh.Ctx) bool
}

// New limits a request body. Register it with app.Use for a global policy or
// pass it before an endpoint handler for a route-specific policy.
func New(limit int) fh.HandlerFunc { return WithConfig(Config{Limit: limit}) }

func WithConfig(cfg Config) fh.HandlerFunc {
	if cfg.Limit <= 0 {
		panic("bodylimit: limit must be positive")
	}
	return func(c *fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		if len(c.Body()) > cfg.Limit {
			return fh.PayloadTooLarge("Request body exceeds the " + formatBytes(cfg.Limit) + " limit")
		}
		return c.Next()
	}
}

func formatBytes(n int) string {
	const unit = 1024
	if n < unit {
		return itoa(n) + " byte"
	}
	if n < unit*unit {
		return itoa(n/unit) + " KiB"
	}
	return itoa(n/(unit*unit)) + " MiB"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
