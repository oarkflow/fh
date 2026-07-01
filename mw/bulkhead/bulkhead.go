package bulkhead

import (
	"strconv"
	"time"

	"github.com/oarkflow/fh"
)

type KeyFunc func(fh.Ctx) string
type RejectHandler func(fh.Ctx, Result) error

type Result struct {
	Key        string
	Limit      int
	InFlight   int
	RetryAfter time.Duration
}

type Config struct {
	MaxConcurrent int
	Queue         int
	Timeout       time.Duration
	KeyFunc       KeyFunc
	Reject        RejectHandler
	Headers       bool
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 1024
	}
	if cfg.Reject == nil {
		cfg.Reject = DefaultReject
	}
	cfg.Headers = true
	sem := make(chan struct{}, cfg.MaxConcurrent)
	return func(c fh.Ctx) error {
		key := "global"
		if cfg.KeyFunc != nil {
			key = cfg.KeyFunc(c)
			if key == "" {
				key = "global"
			}
		}
		if cfg.Timeout <= 0 {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
				if cfg.Headers {
					setHeaders(c, cfg.MaxConcurrent, len(sem))
				}
				return c.Next()
			default:
				return cfg.Reject(c, Result{Key: key, Limit: cfg.MaxConcurrent, InFlight: len(sem), RetryAfter: time.Second})
			}
		}
		timer := time.NewTimer(cfg.Timeout)
		defer timer.Stop()
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			if cfg.Headers {
				setHeaders(c, cfg.MaxConcurrent, len(sem))
			}
			return c.Next()
		case <-timer.C:
			return cfg.Reject(c, Result{Key: key, Limit: cfg.MaxConcurrent, InFlight: len(sem), RetryAfter: cfg.Timeout})
		case <-c.Done():
			return c.Err()
		}
	}
}

func setHeaders(c fh.Ctx, limit, in int) {
	c.Set("X-Bulkhead-Limit", strconv.Itoa(limit))
	c.Set("X-Bulkhead-InFlight", strconv.Itoa(in))
}
func DefaultReject(c fh.Ctx, r Result) error {
	c.Set("Retry-After", strconv.Itoa(max(1, int(r.RetryAfter.Seconds()))))
	return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{"error": "bulkhead_limit_exceeded", "limit": r.Limit, "in_flight": r.InFlight})
}
