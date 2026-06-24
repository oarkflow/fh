package circuitbreaker

import (
	"errors"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

type Config struct {
	FailureThreshold int
	SuccessThreshold int
	ResetAfter       time.Duration
	IsFailure        func(fh.Ctx, error) bool
	OnOpen           func(fh.Ctx) error
}

type Breaker struct {
	cfg                 Config
	mu                  sync.Mutex
	state               string
	failures, successes int
	opened              time.Time
}

func New(cfg Config) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 1
	}
	if cfg.ResetAfter <= 0 {
		cfg.ResetAfter = 30 * time.Second
	}
	return &Breaker{cfg: cfg, state: "closed"}
}

func Middleware(cfg Config) fh.HandlerFunc { return New(cfg).Handler() }

func (b *Breaker) Handler() fh.HandlerFunc {
	return func(c fh.Ctx) error {
		b.mu.Lock()
		if b.state == "open" {
			if time.Since(b.opened) < b.cfg.ResetAfter {
				b.mu.Unlock()
				if b.cfg.OnOpen != nil {
					return b.cfg.OnOpen(c)
				}
				return fh.NewHTTPError(fh.StatusServiceUnavailable, "CIRCUIT_OPEN", "circuit breaker is open")
			}
			b.state = "half-open"
		}
		b.mu.Unlock()
		err := c.Next()
		failed := err != nil || c.StatusCode() >= 500
		var httpErr *fh.HTTPError
		if errors.As(err, &httpErr) {
			failed = httpErr.Status >= 500
		}
		if b.cfg.IsFailure != nil {
			failed = b.cfg.IsFailure(c, err)
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		if failed {
			b.failures++
			b.successes = 0
			if b.failures >= b.cfg.FailureThreshold {
				b.state, b.opened = "open", time.Now()
			}
			return err
		}
		if b.state == "half-open" {
			b.successes++
			if b.successes >= b.cfg.SuccessThreshold {
				b.state, b.failures, b.successes = "closed", 0, 0
			}
		} else {
			b.failures = 0
		}
		return err
	}
}
