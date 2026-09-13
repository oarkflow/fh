package bulkhead

import (
	"strconv"
	"sync"
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
	type bucket struct {
		sem    chan struct{}
		mu     sync.Mutex
		queued int
	}
	var buckets sync.Map
	getBucket := func(key string) *bucket {
		if value, ok := buckets.Load(key); ok {
			return value.(*bucket)
		}
		created := &bucket{sem: make(chan struct{}, cfg.MaxConcurrent)}
		actual, _ := buckets.LoadOrStore(key, created)
		return actual.(*bucket)
	}
	return func(c fh.Ctx) error {
		key := "global"
		if cfg.KeyFunc != nil {
			key = cfg.KeyFunc(c)
			if key == "" {
				key = "global"
			}
		}
		b := getBucket(key)
		if cfg.Timeout <= 0 {
			select {
			case b.sem <- struct{}{}:
				defer func() { <-b.sem }()
				if cfg.Headers {
					setHeaders(c, cfg.MaxConcurrent, len(b.sem))
				}
				return c.Next()
			default:
				return cfg.Reject(c, Result{Key: key, Limit: cfg.MaxConcurrent, InFlight: len(b.sem), RetryAfter: time.Second})
			}
		}
		b.mu.Lock()
		if len(b.sem) == cfg.MaxConcurrent && b.queued >= cfg.Queue {
			inFlight := len(b.sem)
			b.mu.Unlock()
			return cfg.Reject(c, Result{Key: key, Limit: cfg.MaxConcurrent, InFlight: inFlight, RetryAfter: cfg.Timeout})
		}
		b.queued++
		b.mu.Unlock()
		defer func() {
			b.mu.Lock()
			b.queued--
			b.mu.Unlock()
		}()
		timer := time.NewTimer(cfg.Timeout)
		defer timer.Stop()
		select {
		case b.sem <- struct{}{}:
			defer func() { <-b.sem }()
			if cfg.Headers {
				setHeaders(c, cfg.MaxConcurrent, len(b.sem))
			}
			return c.Next()
		case <-timer.C:
			return cfg.Reject(c, Result{Key: key, Limit: cfg.MaxConcurrent, InFlight: len(b.sem), RetryAfter: cfg.Timeout})
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
