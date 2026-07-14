// Package coalesce provides request coalescing middleware. When multiple
// identical requests arrive concurrently, only one executes the handler;
// the others receive the same response. This is critical for preventing
// thundering herd on cache misses and duplicate API calls.
//
// Requests are considered identical if they have the same method and full
// URL (path + query string). POST/PUT/PATCH bodies can optionally be
// included in the key via IncludeBody. Requests carrying an Authorization
// or (by default) Cookie header are never coalesced.
package coalesce

import (
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type key struct{}

// entry represents an in-flight request being coalesced.
type entry struct {
	once       sync.Once
	done       chan struct{}
	statusCode int
	headers    map[string]string
	body       []byte
	err        error
	refs       atomic.Int64
}

// Config holds configuration for the coalesce middleware.
type Config struct {
	// TTL is how long a coalesced entry lives after completion. Default: 5s.
	TTL time.Duration

	// MaxEntries is the maximum number of coalesced entries to track. Default: 4096.
	MaxEntries int

	// IncludeBody if true includes POST/PUT/PATCH body in the key. Default: false.
	IncludeBody bool

	// AllowRequestCookies permits coalescing requests that carry a Cookie
	// header. Default false: requests with an Authorization or Cookie
	// header are never coalesced, since fanning out one caller's
	// personalized/authenticated response to concurrent waiters would leak
	// it to different callers who happened to request the same path at the
	// same time.
	AllowRequestCookies bool

	// Next is an optional skip function.
	Next func(ctx fh.Ctx) bool
}

// DefaultConfig returns the default configuration.
var DefaultConfig = Config{
	TTL:        5 * time.Second,
	MaxEntries: 4096,
}

// New creates a request coalescing middleware.
func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		if config[0].TTL > 0 {
			cfg.TTL = config[0].TTL
		}
		if config[0].MaxEntries > 0 {
			cfg.MaxEntries = config[0].MaxEntries
		}
		cfg.IncludeBody = config[0].IncludeBody
		cfg.AllowRequestCookies = config[0].AllowRequestCookies
		cfg.Next = config[0].Next
	}

	cache := &coalesceCache{
		entries: make(map[string]*entry, cfg.MaxEntries),
		max:     cfg.MaxEntries,
	}

	go cache.evictLoop(cfg.TTL)

	return func(ctx fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(ctx) {
			return ctx.Next()
		}

		method := ctx.Method()
		if method != "GET" && method != "HEAD" && !cfg.IncludeBody {
			return ctx.Next()
		}

		// Never fan out a coalesced response for requests carrying
		// per-caller credentials, unless the operator explicitly opts in
		// for cookies (and even then, distinct callers with distinct
		// cookies will still coalesce onto the same entry unless the app
		// also disables coalescing for those routes — this only restores
		// the same-caller-only default other identity-bearing requests
		// get).
		if ctx.Get("Authorization") != "" || (!cfg.AllowRequestCookies && ctx.Get("Cookie") != "") {
			return ctx.Next()
		}

		rkey := coalesceKey(ctx, cfg.IncludeBody)

		e, loaded := cache.getOrCreate(rkey)
		if loaded {
			// Another request is handling this — wait for its result.
			<-e.done
			if e.err != nil {
				return e.err
			}
			for k, v := range e.headers {
				ctx.Set(k, v)
			}
			if len(e.body) > 0 {
				return ctx.SendBytes(e.body)
			}
			if e.statusCode != 200 {
				return ctx.SendStatus(e.statusCode)
			}
			return nil
		}

		// We won the race — execute the handler.
		err := ctx.Next()

		// Capture response.
		e.statusCode = ctx.StatusCode()
		// Copy response headers.
		respHeaders := ctx.GetRespHeaders()
		e.headers = make(map[string]string, len(respHeaders))
		for k, vals := range respHeaders {
			if len(vals) > 0 {
				e.headers[k] = vals[0]
			}
		}
		if ctx.Responded() {
			e.body = ctx.ResponseBody()
		}
		e.err = err

		e.doneOnce()
		return err
	}
}

// ── Cache implementation ───────────────────────────────────────────────────

type coalesceCache struct {
	mu      sync.RWMutex
	entries map[string]*entry
	max     int
}

func (c *coalesceCache) getOrCreate(key string) (*entry, bool) {
	c.mu.RLock()
	if e, ok := c.entries[key]; ok {
		c.mu.RUnlock()
		e.refs.Add(1)
		return e, true
	}
	c.mu.RUnlock()

	c.mu.Lock()
	// Double-check after acquiring write lock.
	if e, ok := c.entries[key]; ok {
		c.mu.Unlock()
		e.refs.Add(1)
		return e, true
	}

	// Evict if at capacity.
	if len(c.entries) >= c.max {
		for k, v := range c.entries {
			select {
			case <-v.done:
				delete(c.entries, k)
			default:
				// Still in flight — skip.
			}
			if len(c.entries) < c.max {
				break
			}
		}
	}

	e := &entry{
		done: make(chan struct{}),
	}
	c.entries[key] = e
	c.mu.Unlock()

	return e, false
}

func (c *coalesceCache) evictLoop(ttl time.Duration) {
	ticker := time.NewTicker(ttl)
	defer ticker.Stop()
	for range ticker.C {
		func() {
			defer func() { recover() }()
			c.mu.Lock()
			defer c.mu.Unlock()
			for k, e := range c.entries {
				select {
				case <-e.done:
					if e.refs.Load() <= 1 {
						delete(c.entries, k)
					}
				default:
				}
			}
		}()
	}
}

func (e *entry) doneOnce() {
	e.once.Do(func() {
		close(e.done)
	})
}

// ── Key generation ─────────────────────────────────────────────────────────

func coalesceKey(ctx fh.Ctx, includeBody bool) string {
	h := sha256.New()
	h.Write([]byte(ctx.Method()))
	h.Write([]byte{0})
	// OriginalURL includes the query string, so two concurrent requests to
	// the same path differing only by query (e.g. ?q=foo vs ?q=bar) never
	// collide onto the same in-flight entry.
	h.Write([]byte(ctx.OriginalURL()))

	if includeBody {
		h.Write([]byte{0})
		h.Write(ctx.Body())
	}

	return string(h.Sum(nil))
}
