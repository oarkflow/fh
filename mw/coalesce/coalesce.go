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
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

// entry represents an in-flight request being coalesced.
type entry struct {
	once       sync.Once
	done       chan struct{}
	statusCode int
	headers    map[string]string
	body       []byte
	err        error
	share      bool
}

// Config holds configuration for the coalesce middleware.
type Config struct {
	// TTL is retained for source compatibility. Completed entries are no longer
	// retained; coalescing applies only while the original handler is running.
	TTL time.Duration

	// MaxEntries is the maximum number of coalesced entries to track. Default: 4096.
	MaxEntries int

	// MaxBodySize limits response bytes retained for coalesced waiters. If a
	// response exceeds this size, waiters execute their own handler instead.
	// Default: 1 MiB.
	MaxBodySize int

	// IncludeBody if true includes POST/PUT/PATCH body in the key. Default: false.
	IncludeBody bool

	// AllowRequestCookies permits coalescing requests that carry a Cookie
	// header when no KeyFunc is configured. Default false. A KeyFunc is an
	// explicit assertion that identity is safely partitioned in its key.
	AllowRequestCookies bool

	// Next is an optional skip function.
	Next func(ctx fh.Ctx) bool

	// KeyFunc overrides the default method + URL (+ optional body) key. Use
	// this to partition coalesced responses by authenticated identity when
	// enabling coalescing on personalized routes. A custom key must include
	// every identity and request-context value that can change the response.
	KeyFunc func(ctx fh.Ctx) string

	// VaryHeaders adds request headers to the default key. Header names are
	// matched case-insensitively. Use this for representation or tenant headers
	// when a custom KeyFunc is not needed.
	VaryHeaders []string
}

// DefaultConfig returns the default configuration.
var DefaultConfig = Config{
	TTL:         5 * time.Second,
	MaxEntries:  4096,
	MaxBodySize: 1 << 20,
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
		if config[0].MaxBodySize > 0 {
			cfg.MaxBodySize = config[0].MaxBodySize
		}
		cfg.IncludeBody = config[0].IncludeBody
		cfg.AllowRequestCookies = config[0].AllowRequestCookies
		cfg.Next = config[0].Next
		cfg.KeyFunc = config[0].KeyFunc
		cfg.VaryHeaders = append([]string(nil), config[0].VaryHeaders...)
	}

	cache := &coalesceCache{
		entries: make(map[string]*entry, cfg.MaxEntries),
		max:     cfg.MaxEntries,
	}

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
		if ctx.Get("Authorization") != "" || (!cfg.AllowRequestCookies && ctx.Get("Cookie") != "" && cfg.KeyFunc == nil) {
			return ctx.Next()
		}

		rkey := coalesceKey(ctx, cfg.IncludeBody, cfg.VaryHeaders)
		if cfg.KeyFunc != nil {
			rkey = cfg.KeyFunc(ctx)
			if rkey == "" {
				return ctx.Next()
			}
		}

		e, loaded := cache.getOrCreate(rkey)
		if e == nil {
			return ctx.Next()
		}
		if loaded {
			// Another request is handling this — wait for its result.
			<-e.done
			if !e.share {
				return ctx.Next()
			}
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
		ctx.CaptureResponseBody()
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
			e.body = append([]byte(nil), ctx.ResponseBody()...)
		}
		e.err = err
		if len(e.body) > cfg.MaxBodySize {
			e.body = nil
			e.share = false
		}

		cache.remove(rkey, e)
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
		return e, true
	}
	c.mu.RUnlock()

	c.mu.Lock()
	// Double-check after acquiring write lock.
	if e, ok := c.entries[key]; ok {
		c.mu.Unlock()
		return e, true
	}

	if len(c.entries) >= c.max {
		c.mu.Unlock()
		return nil, false
	}

	e := &entry{
		done:  make(chan struct{}),
		share: true,
	}
	c.entries[key] = e
	c.mu.Unlock()

	return e, false
}

func (c *coalesceCache) remove(key string, expected *entry) {
	c.mu.Lock()
	if c.entries[key] == expected {
		delete(c.entries, key)
	}
	c.mu.Unlock()
}

func (e *entry) doneOnce() {
	e.once.Do(func() {
		close(e.done)
	})
}

// ── Key generation ─────────────────────────────────────────────────────────

func coalesceKey(ctx fh.Ctx, includeBody bool, varyHeaders []string) string {
	h := sha256.New()
	h.Write([]byte(ctx.Method()))
	h.Write([]byte{0})
	// OriginalURL includes the query string, so two concurrent requests to
	// the same path differing only by query (e.g. ?q=foo vs ?q=bar) never
	// collide onto the same in-flight entry.
	h.Write([]byte(ctx.OriginalURL()))
	for _, name := range varyHeaders {
		h.Write([]byte{0})
		h.Write([]byte(strings.ToLower(name)))
		h.Write([]byte{0})
		for headerName, values := range ctx.GetReqHeaders() {
			if strings.EqualFold(headerName, name) {
				for _, value := range values {
					h.Write([]byte{0})
					h.Write([]byte(value))
				}
				break
			}
		}
	}

	if includeBody {
		h.Write([]byte{0})
		h.Write(ctx.Body())
	}

	return string(h.Sum(nil))
}
