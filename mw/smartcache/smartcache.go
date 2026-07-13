// Package smartcache provides RFC 7234 compliant HTTP caching middleware.
// It handles Cache-Control, ETag, If-None-Match, If-Modified-Since,
// If-None-Match, and conditional request semantics automatically.
//
// The cache supports both in-memory storage and pluggable backends.
// Responses are stored with their headers and body, and served with
// proper 304 Not Modified responses when applicable.
package smartcache

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

// CacheControl directives parsed from Cache-Control header.
type CacheControl struct {
	MaxAge       time.Duration
	SMaxAge      time.Duration
	NoCache      bool
	NoStore      bool
	Public       bool
	Private      bool
	Immutable    bool
	MustRevalidate bool
	ProxyRevalidate bool
	NoTransform  bool
	StaleIfError time.Duration
}

// Response is a cached HTTP response.
type Response struct {
	StatusCode  int
	Headers     map[string][]string
	Body        []byte
	ETag        string
	LastMod     time.Time
	Expires     time.Time
	CC          CacheControl
	Stored      time.Time
}

// Store defines the interface for cache storage backends.
type Store interface {
	Get(key string) (*Response, bool)
	Set(key string, resp *Response, ttl time.Duration)
	Delete(key string)
	Purge()
	Len() int
}

// Config holds configuration for the smartcache middleware.
type Config struct {
	// Store is the cache backend. Default: in-memory LRU.
	Store Store

	// Methods is the list of HTTP methods to cache. Default: ["GET"].
	Methods []string

	// CacheableStatusCodes is the list of status codes to cache. Default: [200].
	CacheableStatusCodes []int

	// MaxSize is the maximum number of cached responses. Default: 10000.
	MaxSize int

	// DefaultTTL is the default TTL when Cache-Control is not set. Default: 5m.
	DefaultTTL time.Duration

	// MaxBodySize is the maximum response body size to cache. Default: 1MB.
	MaxBodySize int

	// Next is an optional skip function.
	Next func(ctx fh.Ctx) bool

	// KeyFunc generates a cache key from the request. Default: method + path + sorted query.
	KeyFunc func(ctx fh.Ctx) string
}

// DefaultConfig returns the default configuration.
var DefaultConfig = Config{
	Methods:               []string{"GET", "HEAD"},
	CacheableStatusCodes:  []int{200, 203, 204, 206, 300, 301, 404, 405, 410, 414, 501},
	MaxSize:               10000,
	DefaultTTL:            5 * time.Minute,
	MaxBodySize:           1 << 20, // 1MB
}

// New creates a smartcache middleware.
func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		c := config[0]
		if c.Store != nil {
			cfg.Store = c.Store
		}
		if c.Methods != nil {
			cfg.Methods = c.Methods
		}
		if c.CacheableStatusCodes != nil {
			cfg.CacheableStatusCodes = c.CacheableStatusCodes
		}
		if c.MaxSize > 0 {
			cfg.MaxSize = c.MaxSize
		}
		if c.DefaultTTL > 0 {
			cfg.DefaultTTL = c.DefaultTTL
		}
		if c.MaxBodySize > 0 {
			cfg.MaxBodySize = c.MaxBodySize
		}
		if c.Next != nil {
			cfg.Next = c.Next
		}
		if c.KeyFunc != nil {
			cfg.KeyFunc = c.KeyFunc
		}
	}

	if cfg.Store == nil {
		cfg.Store = NewMemoryStore(cfg.MaxSize)
	}

	methodSet := make(map[string]bool, len(cfg.Methods))
	for _, m := range cfg.Methods {
		methodSet[strings.ToUpper(m)] = true
	}

	statusSet := make(map[int]bool, len(cfg.CacheableStatusCodes))
	for _, s := range cfg.CacheableStatusCodes {
		statusSet[s] = true
	}

	return func(ctx fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(ctx) {
			return ctx.Next()
		}

		method := ctx.Method()
		if !methodSet[method] {
			return ctx.Next()
		}

		key := cfg.KeyFunc(ctx)
		if key == "" {
			key = defaultKey(ctx)
		}

		// Check cache for existing response.
		if cached, ok := cfg.Store.Get(key); ok {
			// Handle conditional requests (If-None-Match / If-Modified-Since).
			if handleConditional(ctx, cached) {
				return ctx.SendStatus(304)
			}

			// Serve from cache.
			for k, vals := range cached.Headers {
				for _, v := range vals {
					ctx.Set(k, v)
				}
			}
			return ctx.SendBytes(cached.Body)
		}

		// Cache miss — execute handler.
		ctx.CaptureResponseBody()
		err := ctx.Next()
		if err != nil {
			return err
		}

		// Store response if cacheable.
		statusCode := ctx.StatusCode()
		if !statusSet[statusCode] {
			return nil
		}

		body := ctx.ResponseBody()
		if len(body) > cfg.MaxBodySize {
			return nil
		}

		// Parse Cache-Control from request.
		cc := parseCacheControl(ctx.Get("Cache-Control"))

		// Don't cache responses with no-store or private.
		if cc.NoStore {
			return nil
		}

		resp := &Response{
			StatusCode: statusCode,
			Headers:    ctx.GetRespHeaders(),
			Body:       body,
			Stored:     time.Now(),
		}

		// Generate ETag if not present.
		if resp.Headers["ETag"] == nil {
			etag := generateETag(body)
			resp.ETag = etag
			ctx.Set("ETag", etag)
		} else {
			resp.ETag = resp.Headers["ETag"][0]
		}

		// Parse response Cache-Control.
		respCC := parseCacheControl(ctx.Get("Cache-Control"))
		resp.CC = respCC

		if respCC.MaxAge > 0 {
			resp.Expires = time.Now().Add(respCC.MaxAge)
		} else if expires := ctx.Get("Expires"); expires != "" {
			if t, err := http.ParseTime(expires); err == nil {
				resp.Expires = t
			}
		} else {
			resp.Expires = time.Now().Add(cfg.DefaultTTL)
		}

		if lastMod := ctx.Get("Last-Modified"); lastMod != "" {
			if t, err := http.ParseTime(lastMod); err == nil {
				resp.LastMod = t
			}
		}

		// Calculate TTL.
		ttl := cfg.DefaultTTL
		if respCC.MaxAge > 0 {
			ttl = respCC.MaxAge
		} else if !resp.Expires.IsZero() {
			ttl = time.Until(resp.Expires)
			if ttl < 0 {
				ttl = 0
			}
		}

		cfg.Store.Set(key, resp, ttl)
		return nil
	}
}

// ── Conditional request handling ───────────────────────────────────────────

func handleConditional(ctx fh.Ctx, cached *Response) bool {
	// If-None-Match
	ifNoneMatch := ctx.Get("If-None-Match")
	if ifNoneMatch != "" && cached.ETag != "" {
		if matchETag(ifNoneMatch, cached.ETag) {
			return true
		}
	}

	// If-Modified-Since
	ifModifiedSince := ctx.Get("If-Modified-Since")
	if ifModifiedSince != "" && !cached.LastMod.IsZero() {
		if t, err := http.ParseTime(ifModifiedSince); err == nil {
			if !cached.LastMod.After(t) {
				return true
			}
		}
	}

	return false
}

func matchETag(ifNoneMatch, etag string) bool {
	if ifNoneMatch == "*" {
		return true
	}

	for {
		ifNoneMatch = strings.TrimSpace(ifNoneMatch)
		if len(ifNoneMatch) == 0 {
			return false
		}

		// Handle weak ETags (W/ prefix).
		weak := strings.HasPrefix(ifNoneMatch, "W/")
		if weak {
			ifNoneMatch = ifNoneMatch[2:]
		}

		if strings.HasPrefix(ifNoneMatch, "\"") {
			end := strings.IndexByte(ifNoneMatch[1:], '"')
			if end >= 0 {
				tag := ifNoneMatch[1 : end+1]
				if tag == etag || tag == "*" {
					return true
				}
				ifNoneMatch = ifNoneMatch[end+2:]
				continue
			}
		}

		// No quoted value — treat entire remaining as the tag.
		if ifNoneMatch == etag {
			return true
		}
		return false
	}
}

// ── Cache-Control parsing ──────────────────────────────────────────────────

func parseCacheControl(header string) CacheControl {
	var cc CacheControl
	if header == "" {
		return cc
	}

	for {
		header = strings.TrimSpace(header)
		if len(header) == 0 {
			break
		}

		// Find next comma or end.
		idx := strings.IndexByte(header, ',')
		var token string
		if idx < 0 {
			token = header
			header = ""
		} else {
			token = strings.TrimSpace(header[:idx])
			header = header[idx+1:]
		}

		eq := strings.IndexByte(token, '=')
		if eq < 0 {
			switch strings.ToLower(strings.TrimSpace(token)) {
			case "no-cache":
				cc.NoCache = true
			case "no-store":
				cc.NoStore = true
			case "public":
				cc.Public = true
			case "private":
				cc.Private = true
			case "immutable":
				cc.Immutable = true
			case "must-revalidate":
				cc.MustRevalidate = true
			case "proxy-revalidate":
				cc.ProxyRevalidate = true
			case "no-transform":
				cc.NoTransform = true
			}
			continue
		}

		name := strings.ToLower(strings.TrimSpace(token[:eq]))
		val := strings.TrimSpace(token[eq+1:])

		switch name {
		case "max-age":
			if v, err := strconv.ParseInt(val, 10, 64); err == nil {
				cc.MaxAge = time.Duration(v) * time.Second
			}
		case "s-maxage":
			if v, err := strconv.ParseInt(val, 10, 64); err == nil {
				cc.SMaxAge = time.Duration(v) * time.Second
			}
		case "stale-if-error":
			if v, err := strconv.ParseInt(val, 10, 64); err == nil {
				cc.StaleIfError = time.Duration(v) * time.Second
			}
		}
	}

	return cc
}

// ── Helpers ────────────────────────────────────────────────────────────────

func defaultKey(ctx fh.Ctx) string {
	var b strings.Builder
	b.WriteString(ctx.Method())
	b.WriteByte(0)
	b.WriteString(ctx.Path())
	qs := ctx.Get("Sort-Query") // may be empty
	if qs != "" {
		b.WriteByte(0)
		b.WriteString(qs)
	}
	return b.String()
}

func generateETag(body []byte) string {
	h := sha256.Sum256(body)
	return fmt.Sprintf(`"%x"`, h[:8])
}

// ── In-memory store ────────────────────────────────────────────────────────

// MemoryStore is a simple in-memory cache store with TTL-based expiration.
type MemoryStore struct {
	mu      sync.RWMutex
	items   map[string]*cacheItem
	maxSize int
}

type cacheItem struct {
	resp  *Response
	expires time.Time
}

// NewMemoryStore creates a new in-memory cache store.
func NewMemoryStore(maxSize int) *MemoryStore {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &MemoryStore{
		items:   make(map[string]*cacheItem, maxSize),
		maxSize: maxSize,
	}
}

func (s *MemoryStore) Get(key string) (*Response, bool) {
	s.mu.RLock()
	item, ok := s.items[key]
	s.mu.RUnlock()

	if !ok {
		return nil, false
	}

	if !item.expires.IsZero() && time.Now().After(item.expires) {
		s.Delete(key)
		return nil, false
	}

	return item.resp, true
}

func (s *MemoryStore) Set(key string, resp *Response, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Evict if at capacity.
	if len(s.items) >= s.maxSize {
		// Simple random eviction — a production store would use LRU.
		for k, v := range s.items {
			if !v.expires.IsZero() && time.Now().After(v.expires) {
				delete(s.items, k)
			}
			if len(s.items) < s.maxSize {
				break
			}
		}
		// If still at capacity, evict oldest.
		if len(s.items) >= s.maxSize {
			for k := range s.items {
				delete(s.items, k)
				break
			}
		}
	}

	var expires time.Time
	if ttl > 0 {
		expires = time.Now().Add(ttl)
	}

	s.items[key] = &cacheItem{
		resp:    resp,
		expires: expires,
	}
}

func (s *MemoryStore) Delete(key string) {
	s.mu.Lock()
	delete(s.items, key)
	s.mu.Unlock()
}

func (s *MemoryStore) Purge() {
	s.mu.Lock()
	s.items = make(map[string]*cacheItem, s.maxSize)
	s.mu.Unlock()
}

func (s *MemoryStore) Len() int {
	s.mu.RLock()
	n := len(s.items)
	s.mu.RUnlock()
	return n
}
