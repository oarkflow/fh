// Package cache provides bounded in-memory HTTP response caching.
package cache

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

type Entry struct {
	Status           int
	ContentType      string
	Body             []byte
	Created, Expires time.Time
}
type Store interface {
	Get(string) (Entry, bool)
	Set(string, Entry)
	Delete(string)
}
type Config struct {
	TTL                 time.Duration
	MaxBodySize         int
	MaxEntries          int
	Methods             []string
	VaryHeaders         []string
	AllowRequestCookies bool
	Store               Store
	KeyGenerator        func(fh.Ctx) string
	Next                func(fh.Ctx) bool
}

var DefaultConfig = Config{TTL: time.Minute, MaxBodySize: 1 << 20, MaxEntries: 1024, Methods: []string{"GET", "HEAD"}}

func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		merge(&cfg, config[0])
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore(cfg.MaxEntries)
	}
	if cfg.KeyGenerator == nil {
		cfg.KeyGenerator = func(c fh.Ctx) string {
			return c.Method() + " " + string(c.RequestHeader().Host) + " " + string(c.RequestHeader().URI)
		}
	}
	methods := make(map[string]struct{}, len(cfg.Methods))
	for _, m := range cfg.Methods {
		methods[strings.ToUpper(m)] = struct{}{}
	}
	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		requestCacheControl := strings.ToLower(c.Get("Cache-Control"))
		if _, ok := methods[c.Method()]; !ok || c.Get("Authorization") != "" || !cfg.AllowRequestCookies && c.Get("Cookie") != "" || strings.Contains(requestCacheControl, "no-cache") || strings.Contains(requestCacheControl, "no-store") {
			return c.Next()
		}
		key := cfg.KeyGenerator(c)
		for _, h := range cfg.VaryHeaders {
			key += "\x00" + strings.ToLower(h) + "=" + c.Get(h)
		}
		if entry, ok := cfg.Store.Get(key); ok {
			c.Set("Age", strconv.Itoa(int(time.Since(entry.Created).Seconds())))
			c.Set("X-Cache", "HIT")
			if entry.ContentType != "" {
				c.Type(entry.ContentType)
			}
			return c.Status(entry.Status).SendBytes(entry.Body)
		}
		c.Set("X-Cache", "MISS")
		c.AddBodyTransform(func(body []byte) ([]byte, error) {
			cacheControl := strings.ToLower(c.ResponseHeader("Cache-Control"))
			if c.StatusCode() == fh.StatusOK && len(body) <= cfg.MaxBodySize && !c.HasResponseCookies() && !strings.Contains(cacheControl, "no-store") && !strings.Contains(cacheControl, "private") {
				now := time.Now()
				copyBody := append([]byte(nil), body...)
				cfg.Store.Set(key, Entry{Status: c.StatusCode(), ContentType: c.ResponseHeader("Content-Type"), Body: copyBody, Created: now, Expires: now.Add(cfg.TTL)})
			}
			return body, nil
		})
		return c.Next()
	}
}

func merge(dst *Config, src Config) {
	if src.TTL > 0 {
		dst.TTL = src.TTL
	}
	if src.MaxBodySize > 0 {
		dst.MaxBodySize = src.MaxBodySize
	}
	if src.MaxEntries > 0 {
		dst.MaxEntries = src.MaxEntries
	}
	if src.Methods != nil {
		dst.Methods = src.Methods
	}
	if src.VaryHeaders != nil {
		dst.VaryHeaders = src.VaryHeaders
	}
	if src.Store != nil {
		dst.Store = src.Store
	}
	if src.KeyGenerator != nil {
		dst.KeyGenerator = src.KeyGenerator
	}
	dst.Next = src.Next
	dst.AllowRequestCookies = src.AllowRequestCookies
}

type MemoryStore struct {
	mu         sync.RWMutex
	entries    map[string]Entry
	maxEntries int
}

func NewMemoryStore(maxEntries ...int) *MemoryStore {
	max := 1024
	if len(maxEntries) > 0 && maxEntries[0] > 0 {
		max = maxEntries[0]
	}
	return &MemoryStore{entries: make(map[string]Entry), maxEntries: max}
}
func (s *MemoryStore) Get(key string) (Entry, bool) {
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok || time.Now().After(e.Expires) {
		if ok {
			s.Delete(key)
		}
		return Entry{}, false
	}
	e.Body = append([]byte(nil), e.Body...)
	return e, true
}
func (s *MemoryStore) Set(key string, e Entry) {
	s.mu.Lock()
	if _, exists := s.entries[key]; !exists && len(s.entries) >= s.maxEntries {
		var oldestKey string
		var oldest time.Time
		for candidate, entry := range s.entries {
			if oldestKey == "" || entry.Created.Before(oldest) {
				oldestKey, oldest = candidate, entry.Created
			}
		}
		delete(s.entries, oldestKey)
	}
	s.entries[key] = e
	s.mu.Unlock()
}
func (s *MemoryStore) Delete(key string) { s.mu.Lock(); delete(s.entries, key); s.mu.Unlock() }
