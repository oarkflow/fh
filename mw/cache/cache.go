// Package cache provides bounded in-memory HTTP response caching.
package cache

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

type Entry struct {
	Status           int
	ContentType      string
	Body             []byte
	Created, Expires time.Time
}

const maxCachedBodySize = 8 << 20

const maxStoredEntrySize = maxCachedBodySize + maxCachedBodySize/2 + 4096

type Config struct {
	TTL                 time.Duration
	MaxBodySize         int
	MaxEntries          int
	Methods             []string
	VaryHeaders         []string
	AllowRequestCookies bool
	Store               kv.Store
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
		cfg.Store = kv.NewMemoryStore(kv.WithMaxEntries(cfg.MaxEntries))
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
		if entry, ok := Get(cfg.Store, key); ok {
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
				Set(cfg.Store, key, Entry{Status: c.StatusCode(), ContentType: c.ResponseHeader("Content-Type"), Body: copyBody, Created: now, Expires: now.Add(cfg.TTL)})
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

func Get(store kv.Store, key string) (Entry, bool) {
	data, ok, err := store.Get(key)
	if err != nil || !ok {
		return Entry{}, false
	}
	var e Entry
	if json.Unmarshal(data, &e) != nil {
		return Entry{}, false
	}
	return e, true
}

func Set(store kv.Store, key string, e Entry) {
	if len(e.Body) > maxCachedBodySize {
		return
	}
	data, err := json.Marshal(&e)
	if err != nil {
		return
	}
	ttl := time.Until(e.Expires)
	if ttl <= 0 {
		ttl = time.Nanosecond
	}
	_ = store.Set(key, data, ttl)
}

func Delete(store kv.Store, key string) {
	_ = store.Delete(key)
}
