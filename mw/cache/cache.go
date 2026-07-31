// Package cache provides bounded in-memory HTTP response caching.
package cache

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
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

// MemoryStore is a Store backed by kv.MemoryStore: it JSON-encodes each
// Entry and delegates the map/lock/expiry mechanics to the shared kv
// primitive instead of reimplementing them.
//
// kv.Store has no way to enumerate its keys, which the original
// oldest-entry-first eviction policy needs. Rather than reimplement kv's
// map+mutex storage locally to get that enumeration, MemoryStore keeps a
// small side index of key -> Entry.Created purely for eviction ranking; the
// authoritative entry data still lives in kv.
type MemoryStore struct {
	store      *kv.MemoryStore
	maxEntries int

	mu      sync.Mutex
	created map[string]time.Time // key -> Entry.Created, for oldest-eviction ranking only
}

func NewMemoryStore(maxEntries ...int) *MemoryStore {
	max := 1024
	if len(maxEntries) > 0 && maxEntries[0] > 0 {
		max = maxEntries[0]
	}
	return &MemoryStore{
		store:      kv.NewMemoryStore(),
		maxEntries: max,
		created:    make(map[string]time.Time),
	}
}
func (s *MemoryStore) Get(key string) (Entry, bool) {
	data, ok, err := s.store.Get(key)
	if err != nil || !ok {
		s.mu.Lock()
		delete(s.created, key)
		s.mu.Unlock()
		return Entry{}, false
	}
	var e Entry
	if json.Unmarshal(data, &e) != nil {
		return Entry{}, false
	}
	return e, true
}
func (s *MemoryStore) Set(key string, e Entry) {
	data, err := json.Marshal(&e)
	if err != nil {
		return
	}
	ttl := time.Until(e.Expires)
	if ttl <= 0 {
		ttl = time.Nanosecond
	}

	s.mu.Lock()
	if _, exists := s.created[key]; !exists && len(s.created) >= s.maxEntries {
		s.evictOldestLocked()
	}
	s.created[key] = e.Created
	s.mu.Unlock()

	if err := s.store.Set(key, data, ttl); err != nil {
		s.mu.Lock()
		delete(s.created, key)
		s.mu.Unlock()
	}
}

// evictOldestLocked removes the single tracked key with the oldest Created
// time. Caller holds s.mu.
func (s *MemoryStore) evictOldestLocked() {
	var oldestKey string
	var oldest time.Time
	first := true
	for candidate, created := range s.created {
		if first || created.Before(oldest) {
			oldestKey, oldest = candidate, created
			first = false
		}
	}
	if oldestKey != "" {
		delete(s.created, oldestKey)
		_ = s.store.Delete(oldestKey)
	}
}

func (s *MemoryStore) Delete(key string) {
	s.mu.Lock()
	delete(s.created, key)
	s.mu.Unlock()
	_ = s.store.Delete(key)
}
