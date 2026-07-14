package requestdedup

import (
	"crypto/sha256"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

// Config controls deduplication behavior.
type Config struct {
	// Window is the time window during which duplicate requests are detected.
	// Default: 5 seconds.
	Window time.Duration

	// MaxKeys is the maximum number of dedup keys stored before eviction.
	// Default: 10000.
	MaxKeys int

	// KeyFunc extracts the dedup key from the request. The default computes
	// SHA-256(method + path + body).
	KeyFunc func(fh.Ctx) string

	// OnDuplicate is called when a duplicate request is detected. The default
	// returns 409 Conflict with the original request's response.
	OnDuplicate func(fh.Ctx, *Entry) error
}

// Entry represents a tracked request.
type Entry struct {
	Key        string
	Status     int
	Body       []byte
	Headers    map[string]string
	ReceivedAt time.Time
	ExpiresAt  time.Time
}

// Deduplicator tracks in-flight requests and deduplicates within a window.
type Deduplicator struct {
	cfg     Config
	mu      sync.RWMutex
	entries map[string]*Entry
	order   []string
}

// New creates a deduplicator. If cfg is zero, defaults are applied.
func New(cfg ...Config) *Deduplicator {
	c := defaultConfig()
	if len(cfg) > 0 {
		c = mergeConfig(c, cfg[0])
	}
	return &Deduplicator{
		cfg:     c,
		entries: make(map[string]*Entry, c.MaxKeys),
	}
}

func defaultConfig() Config {
	return Config{
		Window:  5 * time.Second,
		MaxKeys: 10000,
		KeyFunc: defaultKeyFunc,
		OnDuplicate: func(c fh.Ctx, e *Entry) error {
			c.Set("X-Dedup-Key", e.Key)
			c.Set("X-Dedup-Received", e.ReceivedAt.Format(time.RFC3339Nano))
			return c.Status(fh.StatusConflict).JSON(fh.Map{
				"error":     "duplicate_request",
				"key":       e.Key,
				"received":  e.ReceivedAt.Format(time.RFC3339Nano),
				"expires":   e.ExpiresAt.Format(time.RFC3339Nano),
			})
		},
	}
}

func mergeConfig(base, override Config) Config {
	if override.Window > 0 {
		base.Window = override.Window
	}
	if override.MaxKeys > 0 {
		base.MaxKeys = override.MaxKeys
	}
	if override.KeyFunc != nil {
		base.KeyFunc = override.KeyFunc
	}
	if override.OnDuplicate != nil {
		base.OnDuplicate = override.OnDuplicate
	}
	return base
}

func defaultKeyFunc(c fh.Ctx) string {
	h := sha256.New()
	h.Write(c.MethodBytes())
	h.Write([]byte{' '})
	h.Write([]byte(c.OriginalURL()))
	if body := c.BodyRaw(); len(body) > 0 {
		h.Write(body)
	}
	return hexDigest(h.Sum(nil))
}

func hexDigest(b []byte) string {
	const hex = "0123456789abcdef"
	dst := make([]byte, len(b)*2)
	for i, v := range b {
		dst[i*2] = hex[v>>4]
		dst[i*2+1] = hex[v&0x0f]
	}
	return string(dst)
}

// Handler returns middleware that deduplicates requests within the configured window.
func (d *Deduplicator) Handler() fh.HandlerFunc {
	return func(c fh.Ctx) error {
		d.cleanup()

		key := d.cfg.KeyFunc(c)
		if key == "" {
			return c.Next()
		}

		now := time.Now()
		expires := now.Add(d.cfg.Window)

		d.mu.Lock()
		if existing, ok := d.entries[key]; ok && now.Before(existing.ExpiresAt) {
			d.mu.Unlock()
			return d.cfg.OnDuplicate(c, existing)
		}

		d.entries[key] = &Entry{
			Key:        key,
			ReceivedAt: now,
			ExpiresAt:  expires,
		}
		d.order = append(d.order, key)

		if len(d.entries) > d.cfg.MaxKeys {
			oldest := d.order[0]
			d.order = d.order[1:]
			delete(d.entries, oldest)
		}
		d.mu.Unlock()

		err := c.Next()

		d.mu.Lock()
		if entry, ok := d.entries[key]; ok {
			entry.Status = c.StatusCode()
			entry.Headers = make(map[string]string)
			for k, v := range c.GetRespHeaders() {
				if len(v) > 0 {
					entry.Headers[k] = v[0]
				}
			}
		}
		d.mu.Unlock()

		return err
	}
}

func (d *Deduplicator) cleanup() {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()

	cutoff := 0
	for cutoff < len(d.order) {
		key := d.order[cutoff]
		if entry, ok := d.entries[key]; ok && now.After(entry.ExpiresAt) {
			delete(d.entries, key)
			cutoff++
		} else {
			break
		}
	}
	if cutoff > 0 {
		d.order = d.order[cutoff:]
	}
}

// Stats returns current deduplication statistics.
func (d *Deduplicator) Stats() (active int, evictions int) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.entries), 0
}

// New is a convenience function that creates and returns middleware.
func NewMiddleware(cfg ...Config) fh.HandlerFunc {
	return New(cfg...).Handler()
}
