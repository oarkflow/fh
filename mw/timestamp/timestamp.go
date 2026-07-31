package timestamp

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

type Config struct {
	Header         string
	NonceHeader    string
	MaxSkew        time.Duration
	MaxSize        int
	MaxNonceLength int
	Store          ReplayStore
	KeyFunc        func(fh.Ctx) string
	Reject         func(fh.Ctx, string) error
	Skip           func(fh.Ctx) bool
	Required       bool
	RequireNonce   bool
}

type ReplayStore interface {
	Seen(key string, ttl time.Duration) (bool, error)
}

var ErrReplayStoreFull = errors.New("timestamp: replay store capacity exhausted")

func New(cfg Config) (fh.HandlerFunc, func()) {
	cfg = normalize(cfg)
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if ms, ok := cfg.Store.(*MemoryStore); ok {
					ms.cleanup()
				}
			case <-stop:
				return
			}
		}
	}()

	handler := func(c fh.Ctx) error {
		if cfg.Skip != nil && cfg.Skip(c) {
			return c.Next()
		}

		ts := c.Get(cfg.Header)
		if ts == "" && cfg.Required {
			return cfg.Reject(c, "missing timestamp header")
		}

		if ts != "" {
			nonce := c.Get(cfg.NonceHeader)
			if nonce == "" && cfg.RequireNonce {
				return cfg.Reject(c, "missing request nonce")
			}
			if len(nonce) > cfg.MaxNonceLength {
				return cfg.Reject(c, "request nonce too long")
			}
			timestamp, err := strconv.ParseInt(ts, 10, 64)
			if err != nil {
				return cfg.Reject(c, "invalid timestamp format")
			}

			reqTime := time.Unix(timestamp, 0)
			skew := time.Since(reqTime)
			if skew < 0 {
				skew = -skew
			}

			if skew > cfg.MaxSkew {
				return cfg.Reject(c, "request timestamp outside acceptable window")
			}

			key := cfg.KeyFunc(c) + ":" + ts + ":" + nonce
			// A future timestamp remains acceptable until timestamp+MaxSkew.
			// Retain its replay marker for that entire interval.
			ttl := time.Until(reqTime.Add(cfg.MaxSkew))
			if ttl <= 0 {
				return cfg.Reject(c, "request timestamp outside acceptable window")
			}
			seen, err := cfg.Store.Seen(key, ttl)
			if err != nil {
				return err
			}
			if seen {
				return cfg.Reject(c, "duplicate request detected")
			}
		}

		return c.Next()
	}

	var stopOnce sync.Once
	shutdown := func() { stopOnce.Do(func() { close(stop) }) }
	return handler, shutdown
}

func normalize(cfg Config) Config {
	if cfg.Header == "" {
		cfg.Header = "X-Request-Timestamp"
	}
	if cfg.NonceHeader == "" {
		cfg.NonceHeader = "X-Request-Nonce"
	}
	if cfg.MaxSkew <= 0 {
		cfg.MaxSkew = 5 * time.Minute
	}
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = 100000
	}
	if cfg.MaxNonceLength <= 0 {
		cfg.MaxNonceLength = 128
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore(cfg.MaxSize)
	}
	if cfg.KeyFunc == nil {
		cfg.KeyFunc = func(c fh.Ctx) string {
			return c.IP() + ":" + c.Method() + ":" + c.Path()
		}
	}
	if cfg.Reject == nil {
		cfg.Reject = func(c fh.Ctx, msg string) error {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{
				"error":  "timestamp_validation_failed",
				"detail": msg,
			})
		}
	}
	return cfg
}

// seenMarker is the placeholder value written for a replay marker; only its
// presence and TTL matter, never its content.
var seenMarker = []byte{1}

// MemoryStore is an in-process ReplayStore. It is a thin adapter over
// kv.MemoryStore for the actual map/TTL/expiry mechanics; MemoryStore itself
// only adds the "reject when at capacity rather than evict a live marker"
// policy that kv.Store's generic capacity handling does not provide (see
// package doc note in file_store.go for details on that gap).
type MemoryStore struct {
	mu      sync.Mutex
	store   *kv.MemoryStore
	maxSize int
}

func NewMemoryStore(maxSize int) *MemoryStore {
	if maxSize <= 0 {
		maxSize = 100000
	}
	// A single shard mirrors the original design's single mutex-guarded map:
	// MemoryStore serializes all access itself (see Seen below), so there is
	// no concurrency benefit to sharding here, and a single shard keeps
	// capacity accounting exact rather than spread unevenly across shards.
	return &MemoryStore{
		store:   kv.NewMemoryStore(kv.WithShardCount(1)),
		maxSize: maxSize,
	}
}

func (s *MemoryStore) Seen(key string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists, err := s.store.Get(key)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	// Len() sweeps expired entries as a side effect, mirroring the original
	// evictExpired-then-recheck sequence.
	n, err := s.store.Len()
	if err != nil {
		return false, err
	}
	if n >= s.maxSize {
		return false, ErrReplayStoreFull
	}

	if err := s.store.Set(key, seenMarker, ttl); err != nil {
		return false, err
	}
	return false, nil
}

func (s *MemoryStore) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.store.Len()
}
