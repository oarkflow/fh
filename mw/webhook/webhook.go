package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

type SecretFunc func(fh.Ctx) ([]byte, error)
type ReplayStore interface {
	Seen(key string, ttl time.Duration) bool
}
type ErrorHandler func(fh.Ctx, error) error

type Config struct {
	Secret          []byte
	SecretFunc      SecretFunc
	Header          string
	TimestampHeader string
	Tolerance       time.Duration
	Prefix          string
	Algorithm       string
	Replay          ReplayStore
	Error           ErrorHandler
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.Header == "" {
		cfg.Header = "X-Signature"
	}
	if cfg.TimestampHeader == "" {
		cfg.TimestampHeader = "X-Timestamp"
	}
	if cfg.Tolerance == 0 {
		cfg.Tolerance = 5 * time.Minute
	}
	if cfg.Algorithm == "" {
		cfg.Algorithm = "sha256"
	}
	if cfg.Replay == nil {
		// Without a replay store, a captured (signature, timestamp) pair
		// can be resent as many times as an attacker likes within the
		// tolerance window. Default to a bounded in-memory store so replay
		// protection applies out of the box; pass a distributed ReplayStore
		// (e.g. Redis-backed) for multi-instance deployments.
		cfg.Replay = newMemoryReplayStore()
	}
	if cfg.Error == nil {
		cfg.Error = func(c fh.Ctx, err error) error {
			return c.Status(fh.StatusUnauthorized).JSON(fh.Map{"error": "webhook_signature_invalid", "message": err.Error()})
		}
	}
	return func(c fh.Ctx) error {
		if err := Verify(c, cfg); err != nil {
			return cfg.Error(c, err)
		}
		return c.Next()
	}
}
func Verify(c fh.Ctx, cfg Config) error {
	sig := strings.TrimSpace(c.Get(cfg.Header))
	if sig == "" {
		return fmt.Errorf("missing signature in header %q", cfg.Header)
	}
	if cfg.Prefix != "" {
		p := cfg.Prefix + "="
		if !strings.HasPrefix(sig, p) {
			return fmt.Errorf("signature prefix mismatch: expected %q", p)
		}
		sig = strings.TrimPrefix(sig, p)
	} else if i := strings.IndexByte(sig, '='); i > 0 {
		sig = sig[i+1:]
	}
	key := cfg.Secret
	var err error
	if cfg.SecretFunc != nil {
		key, err = cfg.SecretFunc(c)
		if err != nil {
			return err
		}
	}
	if len(key) == 0 {
		return fmt.Errorf("missing secret: both Secret and SecretFunc are empty")
	}
	body := c.BodyRaw()
	ts := c.Get(cfg.TimestampHeader)
	msg := body
	if ts != "" {
		t, err := parseTime(ts)
		if err != nil {
			return fmt.Errorf("invalid timestamp in %q: %w", cfg.TimestampHeader, err)
		}
		if cfg.Tolerance > 0 && time.Since(t) > cfg.Tolerance {
			return fmt.Errorf("timestamp outside tolerance: %v ago exceeds %v", time.Since(t), cfg.Tolerance)
		}
		joined := make([]byte, 0, len(ts)+1+len(body))
		joined = append(joined, ts...)
		joined = append(joined, '.')
		joined = append(joined, body...)
		msg = joined
	}
	h, ok := algo(cfg.Algorithm)
	if !ok {
		return fmt.Errorf("unsupported algorithm %s", cfg.Algorithm)
	}
	mac := hmac.New(h, key)
	mac.Write(msg)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(strings.ToLower(sig)), []byte(expected)) {
		return fmt.Errorf("signature mismatch for algorithm %s", cfg.Algorithm)
	}
	if cfg.Replay != nil && ts != "" {
		if cfg.Replay.Seen(sig+":"+ts, cfg.Tolerance) {
			return fmt.Errorf("signature replayed: seen within %v tolerance", cfg.Tolerance)
		}
	}
	return nil
}
// memoryReplayStore is the default ReplayStore used when Config.Replay is
// unset. It is process-local; deployments running multiple instances behind
// a load balancer should supply a shared store (e.g. Redis-backed) instead.
type memoryReplayStore struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func newMemoryReplayStore() *memoryReplayStore {
	return &memoryReplayStore{seen: map[string]time.Time{}}
}

func (s *memoryReplayStore) Seen(key string, ttl time.Duration) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, exp := range s.seen {
		if exp.Before(now) {
			delete(s.seen, k)
		}
	}
	if exp, ok := s.seen[key]; ok && exp.After(now) {
		return true
	}
	s.seen[key] = now.Add(ttl)
	return false
}

func algo(a string) (func() hash.Hash, bool) {
	switch strings.ToLower(a) {
	case "sha256":
		return sha256.New, true
	case "sha384":
		return sha512.New384, true
	case "sha512":
		return sha512.New, true
	default:
		return nil, false
	}
}
func parseTime(s string) (time.Time, error) {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		if i > 1e12 {
			return time.UnixMilli(i), nil
		}
		return time.Unix(i, 0), nil
	}
	return time.Parse(time.RFC3339, s)
}
