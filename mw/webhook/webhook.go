package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
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
		return errors.New("missing signature")
	}
	if cfg.Prefix != "" {
		p := cfg.Prefix + "="
		if !strings.HasPrefix(sig, p) {
			return errors.New("signature prefix mismatch")
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
		return errors.New("missing secret")
	}
	body := c.BodyRaw()
	ts := c.Get(cfg.TimestampHeader)
	msg := body
	if ts != "" {
		t, err := parseTime(ts)
		if err != nil {
			return err
		}
		if cfg.Tolerance > 0 && time.Since(t) > cfg.Tolerance {
			return errors.New("timestamp outside tolerance")
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
		return errors.New("signature mismatch")
	}
	if cfg.Replay != nil && ts != "" {
		if cfg.Replay.Seen(sig+":"+ts, cfg.Tolerance) {
			return errors.New("signature replayed")
		}
	}
	return nil
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
