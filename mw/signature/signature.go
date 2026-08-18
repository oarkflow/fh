package signature

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

type SecretResolver func(ctx fh.Ctx, keyID string) [][]byte

type Config struct {
	Secrets          [][]byte
	Secret           []byte
	Resolve          SecretResolver
	SignatureHeader  string
	TimestampHeader  string
	KeyIDHeader      string
	Scheme           string
	Tolerance        time.Duration
	MaxReplayEntries int
	SignedPayload    func(fh.Ctx, string) []byte
	// Replay guards against a captured signature being resent within the
	// tolerance window. Defaults to a bounded in-memory kv.Store; supply a
	// distributed kv.Store for multi-instance deployments.
	Replay kv.Store
	Next   func(fh.Ctx) bool
}

func New(config Config) fh.HandlerFunc {
	if config.SignatureHeader == "" {
		config.SignatureHeader = "X-Signature"
	}
	if config.TimestampHeader == "" {
		config.TimestampHeader = "X-Timestamp"
	}
	if config.Scheme == "" {
		config.Scheme = "sha256="
	}
	if config.Tolerance <= 0 {
		config.Tolerance = 5 * time.Minute
	}
	if config.Replay == nil {
		config.Replay = kv.NewMemoryStore(kv.WithShardCount(1))
	}
	var replayMu sync.Mutex
	return func(c fh.Ctx) error {
		if config.Next != nil && config.Next(c) {
			return c.Next()
		}
		ts := c.Get(config.TimestampHeader)
		sig := strings.TrimSpace(c.Get(config.SignatureHeader))
		if parsedTS, parsedSig, ok := parseCombinedSignature(sig); ok {
			if ts == "" {
				ts = parsedTS
			}
			sig = parsedSig
		}
		if config.Scheme != "" {
			sig = strings.TrimPrefix(sig, config.Scheme)
		}
		if ts == "" || sig == "" {
			return fh.NewHTTPError(fh.StatusUnauthorized, "SIGNATURE_MISSING", "signature or timestamp is missing")
		}
		when, err := parseTimestamp(ts)
		if err != nil {
			return fh.NewHTTPError(fh.StatusUnauthorized, "SIGNATURE_BAD_TIMESTAMP", "signature timestamp is invalid")
		}
		delta := time.Since(when)
		if delta < 0 {
			delta = -delta
		}
		if delta > config.Tolerance {
			return fh.NewHTTPError(fh.StatusUnauthorized, "SIGNATURE_STALE", "signature timestamp is outside the accepted window")
		}
		keyID := ""
		if config.KeyIDHeader != "" {
			keyID = c.Get(config.KeyIDHeader)
		}
		secrets := config.Secrets
		if len(secrets) == 0 && len(config.Secret) > 0 {
			secrets = [][]byte{config.Secret}
		}
		if config.Resolve != nil {
			secrets = config.Resolve(c, keyID)
		}
		payload := []byte(ts + ".")
		if config.SignedPayload != nil {
			payload = config.SignedPayload(c, ts)
		} else {
			payload = append(payload, c.Body()...)
		}
		for _, secret := range secrets {
			mac := hmac.New(sha256.New, secret)
			mac.Write(payload)
			if hmac.Equal([]byte(sig), []byte(hex.EncodeToString(mac.Sum(nil)))) {
				replayTTL := time.Until(when.Add(config.Tolerance))
				if replayTTL <= 0 {
					return fh.NewHTTPError(fh.StatusConflict, "SIGNATURE_REPLAYED", "signature has already been used")
				}
				replayed, err := Seen(config.Replay, &replayMu, sig+":"+ts, replayTTL, config.MaxReplayEntries)
				if err != nil {
					return fh.NewHTTPError(fh.StatusServiceUnavailable, "SIGNATURE_STORE_ERROR", "signature replay store error")
				}
				if replayed {
					return fh.NewHTTPError(fh.StatusConflict, "SIGNATURE_REPLAYED", "signature has already been used")
				}
				return c.Next()
			}
		}
		return fh.NewHTTPError(fh.StatusUnauthorized, "SIGNATURE_INVALID", "signature is invalid")
	}
}

func Seen(store kv.Store, mu *sync.Mutex, key string, ttl time.Duration, maxEntries ...int) (bool, error) {
	maxSize := 100000
	if len(maxEntries) > 0 && maxEntries[0] > 0 {
		maxSize = maxEntries[0]
	}
	if mu != nil {
		mu.Lock()
		defer mu.Unlock()
	}
	if _, exists, err := store.Get(key); err != nil {
		return false, err
	} else if exists {
		return true, nil
	}
	n, err := store.Len()
	if err != nil {
		return false, err
	}
	if n >= maxSize {
		return false, ErrStoreFull
	}
	if err := store.Set(key, []byte{1}, ttl); err != nil {
		return false, err
	}
	return false, nil
}

var ErrStoreFull = errors.New("signature: replay store full")

func parseCombinedSignature(value string) (timestamp, signature string, ok bool) {
	for _, part := range strings.Split(value, ",") {
		key, val, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "t":
			timestamp = strings.TrimSpace(val)
		case "sig", "v1":
			signature = strings.TrimSpace(val)
		}
	}
	return timestamp, signature, timestamp != "" && signature != ""
}

func parseTimestamp(value string) (time.Time, error) {
	if unix, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Unix(unix, 0), nil
	}
	return time.Parse(time.RFC3339, value)
}
