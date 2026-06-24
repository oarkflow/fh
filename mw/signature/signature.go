package signature

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

type SecretResolver func(ctx *fh.Ctx, keyID string) [][]byte

type Config struct {
	Secrets         [][]byte
	Secret          []byte
	Resolve         SecretResolver
	SignatureHeader string
	TimestampHeader string
	KeyIDHeader     string
	Scheme          string
	Tolerance       time.Duration
	SignedPayload   func(*fh.Ctx, string) []byte
	Next            func(*fh.Ctx) bool
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
	return func(c *fh.Ctx) error {
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
				return c.Next()
			}
		}
		return fh.NewHTTPError(fh.StatusUnauthorized, "SIGNATURE_INVALID", "signature is invalid")
	}
}

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
