// Package contentdigest implements RFC 9530 Content-Digest verification and
// response generation. It is opt-in and does not affect fh's default hot path.
package contentdigest

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strings"

	"github.com/oarkflow/fh"
)

const (
	SHA256 = "sha-256"
	SHA512 = "sha-512"
)

var (
	ErrMalformedDigest   = errors.New("content-digest: malformed field")
	ErrUnsupportedDigest = errors.New("content-digest: no supported algorithm")
	ErrDigestMismatch    = errors.New("content-digest: digest mismatch")
)

type ResponseMode uint8

const (
	ResponseNone ResponseMode = iota
	ResponseWhenRequested
	ResponseAlways
)

type Config struct {
	RequireRequest          bool
	SkipRequestVerification bool
	Response                ResponseMode
	ResponseAlgorithm       string
	Next                    func(fh.Ctx) bool
}

// New verifies a request Content-Digest whenever the field is present. It can
// require the field and can add a response digest always or when requested by
// Want-Content-Digest.
func New(config ...Config) fh.HandlerFunc {
	cfg := Config{}
	if len(config) > 0 {
		cfg = config[0]
	}
	algorithm := normalizeAlgorithm(cfg.ResponseAlgorithm)
	if algorithm == "" {
		algorithm = SHA256
	}
	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		field := strings.TrimSpace(c.Get(fh.HeaderContentDigest))
		if field == "" {
			if cfg.RequireRequest {
				return fh.NewHTTPError(fh.StatusBadRequest, "CONTENT_DIGEST_REQUIRED", "Content-Digest is required")
			}
		} else if !cfg.SkipRequestVerification {
			if err := Verify(field, c.Body()); err != nil {
				return fh.NewHTTPError(fh.StatusBadRequest, "CONTENT_DIGEST_INVALID", "Content-Digest verification failed").WithCause(err)
			}
		}

		addResponse := cfg.Response == ResponseAlways ||
			(cfg.Response == ResponseWhenRequested && Wants(c.Get(fh.HeaderWantContentDigest), algorithm))
		if addResponse {
			// Register at the last possible point so earlier body transforms (for
			// example compression) run first and the digest covers actual content.
			c.OnBeforeResponse(func(ctx fh.Ctx) error {
				ctx.AddBodyTransform(func(body []byte) ([]byte, error) {
					value, err := Format(algorithm, body)
					if err != nil {
						return nil, err
					}
					ctx.Set(fh.HeaderContentDigest, value)
					return body, nil
				})
				return nil
			})
		}
		return c.Next()
	}
}

// Format returns an RFC 9530 dictionary member containing a Byte Sequence.
func Format(algorithm string, content []byte) (string, error) {
	algorithm = normalizeAlgorithm(algorithm)
	var digest []byte
	switch algorithm {
	case SHA256:
		sum := sha256.Sum256(content)
		digest = sum[:]
	case SHA512:
		sum := sha512.Sum512(content)
		digest = sum[:]
	default:
		return "", ErrUnsupportedDigest
	}
	return algorithm + "=:" + base64.StdEncoding.EncodeToString(digest) + ":", nil
}

// Verify accepts an RFC 9530 dictionary with one or more members and succeeds
// when at least one supported digest matches content.
func Verify(field string, content []byte) error {
	foundSupported := false
	for _, member := range splitMembers(field) {
		key, raw, ok := strings.Cut(member, "=")
		if !ok {
			return ErrMalformedDigest
		}
		algorithm := normalizeAlgorithm(strings.TrimSpace(key))
		if algorithm != SHA256 && algorithm != SHA512 {
			continue
		}
		foundSupported = true
		raw = strings.TrimSpace(raw)
		if len(raw) < 3 || raw[0] != ':' {
			return ErrMalformedDigest
		}
		end := strings.IndexByte(raw[1:], ':')
		if end < 0 {
			return ErrMalformedDigest
		}
		end++
		encoded := raw[1:end]
		tail := strings.TrimSpace(raw[end+1:])
		if tail != "" && tail[0] != ';' {
			return ErrMalformedDigest
		}
		provided, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil {
			return ErrMalformedDigest
		}
		expectedField, _ := Format(algorithm, content)
		expectedEncoded := expectedField[len(algorithm)+2 : len(expectedField)-1]
		expected, _ := base64.StdEncoding.DecodeString(expectedEncoded)
		if len(provided) == len(expected) && subtle.ConstantTimeCompare(provided, expected) == 1 {
			return nil
		}
	}
	if !foundSupported {
		return ErrUnsupportedDigest
	}
	return ErrDigestMismatch
}

// Wants reports whether an RFC 9530 preference dictionary mentions algorithm
// with a non-zero preference.
func Wants(field, algorithm string) bool {
	algorithm = normalizeAlgorithm(algorithm)
	for _, member := range splitMembers(field) {
		member = strings.TrimSpace(member)
		end := len(member)
		if i := strings.IndexAny(member, "=;"); i >= 0 {
			end = i
		}
		if normalizeAlgorithm(member[:end]) != algorithm {
			continue
		}
		if i := strings.IndexByte(member, '='); i >= 0 {
			value := strings.TrimSpace(member[i+1:])
			return value != "0" && value != "0.0" && value != "?0"
		}
		return true
	}
	return false
}

func splitMembers(field string) []string {
	if strings.TrimSpace(field) == "" {
		return nil
	}
	return strings.Split(field, ",")
}

func normalizeAlgorithm(algorithm string) string {
	return strings.ToLower(strings.TrimSpace(algorithm))
}
