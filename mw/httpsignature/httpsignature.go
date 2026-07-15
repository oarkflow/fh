// Package httpsignature signs FH responses using the Ed25519 response profile
// implemented by pkg/httpsignature and the wire format defined by RFC 9421.
package httpsignature

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	protocol "github.com/oarkflow/fh/pkg/httpsignature"
)

type NonceStore interface {
	CheckAndStore(key string, expiresAt time.Time) (accepted bool, err error)
}

type Config struct {
	PrivateKey ed25519.PrivateKey
	KeyID      string
	Label      string
	// Origin is the externally visible request origin, such as
	// https://api.example.com. It is required so @target-uri is not derived from
	// attacker-controlled forwarding headers.
	Origin string
	// AllowedOrigins adds exact alternative external origins. The request Host
	// selects one of these validated origins; unlisted hosts fail closed.
	AllowedOrigins []string

	Validity    time.Duration
	MaxBodySize int
	NonceStore  NonceStore
	Now         func() time.Time
	Skip        func(fh.Ctx) bool
}

func New(config Config) (fh.HandlerFunc, error) {
	if len(config.PrivateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("http signature middleware: a valid Ed25519 private key is required")
	}
	if config.KeyID == "" || len(config.KeyID) > 128 || strings.ContainsAny(config.KeyID, "\x00\r\n") {
		return nil, errors.New("http signature middleware: a valid key ID is required")
	}
	config.Label = defaultString(config.Label, protocol.DefaultLabel)
	origins := make([]*url.URL, 0, len(config.AllowedOrigins)+1)
	for _, rawOrigin := range append([]string{config.Origin}, config.AllowedOrigins...) {
		origin, err := parseOrigin(rawOrigin)
		if err != nil {
			return nil, err
		}
		duplicate := false
		for _, existing := range origins {
			if strings.EqualFold(existing.String(), origin.String()) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			origins = append(origins, origin)
		}
	}
	if config.Validity <= 0 {
		config.Validity = 90 * time.Second
	}
	if config.Validity > 5*time.Minute {
		return nil, errors.New("http signature middleware: validity must not exceed five minutes")
	}
	if config.MaxBodySize <= 0 {
		config.MaxBodySize = 16 << 20
	}
	if config.NonceStore == nil {
		config.NonceStore = NewMemoryNonceStore(250_000)
	}
	privateKey := append(ed25519.PrivateKey(nil), config.PrivateKey...)
	config.PrivateKey = nil

	return func(c fh.Ctx) error {
		if config.Skip != nil && config.Skip(c) {
			return c.Next()
		}
		request, err := protocol.ParseAcceptSignature(c.Get(protocol.HeaderAcceptSignature), config.Label)
		if err != nil || request.KeyID != config.KeyID {
			return fh.NewHTTPError(fh.StatusBadRequest, "HTTP_SIGNATURE_REQUIRED", "a supported RFC 9421 Accept-Signature request is required")
		}
		now := time.Now()
		if config.Now != nil {
			now = config.Now()
		}
		accepted, err := config.NonceStore.CheckAndStore(config.KeyID+":"+request.Nonce, now.Add(config.Validity))
		if err != nil {
			return fh.NewHTTPError(fh.StatusServiceUnavailable, "HTTP_SIGNATURE_NONCE_STORE", "response signature nonce could not be reserved")
		}
		if !accepted {
			return fh.NewHTTPError(fh.StatusConflict, "HTTP_SIGNATURE_NONCE_REPLAY", "response signature nonce was already used")
		}
		selectedOrigin, err := selectOrigin(origins, c.Get(fh.HeaderHostStr))
		if err != nil {
			return fh.NewHTTPError(fh.StatusMisdirectedRequest, "HTTP_SIGNATURE_ORIGIN", "request host is not an allowed signing origin")
		}
		targetURI, err := requestTargetURI(selectedOrigin, c.OriginalURL())
		if err != nil {
			return fh.NewHTTPError(fh.StatusBadRequest, "HTTP_SIGNATURE_TARGET", "request target cannot be signed")
		}
		method := c.Method()
		// Register at response time so ordinary transforms installed by
		// downstream middleware run before the digest and signature are created.
		c.OnBeforeResponse(func(c fh.Ctx) error {
			c.AddBodyTransform(func(body []byte) ([]byte, error) {
				if len(body) > config.MaxBodySize {
					return nil, protocol.ErrPolicy
				}
				if encoding := strings.TrimSpace(c.GetRespHeader("Content-Encoding")); encoding != "" {
					return nil, errors.New("http signature middleware: Content-Encoding is unsupported by the negotiated profile")
				}
				content := body
				status := c.StatusCode()
				if method == fh.MethodHEADStr || status == fh.StatusNoContent || status == fh.StatusResetContent || status == fh.StatusNotModified || status >= 100 && status < 200 {
					content = nil
				}
				contentType := strings.TrimSpace(c.GetRespHeader(fh.HeaderContentTypeStr))
				if contentType == "" {
					contentType = "application/octet-stream"
					c.Type(contentType)
				}
				created := time.Now()
				if config.Now != nil {
					created = config.Now()
				}
				params := protocol.Parameters{
					Created: created.Unix(),
					Expires: created.Add(config.Validity).Unix(),
					Nonce:   request.Nonce,
					KeyID:   config.KeyID,
					Alg:     protocol.Algorithm,
					Tag:     protocol.DefaultTag,
				}
				digest, input, signature, err := protocol.SignResponse(privateKey, config.Label, params, status, contentType, method, targetURI, content)
				if err != nil {
					return nil, err
				}
				c.Set(protocol.HeaderContentDigest, digest)
				c.Set(protocol.HeaderSignatureInput, input)
				c.Set(protocol.HeaderSignature, signature)
				c.Set("Cache-Control", "no-store")
				c.Append("Vary", protocol.HeaderAcceptSignature)
				return body, nil
			})
			return nil
		})
		return c.Next()
	}, nil
}

func parseOrigin(raw string) (*url.URL, error) {
	origin, err := url.Parse(strings.TrimRight(strings.TrimSpace(raw), "/"))
	if err != nil || origin.Scheme == "" || origin.Host == "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") {
		return nil, errors.New("http signature middleware: Origin must be an absolute origin without path, query, credentials, or fragment")
	}
	if origin.Scheme != "https" && !isLoopback(origin.Hostname()) {
		return nil, errors.New("http signature middleware: HTTPS is required outside loopback development")
	}
	origin.Path = ""
	return origin, nil
}

func selectOrigin(origins []*url.URL, host string) (string, error) {
	host = strings.TrimSpace(host)
	for _, origin := range origins {
		if strings.EqualFold(origin.Host, host) {
			return strings.TrimRight(origin.String(), "/"), nil
		}
	}
	return "", protocol.ErrPolicy
}

func requestTargetURI(origin, target string) (string, error) {
	if target == "" || strings.ContainsAny(target, "\x00\r\n#") {
		return "", protocol.ErrPolicy
	}
	if strings.HasPrefix(target, "/") {
		return origin + target, nil
	}
	parsed, err := url.Parse(target)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return "", protocol.ErrPolicy
	}
	want, _ := url.Parse(origin)
	if !strings.EqualFold(parsed.Scheme, want.Scheme) || !strings.EqualFold(parsed.Host, want.Host) {
		return "", protocol.ErrPolicy
	}
	return parsed.String(), nil
}

func isLoopback(host string) bool {
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "0.0.0.0", "::1":
		return true
	default:
		return false
	}
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

type nonceEntry struct{ expiresAt time.Time }

type MemoryNonceStore struct {
	mu         sync.Mutex
	entries    map[string]nonceEntry
	maxEntries int
}

func NewMemoryNonceStore(maxEntries int) *MemoryNonceStore {
	if maxEntries <= 0 {
		maxEntries = 250_000
	}
	return &MemoryNonceStore{entries: make(map[string]nonceEntry), maxEntries: maxEntries}
}

func (s *MemoryNonceStore) CheckAndStore(key string, expiresAt time.Time) (bool, error) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) >= s.maxEntries {
		for key, entry := range s.entries {
			if !entry.expiresAt.After(now) {
				delete(s.entries, key)
			}
		}
	}
	if len(s.entries) >= s.maxEntries {
		return false, fmt.Errorf("http signature nonce store capacity exhausted")
	}
	if entry, exists := s.entries[key]; exists && entry.expiresAt.After(now) {
		return false, nil
	}
	s.entries[key] = nonceEntry{expiresAt: expiresAt}
	return true, nil
}
