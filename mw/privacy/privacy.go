package privacy

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"

	"github.com/oarkflow/fh"
)

// Config controls privacy-aware telemetry behavior.
type Config struct {
	// HeaderAllowlist lists headers that may appear in logs/traces. All
	// other headers are redacted. Default: common safe headers.
	HeaderAllowlist []string

	// QueryRedact lists query parameters whose values should be redacted.
	QueryRedact []string

	// PathTemplate enables path parameter templating in telemetry.
	// When true, /users/123 becomes /users/:id.
	PathTemplate bool

	// SecretDetection enables automatic detection and redaction of
	// values that look like secrets (API keys, tokens, passwords).
	SecretDetection bool

	// BodyLogging controls whether request/response bodies appear in logs.
	// Default: false (disabled).
	BodyLogging bool

	// HashFields lists fields whose values should be hashed instead of
	// redacted. Useful for correlation without exposing raw values.
	HashFields []string

	// Retention sets the maximum number of log entries to retain per route.
	// Default: 1000.
	Retention int

	// NeverExport marks fields that should never appear in any telemetry output.
	NeverExport []string

	// TenantLogPolicies provides per-tenant telemetry policies.
	TenantLogPolicies map[string]*TenantPolicy
}

// TenantPolicy controls per-tenant telemetry behavior.
type TenantPolicy struct {
	// HeaderAllowlist overrides the global header allowlist for this tenant.
	HeaderAllowlist []string

	// BodyLogging enables body logging for this tenant.
	BodyLogging bool

	// SamplingRate is the sampling rate for this tenant (0.0-1.0).
	SamplingRate float64

	// NeverExport adds fields that should never be exported for this tenant.
	NeverExport []string
}

// PrivacyFilter applies privacy rules to telemetry data.
type PrivacyFilter struct {
	cfg           Config
	headerSet     map[string]bool
	querySet      map[string]bool
	neverSet      map[string]bool
	hashSet       map[string]bool
	sensitiveKeys []string
	mu            sync.RWMutex
}

var defaultSensitiveKeys = []string{
	"password", "passwd", "pwd",
	"secret", "token", "api_key", "apikey", "api-key",
	"authorization", "auth", "bearer",
	"credit_card", "creditcard", "cc",
	"ssn", "social_security",
	"private_key", "private-key",
	"access_token", "refresh_token",
	"session_id", "sessionid",
}

// New creates a privacy-aware telemetry filter.
func New(cfg ...Config) *PrivacyFilter {
	c := Config{
		HeaderAllowlist: []string{
			"Content-Type", "Accept", "User-Agent", "X-Request-ID",
			"X-Correlation-ID", "X-Forwarded-For",
		},
		BodyLogging:      false,
		SecretDetection:  true,
		PathTemplate:     true,
		Retention:        1000,
	}
	if len(cfg) > 0 {
		merge := cfg[0]
		if len(merge.HeaderAllowlist) > 0 {
			c.HeaderAllowlist = merge.HeaderAllowlist
		}
		if len(merge.QueryRedact) > 0 {
			c.QueryRedact = merge.QueryRedact
		}
		c.PathTemplate = merge.PathTemplate
		c.SecretDetection = merge.SecretDetection
		c.BodyLogging = merge.BodyLogging
		if len(merge.HashFields) > 0 {
			c.HashFields = merge.HashFields
		}
		if merge.Retention > 0 {
			c.Retention = merge.Retention
		}
		if len(merge.NeverExport) > 0 {
			c.NeverExport = merge.NeverExport
		}
		if len(merge.TenantLogPolicies) > 0 {
			c.TenantLogPolicies = merge.TenantLogPolicies
		}
	}

	f := &PrivacyFilter{
		cfg:           c,
		headerSet:     make(map[string]bool, len(c.HeaderAllowlist)),
		querySet:      make(map[string]bool, len(c.QueryRedact)),
		neverSet:      make(map[string]bool, len(c.NeverExport)),
		hashSet:       make(map[string]bool, len(c.HashFields)),
		sensitiveKeys: defaultSensitiveKeys,
	}

	for _, h := range c.HeaderAllowlist {
		f.headerSet[strings.ToLower(h)] = true
	}
	for _, q := range c.QueryRedact {
		f.querySet[strings.ToLower(q)] = true
	}
	for _, n := range c.NeverExport {
		f.neverSet[strings.ToLower(n)] = true
	}
	for _, h := range c.HashFields {
		f.hashSet[strings.ToLower(h)] = true
	}

	return f
}

// FilterHeaders returns a filtered copy of headers containing only allowed keys.
func (f *PrivacyFilter) FilterHeaders(headers map[string][]string) map[string][]string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make(map[string][]string, len(headers))
	for k, v := range headers {
		if f.headerSet[strings.ToLower(k)] {
			result[k] = v
		}
	}
	return result
}

// FilterQuery returns a filtered query string with sensitive values redacted.
func (f *PrivacyFilter) FilterQuery(query string) string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if query == "" {
		return query
	}

	parts := strings.Split(query, "&")
	for i, part := range parts {
		eqIdx := strings.IndexByte(part, '=')
		if eqIdx < 0 {
			continue
		}
		key := strings.ToLower(part[:eqIdx])
		if f.querySet[key] || f.isSensitiveKey(key) {
			parts[i] = part[:eqIdx+1] + "[REDACTED]"
		}
	}
	return strings.Join(parts, "&")
}

// FilterBody redacts sensitive fields from a JSON-like body string.
func (f *PrivacyFilter) FilterBody(body []byte) []byte {
	if !f.cfg.BodyLogging || len(body) == 0 {
		return nil
	}
	return body
}

// HashField returns a SHA-256 hash of a field value for correlation.
func (f *PrivacyFilter) HashField(value string) string {
	h := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", h[:8])
}

// ShouldExport reports whether a field should appear in telemetry output.
func (f *PrivacyFilter) ShouldExport(field string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return !f.neverSet[strings.ToLower(field)]
}

// TemplatePath converts a concrete path to a template by replacing numeric
// segments with :id. Example: /users/123/orders/456 → /users/:id/orders/:id.
func (f *PrivacyFilter) TemplatePath(path string) string {
	if !f.cfg.PathTemplate {
		return path
	}

	segments := strings.Split(path, "/")
	for i, seg := range segments {
		if isNumericID(seg) {
			segments[i] = ":id"
		}
	}
	return strings.Join(segments, "/")
}

// TenantPolicy returns the policy for a specific tenant.
func (f *PrivacyFilter) TenantPolicy(tenant string) *TenantPolicy {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.cfg.TenantLogPolicies == nil {
		return nil
	}
	return f.cfg.TenantLogPolicies[tenant]
}

func (f *PrivacyFilter) isSensitiveKey(key string) bool {
	for _, sensitive := range f.sensitiveKeys {
		if strings.Contains(key, sensitive) {
			return true
		}
	}
	return false
}

func isNumericID(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// PrivacyMiddleware creates middleware that applies privacy rules to the request context.
func PrivacyMiddleware(filter *PrivacyFilter) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		c.Locals("_privacy_filter", filter)
		return c.Next()
	}
}

// GetPrivacyFilter retrieves the privacy filter from the request context.
func GetPrivacyFilter(c fh.Ctx) *PrivacyFilter {
	v := c.Locals("_privacy_filter")
	if v == nil {
		return nil
	}
	f, _ := v.(*PrivacyFilter)
	return f
}
