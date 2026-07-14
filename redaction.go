package fh

import (
	"fmt"
	"strings"
)

type RedactionMode string

const (
	RedactReplace RedactionMode = "replace"
	RedactPartial RedactionMode = "partial"
)

type RedactionConfig struct {
	Enabled     bool          `json:"enabled"`
	Fields      []string      `json:"fields,omitempty"`
	Replacement string        `json:"replacement,omitempty"`
	Mode        RedactionMode `json:"mode,omitempty"`
}

func DefaultRedactionConfig() RedactionConfig {
	return RedactionConfig{Enabled: true, Replacement: "[REDACTED]", Mode: RedactReplace, Fields: []string{"password", "passwd", "secret", "token", "access_token", "refresh_token", "authorization", "cookie", "set-cookie", "api_key", "apikey", "private_key", "client_secret", "ssn", "card_number", "cvv", "otp", "mfa_code"}}
}

func NewRedactor(cfg RedactionConfig) *Redactor {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Replacement == "" {
		cfg.Replacement = "[REDACTED]"
	}
	if len(cfg.Fields) == 0 {
		cfg.Fields = DefaultRedactionConfig().Fields
	}
	return &Redactor{Keys: cfg.Fields, Replacement: cfg.Replacement}
}

func (r *Redactor) RedactMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		if r != nil && r.isSensitive(k) {
			out[k] = r.Replacement
			continue
		}
		switch vv := v.(type) {
		case map[string]any:
			out[k] = r.RedactMap(vv)
		case string:
			out[k] = r.RedactString(vv)
		default:
			out[k] = vv
		}
	}
	return out
}

func (r *Redactor) RedactHeaders(in map[string][]string) map[string][]string {
	if in == nil {
		return nil
	}
	out := make(map[string][]string, len(in))
	for k, values := range in {
		if r != nil && r.isSensitive(k) {
			out[k] = []string{r.Replacement}
			continue
		}
		out[k] = append([]string(nil), values...)
	}
	return out
}

func (r *Redactor) isSensitive(k string) bool {
	if r == nil {
		return false
	}
	lk := strings.ToLower(k)
	for _, s := range r.Keys {
		ls := strings.ToLower(s)
		if lk == ls || strings.Contains(lk, ls) {
			return true
		}
	}
	return false
}

func MaskEmail(v string) string {
	at := strings.IndexByte(v, '@')
	if at <= 1 {
		return "***"
	}
	return v[:1] + "***" + v[at:]
}

func MaskCard(v string) string {
	if len(v) <= 4 {
		return "****"
	}
	return strings.Repeat("*", len(v)-4) + v[len(v)-4:]
}

var cachedDefaultRedactor = NewRedactor(DefaultRedactionConfig())

func RedactValue(key string, value any) any {
	if cachedDefaultRedactor.isSensitive(key) {
		return cachedDefaultRedactor.Replacement
	}
	return fmt.Sprint(value)
}
