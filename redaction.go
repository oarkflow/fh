package fh

import (
	"fmt"
	"reflect"
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

const maxRedactionDepth = 128

type redactionVisit struct {
	typ reflect.Type
	ptr uintptr
}

var sensitiveKeySeparators = strings.NewReplacer("-", "_", ".", "_", " ", "_")

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
	out, _ := r.redactValue(in, make(map[redactionVisit]struct{}), 0).(map[string]any)
	return out
}

func (r *Redactor) redactMap(in map[string]any, active map[redactionVisit]struct{}, depth int) any {
	if depth >= maxRedactionDepth {
		return "[REDACTION_DEPTH_LIMIT]"
	}
	rv := reflect.ValueOf(in)
	visit := redactionVisit{typ: rv.Type(), ptr: rv.Pointer()}
	if _, exists := active[visit]; exists {
		return "[CIRCULAR]"
	}
	active[visit] = struct{}{}
	defer delete(active, visit)

	out := make(map[string]any, len(in))
	for k, v := range in {
		if r != nil && r.isSensitive(k) {
			out[k] = r.Replacement
			continue
		}
		out[k] = r.redactValue(v, active, depth+1)
	}
	return out
}

// redactValue recurses into nested maps and slices so secrets are not missed
// when they appear inside arrays, e.g. {"users":[{"password":"x"}]}. Without
// this, RedactMap only inspected direct map values: slice/array elements were
// passed through unredacted into audit logs and other redacted output.
func (r *Redactor) redactValue(v any, active map[redactionVisit]struct{}, depth int) any {
	if depth >= maxRedactionDepth {
		return "[REDACTION_DEPTH_LIMIT]"
	}
	switch vv := v.(type) {
	case map[string]any:
		return r.redactMap(vv, active, depth)
	case string:
		return r.RedactString(vv)
	case []any:
		rv := reflect.ValueOf(vv)
		visit := redactionVisit{typ: rv.Type(), ptr: rv.Pointer()}
		if _, exists := active[visit]; exists {
			return "[CIRCULAR]"
		}
		active[visit] = struct{}{}
		defer delete(active, visit)
		out := make([]any, len(vv))
		for i, item := range vv {
			out[i] = r.redactValue(item, active, depth+1)
		}
		return out
	case []map[string]any:
		rv := reflect.ValueOf(vv)
		visit := redactionVisit{typ: rv.Type(), ptr: rv.Pointer()}
		if _, exists := active[visit]; exists {
			return "[CIRCULAR]"
		}
		active[visit] = struct{}{}
		defer delete(active, visit)
		out := make([]map[string]any, len(vv))
		for i, item := range vv {
			if item == nil {
				continue
			}
			out[i], _ = r.redactMap(item, active, depth+1).(map[string]any)
		}
		return out
	default:
		rv := reflect.ValueOf(v)
		if !rv.IsValid() {
			return v
		}
		if rv.Kind() == reflect.Interface {
			if rv.IsNil() {
				return nil
			}
			return r.redactValue(rv.Elem().Interface(), active, depth+1)
		}
		if rv.Kind() == reflect.Pointer {
			if rv.IsNil() {
				return nil
			}
			visit := redactionVisit{typ: rv.Type(), ptr: rv.Pointer()}
			if _, exists := active[visit]; exists {
				return "[CIRCULAR]"
			}
			active[visit] = struct{}{}
			defer delete(active, visit)
			return r.redactValue(rv.Elem().Interface(), active, depth+1)
		}
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			if rv.Kind() == reflect.Slice && !rv.IsNil() {
				visit := redactionVisit{typ: rv.Type(), ptr: rv.Pointer()}
				if _, exists := active[visit]; exists {
					return "[CIRCULAR]"
				}
				active[visit] = struct{}{}
				defer delete(active, visit)
			}
			n := rv.Len()
			out := make([]any, n)
			for i := 0; i < n; i++ {
				out[i] = r.redactValue(rv.Index(i).Interface(), active, depth+1)
			}
			return out
		}
		if rv.Kind() == reflect.Map && rv.Type().Key().Kind() == reflect.String {
			if rv.IsNil() {
				return nil
			}
			visit := redactionVisit{typ: rv.Type(), ptr: rv.Pointer()}
			if _, exists := active[visit]; exists {
				return "[CIRCULAR]"
			}
			active[visit] = struct{}{}
			defer delete(active, visit)
			out := make(map[string]any, rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				key := iter.Key().String()
				if r != nil && r.isSensitive(key) {
					out[key] = r.Replacement
				} else {
					out[key] = r.redactValue(iter.Value().Interface(), active, depth+1)
				}
			}
			return out
		}
		if rv.Kind() == reflect.Struct {
			out := make(map[string]any)
			rt := rv.Type()
			for i := 0; i < rt.NumField(); i++ {
				f := rt.Field(i)
				if !f.IsExported() {
					continue
				}
				fieldName := f.Name
				if r != nil && r.isSensitive(fieldName) {
					out[fieldName] = r.Replacement
				} else {
					out[fieldName] = r.redactValue(rv.Field(i).Interface(), active, depth+1)
				}
			}
			return out
		}
		return v
	}
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
	lk := sensitiveKeySeparators.Replace(strings.ToLower(k))
	for _, s := range r.Keys {
		ls := sensitiveKeySeparators.Replace(strings.ToLower(s))
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
