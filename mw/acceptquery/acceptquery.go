// Package acceptquery advertises and enforces HTTP QUERY request formats as
// defined by RFC 10008.
package acceptquery

import (
	"errors"
	"mime"
	"sort"
	"strings"

	"github.com/oarkflow/fh"
)

type Config struct {
	MediaTypes         []string
	Enforce            bool
	RequireContentType bool
	Next               func(fh.Ctx) bool
}

// New returns route middleware that emits Accept-Query. When Enforce is true,
// QUERY requests using an unadvertised Content-Type are rejected with 415.
func New(cfg Config) fh.HandlerFunc {
	field, accepted, err := Build(cfg.MediaTypes)
	if err != nil {
		panic(err)
	}
	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		c.Set(fh.HeaderAcceptQuery, field)
		if cfg.Enforce && c.Method() == fh.MethodQUERYStr {
			contentType := strings.TrimSpace(c.Get(fh.HeaderContentTypeStr))
			if contentType == "" {
				if cfg.RequireContentType || len(c.Body()) > 0 {
					return fh.NewHTTPError(fh.StatusUnsupportedMediaType, "QUERY_CONTENT_TYPE_REQUIRED", "QUERY Content-Type is required")
				}
			} else {
				base, _, parseErr := mime.ParseMediaType(contentType)
				if parseErr != nil || !containsFold(accepted, base) {
					return fh.NewHTTPError(fh.StatusUnsupportedMediaType, "QUERY_CONTENT_TYPE_UNSUPPORTED", "QUERY Content-Type is not supported")
				}
			}
		}
		return c.Next()
	}
}

// Build converts media types into the Structured Fields List syntax required
// by Accept-Query and also returns normalized base media types for enforcement.
func Build(mediaTypes []string) (field string, accepted []string, err error) {
	if len(mediaTypes) == 0 {
		return "", nil, errors.New("acceptquery: at least one media type is required")
	}
	items := make([]string, 0, len(mediaTypes))
	accepted = make([]string, 0, len(mediaTypes))
	seen := make(map[string]struct{}, len(mediaTypes))
	for _, raw := range mediaTypes {
		base, params, parseErr := mime.ParseMediaType(strings.TrimSpace(raw))
		if parseErr != nil || !strings.Contains(base, "/") {
			return "", nil, errors.New("acceptquery: invalid media type " + raw)
		}
		base = strings.ToLower(base)
		if _, ok := seen[base]; !ok {
			seen[base] = struct{}{}
			accepted = append(accepted, base)
		}
		item := structuredTokenOrString(base)
		keys := make([]string, 0, len(params))
		for key := range params {
			keys = append(keys, strings.ToLower(key))
		}
		sort.Strings(keys)
		for _, key := range keys {
			item += ";" + key + "=" + structuredString(params[key])
		}
		items = append(items, item)
	}
	return strings.Join(items, ", "), accepted, nil
}

func structuredTokenOrString(value string) string {
	if isStructuredToken(value) {
		return value
	}
	return structuredString(value)
}

func structuredString(value string) string {
	var b strings.Builder
	b.Grow(len(value) + 2)
	b.WriteByte('"')
	for i := 0; i < len(value); i++ {
		if value[i] == '"' || value[i] == '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte(value[i])
	}
	b.WriteByte('"')
	return b.String()
}

func isStructuredToken(value string) bool {
	if value == "" || !((value[0] >= 'A' && value[0] <= 'Z') || (value[0] >= 'a' && value[0] <= 'z') || value[0] == '*') {
		return false
	}
	for i := 1; i < len(value); i++ {
		c := value[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~:/", rune(c)) {
			continue
		}
		return false
	}
	return true
}

func containsFold(values []string, value string) bool {
	for _, candidate := range values {
		if strings.EqualFold(candidate, value) {
			return true
		}
	}
	return false
}
