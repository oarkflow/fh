package fh

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// Extractor resolves a value from the current request context. Extractors are
// intentionally small functions so applications can compose identity and
// authorization inputs from headers, route params, query strings, body fields,
// locals, sessions, JWT claims, or any other Ctx-backed source.
type Extractor[T any] func(Ctx) (T, bool, error)

type Principal struct {
	ID          string         `json:"id"`
	Type        string         `json:"type,omitempty"`
	TenantID    string         `json:"tenant_id,omitempty"`
	Subject     string         `json:"subject,omitempty"`
	Roles       []string       `json:"roles,omitempty"`
	Scopes      []string       `json:"scopes,omitempty"`
	Permissions []string       `json:"permissions,omitempty"`
	Claims      map[string]any `json:"claims,omitempty"`
	AuthMethod  string         `json:"auth_method,omitempty"`
}

type principalContextKey struct{}

const (
	principalLocalKey = "fh.principal"
	tenantLocalKey    = "tenant_id"
)

func SetPrincipal(c Ctx, p Principal) {
	if c == nil {
		return
	}
	if p.Type == "" {
		p.Type = "user"
	}
	c.Locals(principalLocalKey, p)
	ctx := context.WithValue(c.Context(), principalContextKey{}, p)
	c.SetContext(ctx)
}

func PrincipalFrom(c Ctx) (Principal, bool) {
	if c == nil {
		return Principal{}, false
	}
	if p, ok := c.Locals(principalLocalKey).(Principal); ok && p.ID != "" {
		return p, true
	}
	if p, ok := c.Context().Value(principalContextKey{}).(Principal); ok && p.ID != "" {
		return p, true
	}
	return Principal{}, false
}

// PrincipalExtractors resolves the standard Principal fields without assuming
// where they are stored. Empty extractors are skipped.
type PrincipalExtractors struct {
	ID          Extractor[string]
	Type        Extractor[string]
	TenantID    Extractor[string]
	Subject     Extractor[string]
	Roles       Extractor[[]string]
	Scopes      Extractor[[]string]
	Permissions Extractor[[]string]
	Claims      Extractor[map[string]any]
	AuthMethod  Extractor[string]
}

// ExtractPrincipal builds a Principal from the configured field extractors.
func ExtractPrincipal(c Ctx, ex PrincipalExtractors) (Principal, bool, error) {
	var p Principal
	var found bool
	var err error

	if p.ID, found, err = extractString(c, ex.ID); err != nil {
		return Principal{}, false, err
	}
	p.Type, _, err = extractString(c, ex.Type)
	if err != nil {
		return Principal{}, false, err
	}
	p.TenantID, _, err = extractString(c, ex.TenantID)
	if err != nil {
		return Principal{}, false, err
	}
	p.Subject, _, err = extractString(c, ex.Subject)
	if err != nil {
		return Principal{}, false, err
	}
	p.AuthMethod, _, err = extractString(c, ex.AuthMethod)
	if err != nil {
		return Principal{}, false, err
	}
	if p.Roles, _, err = extractStrings(c, ex.Roles); err != nil {
		return Principal{}, false, err
	}
	if p.Scopes, _, err = extractStrings(c, ex.Scopes); err != nil {
		return Principal{}, false, err
	}
	if p.Permissions, _, err = extractStrings(c, ex.Permissions); err != nil {
		return Principal{}, false, err
	}
	if ex.Claims != nil {
		if claims, ok, err := ex.Claims(c); err != nil {
			return Principal{}, false, err
		} else if ok {
			p.Claims = claims
		}
	}
	if !found || strings.TrimSpace(p.ID) == "" {
		return Principal{}, false, nil
	}
	if p.Type == "" {
		p.Type = "user"
	}
	return p, true, nil
}

// PrincipalExtractor returns a reusable extractor for a Principal composed from
// field extractors.
func PrincipalExtractor(ex PrincipalExtractors) Extractor[Principal] {
	return func(c Ctx) (Principal, bool, error) {
		return ExtractPrincipal(c, ex)
	}
}

// UsePrincipal resolves and stores a Principal for downstream middleware.
func UsePrincipal(ex Extractor[Principal], required ...bool) HandlerFunc {
	return func(c Ctx) error {
		if ex != nil {
			p, ok, err := ex(c)
			if err != nil {
				return err
			}
			if ok {
				SetPrincipal(c, p)
				return c.Next()
			}
		}
		if len(required) > 0 && required[0] {
			return NewHTTPError(StatusUnauthorized, "AUTH_REQUIRED", "authentication is required")
		}
		return c.Next()
	}
}

func TenantID(c Ctx) string {
	tenant, _, _ := TenantExtractor()(c)
	return tenant
}

func TenantExtractor(sources ...Extractor[string]) Extractor[string] {
	if len(sources) == 0 {
		sources = []Extractor[string]{
			PrincipalTenantExtractor(),
			LocalString(tenantLocalKey),
			HeaderString("X-Tenant-ID"),
		}
	}
	return FirstString(sources...)
}

func RequireAuth() HandlerFunc {
	return func(c Ctx) error {
		if _, ok := PrincipalFrom(c); !ok {
			EmitSecurityEvent(c, "auth.required_missing", nil)
			return NewHTTPError(StatusUnauthorized, "AUTH_REQUIRED", "authentication is required")
		}
		return c.Next()
	}
}

func RequireScope(scopes ...string) HandlerFunc {
	return RequireValues(PrincipalScopesExtractor(), "SCOPE_DENIED", "required scope is missing", "authz.scope_denied", scopes...)
}

func RequireRole(roles ...string) HandlerFunc {
	return RequireValues(PrincipalRolesExtractor(), "ROLE_DENIED", "required role is missing", "authz.role_denied", roles...)
}

func RequirePermission(permissions ...string) HandlerFunc {
	return RequireValues(PrincipalPermissionsExtractor(), "PERMISSION_DENIED", "required permission is missing", "authz.permission_denied", permissions...)
}

func RequireValues(ex Extractor[[]string], code, message, event string, required ...string) HandlerFunc {
	return func(c Ctx) error {
		values, ok, err := extractStrings(c, ex)
		if err != nil {
			return err
		}
		if !ok {
			return NewHTTPError(StatusUnauthorized, "AUTH_REQUIRED", "authentication is required")
		}
		for _, want := range required {
			if !hasString(values, want) {
				EmitSecurityEvent(c, event, map[string]any{"value": want})
				return NewHTTPError(StatusForbidden, code, message)
			}
		}
		return c.Next()
	}
}

func TenantResolver(header string, required bool) HandlerFunc {
	if header == "" {
		header = "X-Tenant-ID"
	}
	return TenantResolverWith(TenantExtractor(PrincipalTenantExtractor(), HeaderString(header)), required)
}

func TenantResolverWith(ex Extractor[string], required bool) HandlerFunc {
	return func(c Ctx) error {
		tenant, _, err := extractString(c, ex)
		if err != nil {
			return err
		}
		if tenant == "" && required {
			return NewHTTPError(StatusBadRequest, "TENANT_REQUIRED", "tenant is required")
		}
		if tenant != "" {
			c.Locals(tenantLocalKey, tenant)
		}
		return c.Next()
	}
}

func hasString(list []string, want string) bool {
	for _, v := range list {
		if strings.EqualFold(v, want) {
			return true
		}
	}
	return false
}

func StaticString(value string) Extractor[string] {
	return func(Ctx) (string, bool, error) { return value, value != "", nil }
}

func HeaderString(name string) Extractor[string] {
	return func(c Ctx) (string, bool, error) {
		if c == nil {
			return "", false, nil
		}
		v := strings.TrimSpace(c.Get(name))
		return v, v != "", nil
	}
}

func QueryString(name string) Extractor[string] {
	return func(c Ctx) (string, bool, error) {
		if c == nil {
			return "", false, nil
		}
		v := strings.TrimSpace(c.Query(name))
		return v, v != "", nil
	}
}

func ParamString(name string) Extractor[string] {
	return func(c Ctx) (string, bool, error) {
		if c == nil {
			return "", false, nil
		}
		v := strings.TrimSpace(c.Param(name))
		return v, v != "", nil
	}
}

func LocalString(key string) Extractor[string] {
	return func(c Ctx) (string, bool, error) {
		if c == nil {
			return "", false, nil
		}
		switch v := c.Locals(key).(type) {
		case string:
			v = strings.TrimSpace(v)
			return v, v != "", nil
		case fmt.Stringer:
			s := strings.TrimSpace(v.String())
			return s, s != "", nil
		default:
			return "", false, nil
		}
	}
}

func HeaderCSV(name string) Extractor[[]string] {
	return func(c Ctx) ([]string, bool, error) {
		if c == nil {
			return nil, false, nil
		}
		values := splitTrim(c.Get(name), ",")
		return values, len(values) > 0, nil
	}
}

func QueryCSV(name string) Extractor[[]string] {
	return func(c Ctx) ([]string, bool, error) {
		if c == nil {
			return nil, false, nil
		}
		values := splitTrim(c.Query(name), ",")
		return values, len(values) > 0, nil
	}
}

func BodyField(path string) Extractor[any] {
	parts := splitPath(path)
	return func(c Ctx) (any, bool, error) {
		if c == nil || len(c.Body()) == 0 || len(parts) == 0 {
			return nil, false, nil
		}
		var body any
		if err := json.Unmarshal(c.Body(), &body); err != nil {
			return nil, false, err
		}
		return lookupPath(body, parts)
	}
}

func BodyString(path string) Extractor[string] {
	return StringFrom(BodyField(path))
}

func BodyCSV(path string) Extractor[[]string] {
	return StringsFrom(BodyField(path))
}

func StringFrom(ex Extractor[any]) Extractor[string] {
	return func(c Ctx) (string, bool, error) {
		v, ok, err := extractAny(c, ex)
		if err != nil || !ok || v == nil {
			return "", false, err
		}
		switch val := v.(type) {
		case string:
			val = strings.TrimSpace(val)
			return val, val != "", nil
		case fmt.Stringer:
			s := strings.TrimSpace(val.String())
			return s, s != "", nil
		default:
			s := strings.TrimSpace(fmt.Sprint(val))
			return s, s != "", nil
		}
	}
}

func StringsFrom(ex Extractor[any]) Extractor[[]string] {
	return func(c Ctx) ([]string, bool, error) {
		v, ok, err := extractAny(c, ex)
		if err != nil || !ok || v == nil {
			return nil, false, err
		}
		values := stringsFromAny(v)
		return values, len(values) > 0, nil
	}
}

func FirstString(extractors ...Extractor[string]) Extractor[string] {
	return func(c Ctx) (string, bool, error) {
		for _, ex := range extractors {
			value, ok, err := extractString(c, ex)
			if err != nil || ok {
				return value, ok, err
			}
		}
		return "", false, nil
	}
}

func FirstStrings(extractors ...Extractor[[]string]) Extractor[[]string] {
	return func(c Ctx) ([]string, bool, error) {
		for _, ex := range extractors {
			value, ok, err := extractStrings(c, ex)
			if err != nil || ok {
				return value, ok, err
			}
		}
		return nil, false, nil
	}
}

func PrincipalTenantExtractor() Extractor[string] {
	return func(c Ctx) (string, bool, error) {
		if p, ok := PrincipalFrom(c); ok && p.TenantID != "" {
			return p.TenantID, true, nil
		}
		return "", false, nil
	}
}

func PrincipalRolesExtractor() Extractor[[]string] {
	return func(c Ctx) ([]string, bool, error) {
		p, ok := PrincipalFrom(c)
		if !ok {
			return nil, false, nil
		}
		return p.Roles, true, nil
	}
}

func PrincipalScopesExtractor() Extractor[[]string] {
	return func(c Ctx) ([]string, bool, error) {
		p, ok := PrincipalFrom(c)
		if !ok {
			return nil, false, nil
		}
		return p.Scopes, true, nil
	}
}

func PrincipalPermissionsExtractor() Extractor[[]string] {
	return func(c Ctx) ([]string, bool, error) {
		p, ok := PrincipalFrom(c)
		if !ok {
			return nil, false, nil
		}
		return p.Permissions, true, nil
	}
}

func extractString(c Ctx, ex Extractor[string]) (string, bool, error) {
	if ex == nil {
		return "", false, nil
	}
	v, ok, err := ex(c)
	if !ok {
		return "", false, err
	}
	v = strings.TrimSpace(v)
	return v, v != "", err
}

func extractStrings(c Ctx, ex Extractor[[]string]) ([]string, bool, error) {
	if ex == nil {
		return nil, false, nil
	}
	values, ok, err := ex(c)
	if err != nil || !ok {
		return nil, false, err
	}
	values = cleanStrings(values)
	return values, len(values) > 0, nil
}

func extractAny(c Ctx, ex Extractor[any]) (any, bool, error) {
	if ex == nil {
		return nil, false, nil
	}
	return ex(c)
}

func extractMap(c Ctx, ex Extractor[map[string]any]) (map[string]any, bool, error) {
	if ex == nil {
		return nil, false, nil
	}
	return ex(c)
}

func splitTrim(s, sep string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func cleanStrings(values []string) []string {
	out := values[:0]
	for _, value := range values {
		for _, part := range splitTrim(value, ",") {
			out = append(out, part)
		}
	}
	return out
}

func stringsFromAny(v any) []string {
	switch value := v.(type) {
	case []string:
		return cleanStrings(value)
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			out = append(out, strings.TrimSpace(fmt.Sprint(item)))
		}
		return cleanStrings(out)
	case string:
		return splitTrim(value, ",")
	default:
		s := strings.TrimSpace(fmt.Sprint(value))
		if s == "" {
			return nil
		}
		return []string{s}
	}
}

func splitPath(path string) []string {
	raw := strings.FieldsFunc(path, func(r rune) bool { return r == '.' || r == '/' })
	parts := raw[:0]
	for _, part := range raw {
		part = strings.TrimSpace(part)
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func lookupPath(v any, parts []string) (any, bool, error) {
	cur := v
	for _, part := range parts {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false, nil
		}
		cur, ok = obj[part]
		if !ok {
			return nil, false, nil
		}
	}
	return cur, true, nil
}
