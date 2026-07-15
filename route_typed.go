package fh

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Validator is implemented by request DTOs that can validate themselves.
type Validator interface{ Validate() error }

type StructuredError struct {
	Error     string       `json:"error"`
	Message   string       `json:"message,omitempty"`
	RequestID string       `json:"request_id,omitempty"`
	Fields    []FieldError `json:"fields,omitempty"`
}

// PostTyped registers a typed JSON endpoint. Go does not support generic methods,
// so the handler is supplied as a typed function with this shape:
//
//	func(fh.Ctx, CreateUserRequest) (UserResponse, error)
//
// fh validates that shape at registration time and builds the parsing/encoding wrapper.
func (a *App) GetTyped(path string, handler any, middleware ...HandlerFunc) *App {
	return a.addTyped("GET", path, handler, middleware...)
}
func (a *App) HeadTyped(path string, handler any, middleware ...HandlerFunc) *App {
	return a.addTyped("HEAD", path, handler, middleware...)
}
func (a *App) PostTyped(path string, handler any, middleware ...HandlerFunc) *App {
	return a.addTyped("POST", path, handler, middleware...)
}
func (a *App) PutTyped(path string, handler any, middleware ...HandlerFunc) *App {
	return a.addTyped("PUT", path, handler, middleware...)
}
func (a *App) PatchTyped(path string, handler any, middleware ...HandlerFunc) *App {
	return a.addTyped("PATCH", path, handler, middleware...)
}
func (a *App) DeleteTyped(path string, handler any, middleware ...HandlerFunc) *App {
	return a.addTyped("DELETE", path, handler, middleware...)
}
func (a *App) OptionsTyped(path string, handler any, middleware ...HandlerFunc) *App {
	return a.addTyped("OPTIONS", path, handler, middleware...)
}
func (a *App) ConnectTyped(path string, handler any, middleware ...HandlerFunc) *App {
	return a.addTyped("CONNECT", path, handler, middleware...)
}
func (a *App) TraceTyped(path string, handler any, middleware ...HandlerFunc) *App {
	return a.addTyped("TRACE", path, handler, middleware...)
}
func (a *App) QueryTyped(path string, handler any, middleware ...HandlerFunc) *App {
	return a.addTyped("QUERY", path, handler, middleware...)
}
func (a *App) AllTyped(path string, handler any, middleware ...HandlerFunc) *App {
	for _, m := range []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE", "QUERY"} {
		a.addTyped(m, path, handler, middleware...)
	}
	return a
}

func (a *App) addTyped(method, path string, handler any, middleware ...HandlerFunc) *App {
	hv := reflect.ValueOf(handler)
	ht := hv.Type()
	ctxType := reflect.TypeFor[Ctx]()
	errType := reflect.TypeFor[error]()
	if ht.Kind() != reflect.Func || ht.NumIn() != 2 || ht.In(0) != ctxType || ht.NumOut() != 2 || !ht.Out(1).Implements(errType) {
		panic("fh: typed handler must be func(fh.Ctx, Req) (Res, error)")
	}
	reqType, resType := ht.In(1), ht.Out(0)
	h := func(c Ctx) error {
		reqPtr := reflect.New(reqType)
		if len(c.Body()) != 0 {
			if err := c.BodyParser(reqPtr.Interface()); err != nil {
				return c.Status(StatusBadRequest).JSON(StructuredError{Error: "invalid_body", Message: err.Error(), RequestID: requestIDFromCtx(c)})
			}
		}
		if err := bindTaggedFields(c, reqPtr.Elem()); err != nil {
			return c.Status(StatusBadRequest).JSON(StructuredError{Error: "invalid_request", Message: err.Error(), RequestID: requestIDFromCtx(c)})
		}
		if err := ValidateStruct(reqPtr.Interface()); err != nil {
			return typedValidationError(c, err)
		}
		reqVal := reqPtr.Elem()
		if v, ok := reqPtr.Interface().(Validator); ok {
			if err := v.Validate(); err != nil {
				return typedValidationError(c, err)
			}
		} else if reqVal.CanInterface() {
			if v, ok := reqVal.Interface().(Validator); ok {
				if err := v.Validate(); err != nil {
					return typedValidationError(c, err)
				}
			}
		}
		out := hv.Call([]reflect.Value{reflect.ValueOf(c), reqVal})
		if !out[1].IsNil() {
			return out[1].Interface().(error)
		}
		return c.JSON(out[0].Interface())
	}
	handlers := append([]HandlerFunc{}, middleware...)
	handlers = append(handlers, h)
	a.Add(method, path, handlers...)
	a.updateRouteInfo(method, path, func(ri *RouteInfo) {
		ri.Typed = true
		ri.RequestType = niceTypeName(reqType)
		ri.ResponseType = niceTypeName(resType)
		ri.RequestSchema = schemaFromType(reqType, map[reflect.Type]bool{})
		ri.ResponseSchema = schemaFromType(resType, map[reflect.Type]bool{})
	})
	return a
}

func bindTaggedFields(c Ctx, v reflect.Value) error {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		fv := v.Field(i)
		var raw string
		name := ""
		source := ""
		for _, key := range []string{"param", "path"} {
			if tag := f.Tag.Get(key); tag != "" && tag != "-" {
				name, source = strings.Split(tag, ",")[0], "param"
				raw = c.Param(name)
				break
			}
		}
		if source == "" {
			if tag := f.Tag.Get("query"); tag != "" && tag != "-" {
				name, source = strings.Split(tag, ",")[0], "query"
				raw = c.Query(name)
			}
		}
		if source == "" {
			if tag := f.Tag.Get("header"); tag != "" && tag != "-" {
				name, source = strings.Split(tag, ",")[0], "header"
				raw = c.Get(name)
			}
		}
		if source == "" {
			if tag := f.Tag.Get("cookie"); tag != "" && tag != "-" {
				name, source = strings.Split(tag, ",")[0], "cookie"
				raw = c.GetCookie(name)
			}
		}
		if source == "" || raw == "" {
			continue
		}
		if err := setReflectScalar(fv, raw); err != nil {
			return fmt.Errorf("%s %q: %w", source, name, err)
		}
	}
	return nil
}

func setReflectScalar(v reflect.Value, raw string) error {
	if !v.CanSet() {
		return nil
	}
	if v.Kind() == reflect.Pointer {
		v.Set(reflect.New(v.Type().Elem()))
		return setReflectScalar(v.Elem(), raw)
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(raw)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := strconv.ParseInt(raw, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetInt(i)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u, err := strconv.ParseUint(raw, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetUint(u)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(raw, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetFloat(f)
	default:
		return nil
	}
	return nil
}

func niceTypeName(t reflect.Type) string {
	if t == nil {
		return "any"
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Name() == "" {
		return t.String()
	}
	return t.Name()
}
func typedValidationError(c Ctx, err error) error {
	se := StructuredError{Error: "validation_failed", Message: err.Error(), RequestID: requestIDFromCtx(c)}
	if ve, ok := err.(*ValidationError); ok {
		se.Fields = ve.Fields
	} else if ve, ok := err.(ValidationErrors); ok {
		se.Fields = []FieldError(ve)
	}
	return c.Status(StatusUnprocessableEntity).JSON(se)
}
func requestIDFromCtx(c Ctx) string {
	if v := c.Get(HeaderRequestID); v != "" {
		return v
	}
	return ""
}

type JSONSchema map[string]any

func schemaFromType(t reflect.Type, seen map[reflect.Type]bool) JSONSchema {
	if t == nil {
		return JSONSchema{"type": "object"}
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if seen[t] {
		return JSONSchema{"type": "object"}
	}
	switch t.Kind() {
	case reflect.String:
		return JSONSchema{"type": "string"}
	case reflect.Bool:
		return JSONSchema{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return JSONSchema{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return JSONSchema{"type": "number"}
	case reflect.Slice, reflect.Array:
		return JSONSchema{"type": "array", "items": schemaFromType(t.Elem(), seen)}
	case reflect.Map:
		return JSONSchema{"type": "object", "additionalProperties": schemaFromType(t.Elem(), seen)}
	case reflect.Struct:
		if t.PkgPath() == "time" && t.Name() == "Time" {
			return JSONSchema{"type": "string", "format": "date-time"}
		}
		seen[t] = true
		props := map[string]any{}
		req := []string{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue
			}
			name := strings.Split(f.Tag.Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			fs := schemaFromType(f.Type, seen)
			if tag := f.Tag.Get("format"); tag != "" {
				fs["format"] = tag
			}
			if desc := f.Tag.Get("description"); desc != "" {
				fs["description"] = desc
			}
			props[name] = fs
			if strings.Contains(f.Tag.Get("validate"), "required") {
				req = append(req, name)
			}
		}
		out := JSONSchema{"type": "object", "properties": props}
		if len(req) > 0 {
			out["required"] = req
		}
		return out
	default:
		return JSONSchema{"type": "object"}
	}
}

type RouteInfo struct {
	Method         string              `json:"method"`
	Path           string              `json:"path"`
	Name           string              `json:"name,omitempty"`
	Typed          bool                `json:"typed,omitempty"`
	RequestType    string              `json:"request_type,omitempty"`
	ResponseType   string              `json:"response_type,omitempty"`
	RequestSchema  JSONSchema          `json:"request_schema,omitempty"`
	ResponseSchema JSONSchema          `json:"response_schema,omitempty"`
	Deprecated     bool                `json:"deprecated,omitempty"`
	Tags           []string            `json:"tags,omitempty"`
	Security       RouteSecurityConfig `json:"security,omitempty"`
	Data           DataPolicy          `json:"data,omitempty"`
}

func (a *App) registerRouteInfo(info RouteInfo) {
	a.routeMetaMu.Lock()
	defer a.routeMetaMu.Unlock()
	for _, r := range a.routeMeta {
		if r.Method == info.Method && r.Path == info.Path {
			return
		}
	}
	a.routeMeta = append(a.routeMeta, info)
}
func (a *App) updateRouteInfo(method, path string, fn func(*RouteInfo)) {
	method = strings.ToUpper(method)
	path = normalizeRoutePath(method, path)
	a.routeMetaMu.Lock()
	defer a.routeMetaMu.Unlock()
	for i := range a.routeMeta {
		if a.routeMeta[i].Method == method && a.routeMeta[i].Path == path {
			fn(&a.routeMeta[i])
			return
		}
	}
	ri := RouteInfo{Method: method, Path: path}
	fn(&ri)
	a.routeMeta = append(a.routeMeta, ri)
}
func (a *App) Routes() []RouteInfo {
	a.routeMetaMu.RLock()
	defer a.routeMetaMu.RUnlock()
	out := append([]RouteInfo(nil), a.routeMeta...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path == out[j].Path {
			return out[i].Method < out[j].Method
		}
		return out[i].Path < out[j].Path
	})
	return out
}

// OpenAPIConfig controls native OpenAPI export and docs UI.
type OpenAPIConfig struct {
	Title, Version, Description string
	Servers                     []string
}

func (a *App) EnableOpenAPI(path string, cfg OpenAPIConfig) *App {
	if path == "" {
		path = "/openapi.json"
	}
	a.openapi = cfg
	a.Get(path, func(c Ctx) error { return c.JSON(a.OpenAPI()) })
	return a
}
func (a *App) EnableDocs(path string) *App {
	if path == "" {
		path = "/docs"
	}
	a.Get(path, func(c Ctx) error { c.Type("text/html; charset=utf-8"); return c.SendString(docsHTML) })
	return a
}
func (a *App) OpenAPI() map[string]any {
	cfg := a.openapi
	if cfg.Title == "" {
		cfg.Title = "fh API"
	}
	if cfg.Version == "" {
		cfg.Version = "1.0.0"
	}
	paths := map[string]any{}
	for _, r := range a.Routes() {
		if r.Path == "*" {
			continue
		}
		item, _ := paths[r.Path].(map[string]any)
		if item == nil {
			item = map[string]any{}
			paths[r.Path] = item
		}
		op := map[string]any{"responses": map[string]any{"200": map[string]any{"description": "OK"}}}
		if len(r.Tags) > 0 {
			op["tags"] = r.Tags
		}
		if r.Security.AuthRequired || len(r.Security.Scopes) > 0 {
			op["security"] = []map[string][]string{{"bearerAuth": r.Security.Scopes}}
		}
		if r.Security.IdempotencyRequired {
			op["x-idempotency-required"] = true
		}
		if r.Data.Sensitivity != "" {
			op["x-data-class"] = r.Data.Sensitivity
		}
		if r.Deprecated {
			op["deprecated"] = true
		}
		if r.RequestSchema != nil {
			op["requestBody"] = map[string]any{"required": true, "content": map[string]any{"application/json": map[string]any{"schema": r.RequestSchema}}}
		}
		if r.ResponseSchema != nil {
			op["responses"] = map[string]any{"200": map[string]any{"description": "OK", "content": map[string]any{"application/json": map[string]any{"schema": r.ResponseSchema}}}}
		}
		item[strings.ToLower(r.Method)] = op
	}
	servers := []map[string]string{}
	for _, s := range cfg.Servers {
		servers = append(servers, map[string]string{"url": s})
	}
	return map[string]any{"openapi": "3.1.0", "info": map[string]any{"title": cfg.Title, "version": cfg.Version, "description": cfg.Description}, "servers": servers, "paths": paths, "components": map[string]any{"securitySchemes": map[string]any{"bearerAuth": map[string]any{"type": "http", "scheme": "bearer"}, "apiKeyAuth": map[string]any{"type": "apiKey", "in": "header", "name": "X-API-Key"}, "hmacSignature": map[string]any{"type": "apiKey", "in": "header", "name": "X-Signature"}}}}
}

const docsHTML = `<!doctype html><html><head><title>fh API Docs</title><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font-family:system-ui;margin:2rem;max-width:980px}pre{background:#f6f8fa;padding:1rem;overflow:auto}</style></head><body><h1>fh API Docs</h1><p>OpenAPI JSON is available at <a href="/openapi.json">/openapi.json</a>.</p><pre id="spec">Loading...</pre><script>fetch('/openapi.json').then(r=>r.json()).then(j=>spec.textContent=JSON.stringify(j,null,2))</script></body></html>`

// EnableRouteList mounts a route-table introspection endpoint. It reveals
// every route's path, method, and security metadata (including which routes
// have no auth requirement) — pass an auth middleware in deployments
// reachable from outside a trusted network.
func (a *App) EnableRouteList(path string, middleware ...HandlerFunc) *App {
	if path == "" {
		path = "/_fh/routes"
	}
	a.Get(path, withHandlers(middleware, func(c Ctx) error { return c.JSON(a.Routes()) })...)
	return a
}

/* metrics are implemented in policy.go */

// Security helpers.
func ConstantTimeEqual(a, b string) bool { return hmac.Equal([]byte(a), []byte(b)) }
func RedactSecret(s string) string {
	if s == "" {
		return ""
	}
	if len(s) <= 8 {
		return "[REDACTED]"
	}
	return s[:4] + "…" + s[len(s)-4:]
}
func SignCookie(value string, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(value))
	return value + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func VerifySignedCookie(s string, secret []byte) (string, bool) {
	i := strings.LastIndexByte(s, '.')
	if i < 0 {
		return "", false
	}
	val, sig := s[:i], s[i+1:]
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(val))
	exp := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return val, hmac.Equal([]byte(sig), []byte(exp))
}

type AtomicJobOptions struct {
	Type           string
	Body           []byte
	Priority       int
	Delay          time.Duration
	RunAt          time.Time
	ConcurrencyKey string
	Headers        map[string]string
	MaxAttempts    int
}
type AtomicJobResult struct {
	ID string `json:"id"`
}

const (
	PriorityLow    = -10
	PriorityNormal = 0
	PriorityHigh   = 10
)

func AtomicJob(c Ctx, opt AtomicJobOptions) (*AtomicJobResult, error) {
	if c == nil || c.Reliability() == nil || c.Queue() == nil {
		return nil, errors.New("fh: reliability queue is not enabled")
	}
	spec := QueueJob{Type: opt.Type, Payload: opt.Body, Priority: opt.Priority, RunAt: opt.RunAt, VisibleAt: opt.RunAt, ConcurrencyKey: opt.ConcurrencyKey, MaxAttempts: opt.MaxAttempts, Headers: opt.Headers}
	if opt.Delay > 0 {
		spec.VisibleAt = time.Now().UTC().Add(opt.Delay)
		spec.RunAt = spec.VisibleAt
	}
	id, err := c.Reliability().queue.EnqueueJob(spec, opt.Body, opt.Headers)
	if err != nil {
		return nil, err
	}
	return &AtomicJobResult{ID: id}, nil
}

// parseHeadersLimit is app-configurable while preserving parseHeaders for tests.
func parseHeadersLimit(src []byte, h *RequestHeader, maxCount int, strictValueValidation bool) (int, error) {
	if maxCount <= 0 {
		maxCount = maxHeaders
	}
	if maxCount > maxServerHeaderCount {
		maxCount = maxServerHeaderCount
	}
	return parseHeadersWithLimitStrict(src, h, maxCount, strictValueValidation)
}
