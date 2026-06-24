package fh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"runtime/debug"
	"strings"
	"time"
)

// Environment controls how much diagnostic information is exposed to clients.
type Environment string

const (
	EnvProduction  Environment = "production"
	EnvStaging     Environment = "staging"
	EnvDevelopment Environment = "development"
	EnvTest        Environment = "test"
)

func (e Environment) debugAllowed() bool {
	switch strings.ToLower(string(e)) {
	case "dev", "devel", "development", "local", "test", "testing":
		return true
	default:
		return false
	}
}

// ErrorKind classifies failures for policy, metrics, alerting, and retry logic.
type ErrorKind string

const (
	KindBadRequest     ErrorKind = "bad_request"
	KindAuth           ErrorKind = "auth"
	KindPermission     ErrorKind = "permission"
	KindNotFound       ErrorKind = "not_found"
	KindConflict       ErrorKind = "conflict"
	KindValidation     ErrorKind = "validation"
	KindRateLimit      ErrorKind = "rate_limit"
	KindTimeout        ErrorKind = "timeout"
	KindDependency     ErrorKind = "dependency"
	KindCapacity       ErrorKind = "capacity"
	KindPanic          ErrorKind = "panic"
	KindInternal       ErrorKind = "internal"
	KindProtocol       ErrorKind = "protocol"
	KindUnavailable    ErrorKind = "unavailable"
	KindNotImplemented ErrorKind = "not_implemented"
)

// ErrorSeverity identifies operational urgency without changing the HTTP status.
type ErrorSeverity string

const (
	SeverityInfo     ErrorSeverity = "info"
	SeverityWarning  ErrorSeverity = "warning"
	SeverityError    ErrorSeverity = "error"
	SeverityCritical ErrorSeverity = "critical"
)

// ErrorOptions controls problem-detail rendering and debug friendliness.
type ErrorOptions struct {
	Environment      Environment
	ExposeDebug      bool
	ExposeStackTrace bool
	ExposeCauses     bool
	ProblemTypeBase  string
	IncludeRequestID bool
	IncludeTimestamp bool
	IncludeInstance  bool
	LogInternal      bool
	Redact           func(string) string
}

func (o ErrorOptions) normalize(appDebug bool) ErrorOptions {
	if o.Environment == "" {
		o.Environment = EnvProduction
	}
	if o.ProblemTypeBase == "" {
		o.ProblemTypeBase = "about:blank"
	} else {
		o.ProblemTypeBase = strings.TrimRight(o.ProblemTypeBase, "/")
	}
	if !o.IncludeRequestID {
		o.IncludeRequestID = true
	}
	if !o.IncludeTimestamp {
		o.IncludeTimestamp = true
	}
	if !o.IncludeInstance {
		o.IncludeInstance = true
	}
	if !o.LogInternal {
		o.LogInternal = true
	}
	if o.Redact == nil {
		o.Redact = RedactSecrets
	}
	if appDebug || o.Environment.debugAllowed() {
		o.ExposeDebug = true
	}
	return o
}

// HTTPError is a typed handler error. Message is safe to expose to clients;
// Err retains the private cause for logging and errors.Is/errors.As.
type HTTPError struct {
	Status    int
	Code      string
	Message   string
	Err       error
	Details   any
	Headers   map[string]string
	Kind      ErrorKind
	Severity  ErrorSeverity
	Retryable bool
	Meta      map[string]any
}

func (e *HTTPError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	if e.Message != "" {
		return e.Message
	}
	return StatusReason(e.Status)
}

func (e *HTTPError) Unwrap() error { return e.Err }

func (e *HTTPError) WithCause(err error) *HTTPError     { e.Err = err; return e }
func (e *HTTPError) WithDetails(details any) *HTTPError { e.Details = details; return e }
func (e *HTTPError) WithHeader(k, v string) *HTTPError {
	if e.Headers == nil {
		e.Headers = make(map[string]string, 1)
	}
	e.Headers[k] = v
	return e
}
func (e *HTTPError) WithMeta(k string, v any) *HTTPError {
	if e.Meta == nil {
		e.Meta = make(map[string]any, 1)
	}
	e.Meta[k] = v
	return e
}
func (e *HTTPError) Temporary() *HTTPError { e.Retryable = true; return e }

// NewHTTPError constructs a client-safe typed error.
func NewHTTPError(status int, code, message string) *HTTPError {
	if status < 400 || status > 599 {
		status = StatusInternalServerError
	}
	if code == "" {
		code = strings.ToUpper(strings.ReplaceAll(StatusReason(status), " ", "_"))
	}
	if message == "" {
		message = StatusReason(status)
	}
	e := &HTTPError{Status: status, Code: code, Message: message, Kind: kindForStatus(status), Severity: severityForStatus(status)}
	if status == StatusTooManyRequests || status == StatusServiceUnavailable || status == StatusGatewayTimeout || status == StatusBadGateway {
		e.Retryable = true
	}
	return e
}

// WrapHTTPError attaches a private cause to a client-safe typed error.
func WrapHTTPError(err error, status int, code, message string) *HTTPError {
	e := NewHTTPError(status, code, message)
	e.Err = err
	return e
}

func BadRequest(message string) *HTTPError {
	return NewHTTPError(StatusBadRequest, "BAD_REQUEST", message)
}
func Unauthorized(message string) *HTTPError {
	return NewHTTPError(StatusUnauthorized, "UNAUTHORIZED", message)
}
func Forbidden(message string) *HTTPError { return NewHTTPError(StatusForbidden, "FORBIDDEN", message) }
func NotFound(message string) *HTTPError  { return NewHTTPError(StatusNotFound, "NOT_FOUND", message) }
func MethodNotAllowed(message string) *HTTPError {
	return NewHTTPError(StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", message)
}
func Conflict(message string) *HTTPError { return NewHTTPError(StatusConflict, "CONFLICT", message) }
func PreconditionFailed(message string) *HTTPError {
	return NewHTTPError(StatusPreconditionFailed, "PRECONDITION_FAILED", message)
}
func UnsupportedMediaType(message string) *HTTPError {
	return NewHTTPError(StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", message)
}
func PayloadTooLarge(message string) *HTTPError {
	return NewHTTPError(StatusPayloadTooLarge, "PAYLOAD_TOO_LARGE", message)
}
func DependencyFailure(message string) *HTTPError {
	return NewHTTPError(StatusBadGateway, "DEPENDENCY_FAILURE", message).Temporary()
}
func Timeout(message string) *HTTPError {
	return NewHTTPError(StatusGatewayTimeout, "TIMEOUT", message).Temporary()
}
func Unavailable(message string) *HTTPError {
	return NewHTTPError(StatusServiceUnavailable, "SERVICE_UNAVAILABLE", message).Temporary()
}
func RateLimited(message string, retryAfter string) *HTTPError {
	e := NewHTTPError(StatusTooManyRequests, "RATE_LIMITED", message).Temporary()
	if retryAfter != "" {
		e.Headers = map[string]string{"Retry-After": retryAfter}
	}
	return e
}
func InternalError(err error) *HTTPError {
	return WrapHTTPError(err, StatusInternalServerError, "INTERNAL_ERROR", "An internal server error occurred")
}

// PanicError wraps a recovered panic. Stack is intentionally not returned to clients unless debug exposure is enabled.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string       { return fmt.Sprintf("panic: %v", e.Value) }
func NewPanicError(value any) *PanicError { return &PanicError{Value: value, Stack: debug.Stack()} }

// ErrorDefinition describes a built-in error for documentation generators.
type ErrorDefinition struct {
	Status    int       `json:"status"`
	Code      string    `json:"code"`
	Message   string    `json:"message"`
	Kind      ErrorKind `json:"kind"`
	Retryable bool      `json:"retryable"`
}

func ErrorCatalog() []ErrorDefinition {
	return []ErrorDefinition{
		{400, "BAD_REQUEST", "Bad Request", KindBadRequest, false}, {400, "MALFORMED_REQUEST", "Malformed HTTP request", KindProtocol, false},
		{401, "UNAUTHORIZED", "Unauthorized", KindAuth, false}, {403, "FORBIDDEN", "Forbidden", KindPermission, false},
		{404, "NOT_FOUND", "Not Found", KindNotFound, false}, {405, "METHOD_NOT_ALLOWED", "Method Not Allowed", KindBadRequest, false},
		{408, "REQUEST_TIMEOUT", "Request timeout", KindTimeout, true}, {409, "CONFLICT", "Conflict", KindConflict, false},
		{412, "PRECONDITION_FAILED", "Precondition Failed", KindConflict, false}, {413, "PAYLOAD_TOO_LARGE", "Payload Too Large", KindBadRequest, false},
		{415, "UNSUPPORTED_MEDIA_TYPE", "Unsupported Media Type", KindBadRequest, false}, {422, "VALIDATION_FAILED", "Validation failed", KindValidation, false},
		{425, "TOO_EARLY", "Retry the request after the TLS handshake completes", KindBadRequest, true}, {429, "RATE_LIMITED", "Too Many Requests", KindRateLimit, true},
		{431, "REQUEST_HEADERS_TOO_LARGE", "Request headers too large", KindBadRequest, false}, {500, "INTERNAL_ERROR", "An internal server error occurred", KindInternal, false},
		{500, "PANIC", "An internal server error occurred", KindPanic, false}, {501, "NOT_IMPLEMENTED", "Not implemented", KindNotImplemented, false},
		{502, "DEPENDENCY_FAILURE", "Dependency failure", KindDependency, true}, {503, "SERVICE_UNAVAILABLE", "Service unavailable", KindUnavailable, true},
		{504, "TIMEOUT", "The operation timed out", KindTimeout, true}, {508, "REWRITE_LOOP", "Too many internal rewrites", KindProtocol, false},
	}
}

type FieldError struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}
type ValidationError struct{ Fields []FieldError }

func (e *ValidationError) Error() string { return "validation failed" }

// Problem is an RFC 9457 Problem Details document. Extensions are emitted as top-level members.
type Problem struct {
	Type       string         `json:"type,omitempty"`
	Title      string         `json:"title,omitempty"`
	Status     int            `json:"status,omitempty"`
	Detail     string         `json:"detail,omitempty"`
	Instance   string         `json:"instance,omitempty"`
	Code       string         `json:"code,omitempty"`
	Extensions map[string]any `json:"-"`
}

func (p Problem) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, len(p.Extensions)+6)
	if p.Type != "" {
		m["type"] = p.Type
	}
	if p.Title != "" {
		m["title"] = p.Title
	}
	if p.Status != 0 {
		m["status"] = p.Status
	}
	if p.Detail != "" {
		m["detail"] = p.Detail
	}
	if p.Instance != "" {
		m["instance"] = p.Instance
	}
	if p.Code != "" {
		m["code"] = p.Code
	}
	for k, v := range p.Extensions {
		switch k {
		case "type", "title", "status", "detail", "instance", "code":
			continue
		}
		m[k] = v
	}
	return json.Marshal(m)
}

func (c *DefaultCtx) Problem(p Problem) error {
	if p.Status == 0 {
		p.Status = StatusInternalServerError
	}
	if p.Title == "" {
		p.Title = StatusReason(p.Status)
	}
	if p.Type == "" {
		p.Type = "about:blank"
	}
	c.status = p.Status
	c.contentType = []byte("application/problem+json; charset=utf-8")
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return c.writeResponse(b)
}

// ErrorReport is the fully classified server-side view of an error.
type ErrorReport struct {
	Error     *HTTPError
	Problem   Problem
	RequestID string
	Timestamp time.Time
	Path      string
	Method    string
	RemoteIP  string
	Stack     []byte
	Cause     string
}

func classifyError(err error, opts ErrorOptions) (*HTTPError, Problem, []byte) {
	var ve *ValidationError
	if errors.As(err, &ve) {
		he := NewHTTPError(StatusUnprocessableEntity, "VALIDATION_FAILED", "Validation failed")
		he.Details = ve.Fields
		return he, Problem{Status: he.Status, Code: he.Code, Detail: he.Message, Extensions: map[string]any{"errors": ve.Fields}}, nil
	}
	var pe *PanicError
	if errors.As(err, &pe) {
		he := WrapHTTPError(pe, StatusInternalServerError, "PANIC", "An internal server error occurred")
		he.Kind, he.Severity = KindPanic, SeverityCritical
		return he, Problem{Status: he.Status, Code: he.Code, Detail: he.Message}, pe.Stack
	}
	var he *HTTPError
	if errors.As(err, &he) {
		p := Problem{Status: he.Status, Code: he.Code, Detail: he.Message, Extensions: map[string]any{}}
		if he.Details != nil {
			p.Extensions["details"] = he.Details
		}
		for k, v := range he.Meta {
			p.Extensions[k] = v
		}
		return he, p, nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		he = Timeout("The operation timed out")
		return he, Problem{Status: he.Status, Code: he.Code, Detail: he.Message}, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		he = NotFound("Resource not found")
		return he, Problem{Status: he.Status, Code: he.Code, Detail: he.Message}, nil
	}
	if errors.Is(err, ErrBodyTooLarge) {
		he = PayloadTooLarge("Request body too large")
		return he, Problem{Status: he.Status, Code: he.Code, Detail: he.Message}, nil
	}
	if errors.Is(err, ErrMalformedRequest) {
		he = NewHTTPError(StatusBadRequest, "MALFORMED_REQUEST", "Malformed HTTP request")
		he.Kind = KindProtocol
		return he, Problem{Status: he.Status, Code: he.Code, Detail: he.Message}, nil
	}
	if errors.Is(err, ErrRequestLineTooLarge) {
		he = NewHTTPError(StatusURITooLong, "REQUEST_URI_TOO_LONG", "Request URI too long")
		return he, Problem{Status: he.Status, Code: he.Code, Detail: he.Message}, nil
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		he = Timeout("Network operation timed out")
		return he, Problem{Status: he.Status, Code: he.Code, Detail: he.Message}, nil
	}
	detail := "An internal server error occurred"
	if opts.ExposeDebug {
		detail = opts.Redact(err.Error())
	}
	he = WrapHTTPError(err, StatusInternalServerError, "INTERNAL_ERROR", detail)
	return he, Problem{Status: he.Status, Code: he.Code, Detail: detail}, nil
}

func (c *DefaultCtx) ErrorReport(err error) ErrorReport {
	opts := ErrorOptions{}
	if c.server != nil {
		opts = c.server.cfg.ErrorOptions.normalize(c.server.cfg.Debug)
	} else {
		opts = opts.normalize(false)
	}
	he, p, stack := classifyError(err, opts)
	decorateProblem(c, &p, he, err, opts, stack)
	return ErrorReport{Error: he, Problem: p, RequestID: errorRequestIDFromCtx(c), Timestamp: time.Now().UTC(), Path: c.Path(), Method: c.Method(), RemoteIP: c.IP(), Stack: stack, Cause: he.Error()}
}

func (c *DefaultCtx) ErrorResponse(err error) error { return c.SafeErrorResponse(err) }

// SafeErrorResponse classifies and writes err. It never lets a secondary render failure escape to the client.
func (c *DefaultCtx) SafeErrorResponse(err error) error {
	if err == nil || c.responded {
		return nil
	}
	opts := ErrorOptions{}
	if c.server != nil {
		opts = c.server.cfg.ErrorOptions.normalize(c.server.cfg.Debug)
	} else {
		opts = opts.normalize(false)
	}
	he, p, stack := classifyError(err, opts)
	decorateProblem(c, &p, he, err, opts, stack)
	if c.server != nil {
		c.server.recordError(he.Code)
	}
	for k, v := range he.Headers {
		c.Set(k, v)
	}
	c.Set("Cache-Control", "no-store")
	if he.Retryable && c.ResponseHeader("Retry-After") == "" && (he.Status == StatusServiceUnavailable || he.Status == StatusTooManyRequests) {
		c.Set("Retry-After", "1")
	}
	if e := c.Problem(p); e != nil && !c.responded {
		return c.fallbackErrorResponse(he.Status, he.Code, StatusReason(he.Status))
	}
	return nil
}

func decorateProblem(c *DefaultCtx, p *Problem, he *HTTPError, raw error, opts ErrorOptions, stack []byte) {
	if p.Status == 0 {
		p.Status = he.Status
	}
	if p.Title == "" {
		p.Title = StatusReason(p.Status)
	}
	if opts.ProblemTypeBase != "about:blank" && he.Code != "" {
		p.Type = opts.ProblemTypeBase + "/" + strings.ToLower(strings.ReplaceAll(he.Code, "_", "-"))
	}
	if p.Type == "" {
		p.Type = "about:blank"
	}
	if opts.IncludeInstance && c != nil {
		p.Instance = c.OriginalURL()
	}
	if p.Extensions == nil {
		p.Extensions = map[string]any{}
	}
	p.Extensions["kind"] = he.Kind
	p.Extensions["severity"] = he.Severity
	p.Extensions["retryable"] = he.Retryable
	if opts.IncludeRequestID && c != nil {
		if rid := errorRequestIDFromCtx(c); rid != "" {
			p.Extensions["request_id"] = rid
		}
	}
	if opts.IncludeTimestamp {
		p.Extensions["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if opts.ExposeDebug {
		p.Extensions["debug"] = map[string]any{"error": opts.Redact(raw.Error())}
		if opts.ExposeCauses {
			p.Extensions["cause"] = opts.Redact(he.Error())
		}
		if opts.ExposeStackTrace && len(stack) != 0 {
			p.Extensions["stack"] = opts.Redact(string(stack))
		}
	}
}

func (c *DefaultCtx) fallbackErrorResponse(status int, code, title string) error {
	if status < 400 {
		status = StatusInternalServerError
	}
	if title == "" {
		title = StatusReason(status)
	}
	body := `{"type":"about:blank","title":"` + escapeJSONString(title) + `","status":` + fmt.Sprint(status) + `,"code":"` + escapeJSONString(code) + `"}`
	c.status = status
	c.contentType = []byte("application/problem+json; charset=utf-8")
	return c.writeResponseString(body)
}

func errorRequestIDFromCtx(c *DefaultCtx) string {
	if c == nil {
		return ""
	}
	if v, ok := c.Locals("request_id").(string); ok && v != "" {
		return v
	}
	if v := c.Get(HeaderXRequestID); v != "" {
		return v
	}
	return ""
}

func kindForStatus(status int) ErrorKind {
	switch status {
	case StatusUnauthorized:
		return KindAuth
	case StatusForbidden:
		return KindPermission
	case StatusNotFound:
		return KindNotFound
	case StatusConflict, StatusPreconditionFailed, StatusPreconditionRequired:
		return KindConflict
	case StatusUnprocessableEntity:
		return KindValidation
	case StatusTooManyRequests:
		return KindRateLimit
	case StatusGatewayTimeout, StatusRequestTimeout:
		return KindTimeout
	case StatusBadGateway, StatusFailedDependency:
		return KindDependency
	case StatusServiceUnavailable, StatusInsufficientStorage:
		return KindUnavailable
	case StatusNotImplemented, StatusHTTPVersionNotSupported:
		return KindNotImplemented
	default:
		if status >= 400 && status < 500 {
			return KindBadRequest
		}
		return KindInternal
	}
}
func severityForStatus(status int) ErrorSeverity {
	if status >= 500 {
		return SeverityError
	}
	if status == StatusTooManyRequests || status == StatusRequestTimeout {
		return SeverityWarning
	}
	return SeverityInfo
}

// RedactSecrets removes common secret-bearing values from debug strings.
func RedactSecrets(s string) string {
	keys := []string{"password", "passwd", "pwd", "secret", "token", "authorization", "api_key", "apikey", "access_token", "refresh_token", "cookie", "set-cookie"}
	lower := strings.ToLower(s)
	out := s
	for _, key := range keys {
		idx := strings.Index(lower, key)
		for idx >= 0 {
			end := idx + len(key)
			for end < len(out) && (out[end] == ' ' || out[end] == ':' || out[end] == '=' || out[end] == '\t' || out[end] == '"' || out[end] == '\'') {
				end++
			}
			valEnd := end
			for valEnd < len(out) && out[valEnd] != ' ' && out[valEnd] != '&' && out[valEnd] != ',' && out[valEnd] != '\n' && out[valEnd] != '\r' && out[valEnd] != '"' && out[valEnd] != '\'' {
				valEnd++
			}
			if valEnd > end {
				out = out[:end] + "[REDACTED]" + out[valEnd:]
			}
			lower = strings.ToLower(out)
			next := strings.Index(lower[end:], key)
			if next < 0 {
				break
			}
			idx = end + next
		}
	}
	return out
}

func escapeJSONString(s string) string {
	b, _ := json.Marshal(s)
	if len(b) >= 2 {
		return string(b[1 : len(b)-1])
	}
	return s
}

func (e *HTTPError) GoString() string {
	return fmt.Sprintf("HTTPError{Status:%d, Code:%q, Message:%q, Kind:%q, Retryable:%t, Err:%v}", e.Status, e.Code, e.Message, e.Kind, e.Retryable, e.Err)
}
