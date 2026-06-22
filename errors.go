package fh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
)

// HTTPError is a typed handler error. Message is safe to expose to clients;
// Err retains the private cause for logging and errors.Is/errors.As.
type HTTPError struct {
	Status  int
	Code    string
	Message string
	Err     error
	Details any
	Headers map[string]string
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

// NewHTTPError constructs a client-safe typed error.
func NewHTTPError(status int, code, message string) *HTTPError {
	if status < 400 || status > 599 {
		status = StatusInternalServerError
	}
	if message == "" {
		message = StatusReason(status)
	}
	return &HTTPError{Status: status, Code: code, Message: message}
}

// WrapHTTPError attaches a private cause to a client-safe typed error.
func WrapHTTPError(err error, status int, code, message string) *HTTPError {
	e := NewHTTPError(status, code, message)
	e.Err = err
	return e
}

// Common framework errors.
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
	return NewHTTPError(StatusBadGateway, "DEPENDENCY_FAILURE", message)
}
func Timeout(message string) *HTTPError {
	return NewHTTPError(StatusGatewayTimeout, "TIMEOUT", message)
}
func RateLimited(message string, retryAfter string) *HTTPError {
	e := NewHTTPError(StatusTooManyRequests, "RATE_LIMITED", message)
	if retryAfter != "" {
		e.Headers = map[string]string{"Retry-After": retryAfter}
	}
	return e
}
func InternalError(err error) *HTTPError {
	return WrapHTTPError(err, StatusInternalServerError, "INTERNAL_ERROR", "An internal server error occurred")
}

// ErrorDefinition describes a built-in error for documentation generators.
type ErrorDefinition struct {
	Status  int    `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ErrorCatalog returns the stable built-in error vocabulary.
func ErrorCatalog() []ErrorDefinition {
	return []ErrorDefinition{
		{400, "BAD_REQUEST", "Bad Request"}, {401, "UNAUTHORIZED", "Unauthorized"},
		{403, "FORBIDDEN", "Forbidden"}, {404, "NOT_FOUND", "Not Found"},
		{405, "METHOD_NOT_ALLOWED", "Method Not Allowed"}, {409, "CONFLICT", "Conflict"},
		{412, "PRECONDITION_FAILED", "Precondition Failed"}, {413, "PAYLOAD_TOO_LARGE", "Payload Too Large"},
		{415, "UNSUPPORTED_MEDIA_TYPE", "Unsupported Media Type"}, {422, "VALIDATION_FAILED", "Validation failed"},
		{429, "RATE_LIMITED", "Too Many Requests"}, {500, "INTERNAL_ERROR", "An internal server error occurred"},
		{425, "TOO_EARLY", "Retry the request after the TLS handshake completes"},
		{403, "CSRF_INVALID", "CSRF token is missing or invalid"},
		{508, "REWRITE_LOOP", "Too many internal rewrites"},
		{502, "DEPENDENCY_FAILURE", "Dependency failure"}, {504, "TIMEOUT", "The operation timed out"},
	}
}

// FieldError identifies one invalid input field.
type FieldError struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ValidationError produces a stable validation-error response.
type ValidationError struct{ Fields []FieldError }

func (e *ValidationError) Error() string { return "validation failed" }

// Problem is an RFC 9457 Problem Details document. Extensions are emitted as
// top-level members, as required by the format.
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

// Problem sends an application/problem+json response.
func (c *Ctx) Problem(p Problem) error {
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
	c.contentType = []byte("application/problem+json")
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return c.writeResponse(b)
}

func classifyError(err error, debug bool) (*HTTPError, Problem) {
	var ve *ValidationError
	if errors.As(err, &ve) {
		he := NewHTTPError(StatusUnprocessableEntity, "VALIDATION_FAILED", "Validation failed")
		he.Details = ve.Fields
		return he, Problem{Status: he.Status, Code: he.Code, Detail: he.Message, Extensions: map[string]any{"errors": ve.Fields}}
	}
	var he *HTTPError
	if errors.As(err, &he) {
		p := Problem{Status: he.Status, Code: he.Code, Detail: he.Message}
		if he.Details != nil {
			p.Extensions = map[string]any{"details": he.Details}
		}
		return he, p
	}
	if errors.Is(err, context.DeadlineExceeded) {
		he = NewHTTPError(StatusGatewayTimeout, "TIMEOUT", "The operation timed out")
		return he, Problem{Status: he.Status, Code: he.Code, Detail: he.Message}
	}
	if errors.Is(err, fs.ErrNotExist) {
		he = NotFound("Resource not found")
		return he, Problem{Status: he.Status, Code: he.Code, Detail: he.Message}
	}
	detail := "An internal server error occurred"
	if debug {
		detail = err.Error()
	}
	he = WrapHTTPError(err, StatusInternalServerError, "INTERNAL_ERROR", detail)
	return he, Problem{Status: he.Status, Code: he.Code, Detail: detail}
}

// ErrorResponse classifies and writes err using Problem Details.
func (c *Ctx) ErrorResponse(err error) error {
	if err == nil {
		return nil
	}
	he, p := classifyError(err, c.server != nil && c.server.cfg.Debug)
	if c.server != nil {
		c.server.recordError(he.Code)
	}
	for k, v := range he.Headers {
		c.Set(k, v)
	}
	return c.Problem(p)
}

func (e *HTTPError) GoString() string {
	return fmt.Sprintf("HTTPError{Status:%d, Code:%q, Message:%q, Err:%v}", e.Status, e.Code, e.Message, e.Err)
}
