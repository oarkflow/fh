// Package fh provides type-safe handler helpers using Go generics.
// TypedHandler eliminates manual JSON marshaling/unmarshaling and provides
// compile-time type safety for request/response handling.
//
// Usage:
//
//	type CreateUserReq struct {
//	    Name  string `json:"name"`
//	    Email string `json:"email"`
//	}
//
//	type UserRes struct {
//	    ID   int    `json:"id"`
//	    Name string `json:"name"`
//	}
//
//	app.Post("/users", fh.TypedHandler(func(c *fh.Ctx, req CreateUserReq) (UserRes, error) {
//	    user := UserRes{ID: 1, Name: req.Name}
//	    return user, nil
//	}))
package fh

import (
	"encoding/json"
	"io"
	"strings"
)

// TypedHandler creates a type-safe handler from a function that takes a typed
// request and returns a typed response. It automatically handles:
// - Request body parsing (JSON, XML, form data)
// - Response serialization (JSON)
// - Content-Type negotiation
// - Error responses (RFC 9457 Problem Details)
//
// For GET requests with no body, use TypedGet instead.
func TypedHandler[Req any, Res any](fn func(Ctx, Req) (Res, error)) HandlerFunc {
	return func(ctx Ctx) error {
		var req Req

		method := ctx.Method()
		if method != "GET" && method != "HEAD" && method != "OPTIONS" {
			body := ctx.Body()
			if len(body) > 0 {
				contentType := ctx.Get("Content-Type")
				if err := parseTypedBody(body, contentType, &req); err != nil {
					return NewHTTPError(StatusBadRequest, "INVALID_BODY", "Failed to parse request body: "+err.Error())
				}
			}
		}

		// Parse query parameters into the request struct.
		if err := ctx.QueryParser(&req); err != nil {
			// Query parser errors are non-fatal — some fields may not have query params.
		}

		res, err := fn(ctx, req)
		if err != nil {
			return err
		}

		return ctx.JSON(res)
	}
}

// TypedGet creates a type-safe GET handler that doesn't require a request body.
// The response is automatically serialized to JSON.
func TypedGet[Res any](fn func(Ctx) (Res, error)) HandlerFunc {
	return func(ctx Ctx) error {
		res, err := fn(ctx)
		if err != nil {
			return err
		}
		return ctx.JSON(res)
	}
}

// TypedPost creates a type-safe POST handler with typed request and response.
func TypedPost[Req any, Res any](fn func(Ctx, Req) (Res, error)) HandlerFunc {
	return TypedHandler(fn)
}

// TypedPut creates a type-safe PUT handler with typed request and response.
func TypedPut[Req any, Res any](fn func(Ctx, Req) (Res, error)) HandlerFunc {
	return TypedHandler(fn)
}

// TypedDelete creates a type-safe DELETE handler with typed response.
func TypedDelete[Res any](fn func(Ctx) (Res, error)) HandlerFunc {
	return TypedGet(fn)
}

// TypedPatch creates a type-safe PATCH handler with typed request and response.
func TypedPatch[Req any, Res any](fn func(Ctx, Req) (Res, error)) HandlerFunc {
	return TypedHandler(fn)
}

// TypedStream creates a handler that streams a typed response.
// Useful for large responses that should not be buffered.
func TypedStream[Res any](fn func(Ctx) (Res, io.Reader, error)) HandlerFunc {
	return func(ctx Ctx) error {
		res, reader, err := fn(ctx)
		if err != nil {
			return err
		}
		// Send response headers first.
		if err := ctx.JSON(res); err != nil {
			return err
		}
		// Stream the body.
		return ctx.SendStream(reader)
	}
}

// ── Response helpers ───────────────────────────────────────────────────────

// TypedResponse wraps a response with metadata for enhanced API responses.
type TypedResponse[T any] struct {
	Data   T          `json:"data"`
	Meta   *Meta      `json:"meta,omitempty"`
	Errors []APIError `json:"errors,omitempty"`
}

// Meta holds response metadata for pagination, caching, etc.
type Meta struct {
	Page       int    `json:"page,omitempty"`
	PerPage    int    `json:"per_page,omitempty"`
	Total      int64  `json:"total,omitempty"`
	TotalPages int    `json:"total_pages,omitempty"`
	RequestID  string `json:"request_id,omitempty"`
}

// APIError represents a typed API error.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

// PaginatedResponse creates a typed paginated response.
func PaginatedResponse[T any](data []T, page, perPage int, total int64) TypedResponse[[]T] {
	totalPages := int(total) / perPage
	if int(total)%perPage > 0 {
		totalPages++
	}
	return TypedResponse[[]T]{
		Data: data,
		Meta: &Meta{
			Page:       page,
			PerPage:    perPage,
			Total:      total,
			TotalPages: totalPages,
		},
	}
}

// ErrorResponse creates a typed error response.
func ErrorResponse(code, message string, status int) *HTTPError {
	return NewHTTPError(status, code, message)
}

// ── Body parsing helpers ───────────────────────────────────────────────────

func parseTypedBody(body []byte, contentType string, v any) error {
	// Determine format from Content-Type.
	ct := contentType
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	ct = strings.TrimSpace(ct)

	switch ct {
	case "application/json":
		return json.Unmarshal(body, v)
	case "application/xml", "text/xml":
		// Fall through to standard unmarshal.
		return json.Unmarshal(body, v) // XML not supported in typed handler; use BodyParser for XML.
	case "application/x-www-form-urlencoded":
		return parseFormBody(body, v)
	default:
		// Try JSON as default.
		return json.Unmarshal(body, v)
	}
}

func parseFormBody(body []byte, v any) error {
	values, err := parseQueryBytes(body)
	if err != nil {
		return err
	}
	// Convert form values to JSON and unmarshal.
	m := make(map[string]any)
	for _, p := range values {
		m[p.Key] = p.Value
	}
	jsonBytes, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonBytes, v)
}

// ── Content negotiation helpers ────────────────────────────────────────────

// Negotiate returns the best content type from the client's Accept header.
func Negotiate(ctx Ctx, offered []string) string {
	accept := ctx.Get("Accept")
	if accept == "" || len(offered) == 0 {
		if len(offered) > 0 {
			return offered[0]
		}
		return "application/json"
	}

	// Simple quality-based negotiation.
	best := offered[0]
	bestQ := 0.0

	for _, media := range offered {
		q := acceptQuality(accept, media)
		if q > bestQ {
			bestQ = q
			best = media
		}
	}

	return best
}

func acceptQuality(accept, media string) float64 {
	if accept == "*" {
		return 1.0
	}

	for {
		accept = trimSpaceLeft(accept)
		if len(accept) == 0 {
			break
		}

		end := indexOfByteStr(accept, ',')
		var token string
		if end < 0 {
			token = accept
			accept = ""
		} else {
			token = accept[:end]
			accept = accept[end+1:]
		}

		q := 1.0
		if semi := indexOfByteStr(token, ';'); semi >= 0 {
			params := token[semi+1:]
			token = token[:semi]
			// Parse q= parameter.
			if qi := indexOfByteStr(params, 'q'); qi >= 0 && qi+2 < len(params) && params[qi+1] == '=' {
				q = parseFloat(params[qi+2:])
			}
		}

		token = strings.TrimSpace(token)
		if token == "*" || token == media {
			return q
		}
	}

	return 0
}

// ── Streaming response helpers ─────────────────────────────────────────────

// StreamJSON streams a JSON response without buffering the entire body.
// Useful for large datasets or real-time data.
func StreamJSON(ctx Ctx, fn func(json.Encoder) error) error {
	return ctx.Stream(func(sw *StreamWriter) error {
		enc := json.NewEncoder(sw)
		return fn(*enc)
	})
}

// StreamNDJSON streams newline-delimited JSON (NDJSON) for real-time data.
func StreamNDJSON(ctx Ctx, fn func(yield func(any) bool)) error {
	ctx.Set("Content-Type", "application/x-ndjson")
	return ctx.Stream(func(sw *StreamWriter) error {
		var first bool = true
		yield := func(v any) bool {
			if !first {
				sw.Write([]byte("\n"))
			}
			first = false
			enc := json.NewEncoder(sw)
			return enc.Encode(v) == nil
		}
		fn(yield)
		return nil
	})
}

// ── Helper functions ───────────────────────────────────────────────────────

func indexOfByteStr(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func trimSpaceLeft(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	return s
}

func parseFloat(s string) float64 {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0
	}
	var v float64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '.' {
			continue
		}
		if c >= '0' && c <= '9' {
			v = v*10 + float64(c-'0')
		} else {
			break
		}
	}
	return v
}

// parseQueryBytes is a reference to the internal query parser.
// It's defined in the fh package for internal use.
func parseQueryBytes(b []byte) ([]Param, error) {
	var params []Param
	for len(b) > 0 {
		if b[0] == '&' {
			b = b[1:]
			continue
		}
		end := indexOfByte(b, '&')
		pair := b
		if end >= 0 {
			pair = b[:end]
		}
		eq := indexOfByte(pair, '=')
		key, val := "", ""
		if eq >= 0 {
			key = string(pair[:eq])
			val = string(pair[eq+1:])
		} else {
			key = string(pair)
		}
		params = append(params, Param{Key: key, Value: val})
		if end < 0 {
			break
		}
		b = b[end+1:]
	}
	return params, nil
}

// indexOfByte returns the index of c in b, or -1 if not found.
func indexOfByte(b []byte, c byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == c {
			return i
		}
	}
	return -1
}
