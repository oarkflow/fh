package http

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/runtime"
)

type AdapterOptions struct {
	StatusCode      int
	MaxBodyBytes    int
	RequestIDHeader string
	RoutePattern    string
	OnInternalError func(error)
}

// Adapter creates an fh.HandlerFunc that dispatches requests through the REF engine.
func Adapter(engine *runtime.Engine, intentName intent.Name) fh.HandlerFunc {
	return AdapterWithOptions(engine, intentName, AdapterOptions{})
}

// AdapterWithOptions creates an HTTP handler with explicit transport contract settings.
func AdapterWithOptions(engine *runtime.Engine, intentName intent.Name, opts AdapterOptions) fh.HandlerFunc {
	if opts.StatusCode == 0 {
		opts.StatusCode = 200
	}
	if opts.RequestIDHeader == "" {
		opts.RequestIDHeader = "X-Request-ID"
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = 1 << 20
	}
	return func(c fh.Ctx) error {
		requestID := validRequestID(c.Get(opts.RequestIDHeader))
		if requestID == "" {
			requestID = newRequestID()
		}
		c.Set(opts.RequestIDHeader, requestID)
		body := c.BodyRaw()
		if len(body) > opts.MaxBodyBytes {
			return c.Status(413).JSON(map[string]any{"error": map[string]any{"code": "PAYLOAD_TOO_LARGE", "message": "request body exceeds the configured limit"}})
		}
		var queryValues map[string][]string
		if rawQuery := queryString(c.OriginalURL()); rawQuery != "" {
			query, _ := url.ParseQuery(rawQuery)
			queryValues = make(map[string][]string, len(query))
			for key, values := range query {
				queryValues[key] = append([]string(nil), values...)
			}
		}
		inv := &invocation.Invocation{
			ID: invocation.ID(requestID), Intent: invocation.IntentID(intentName),
			Input:     invocation.NewInputDirect(body, contentType(c.Get("Content-Type"))),
			Principal: invocation.NewPrincipalHint(extractBearer(c), extractAPIKey(c), nil, ""),
			Metadata:  invocation.NewHTTPMetaDirect(c.Method(), c.Path(), routePattern(opts.RoutePattern, c.Path()), c.Hostname(), c.GetReqHeaders(), queryValues, c.AllParams()),
			Transport: invocation.Transport{Protocol: "http", RemoteIP: c.IP(), TLS: c.Protocol() == "https"},
		}
		result, err := engine.Dispatch(c.Context(), inv)
		if err != nil {
			return projectFailure(c, err, opts.OnInternalError)
		}
		if result.Meta.CacheControl != "" {
			c.Set("Cache-Control", result.Meta.CacheControl)
		}
		err = c.Status(opts.StatusCode).JSON(result.Value)
		runtime.ReleaseDispatchResult(result)
		return err
	}
}

func routePattern(pattern, fallback string) string {
	if strings.TrimSpace(pattern) != "" {
		return pattern
	}
	return fallback
}

func contentType(value string) string {
	if strings.TrimSpace(value) == "" {
		return "application/json"
	}
	return value
}

func queryString(rawURL string) string {
	if i := strings.IndexByte(rawURL, '?'); i >= 0 {
		return rawURL[i+1:]
	}
	return ""
}
func validRequestID(value string) string {
	if len(value) == 0 || len(value) > 128 {
		return ""
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("-_.:", r)) {
			return ""
		}
	}
	return value
}
func newRequestID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "req-unavailable"
	}
	return hex.EncodeToString(id[:])
}

// projectFailure maps expected domain failures to HTTP and hides internal details.
func projectFailure(c fh.Ctx, err error, onInternalError ...func(error)) error {
	status, code, message := 500, "INTERNAL_ERROR", "internal error"
	if errors.Is(err, context.DeadlineExceeded) {
		status, code, message = 504, "TIMEOUT", "request timed out"
	}
	var failure intent.Failure
	if status != 504 && errors.As(err, &failure) {
		code = failure.Code
		switch failure.Category {
		case intent.CategoryInvalidInput:
			status, message = 422, failure.Message
		case intent.CategoryNotFound:
			status, message = 404, failure.Message
		case intent.CategoryConflict:
			status, message = 409, failure.Message
		case intent.CategoryPermission:
			status, message = 403, failure.Message
		case intent.CategoryAuth:
			status, message = 401, failure.Message
		case intent.CategoryRateLimit:
			status, message = 429, failure.Message
		case intent.CategoryUnavailable:
			status, message = 503, failure.Message
		case intent.CategoryTimeout:
			status, message = 504, failure.Message
		default:
			status, code, message = 500, "INTERNAL_ERROR", "internal error"
		}
		if message == "" {
			message = "request failed"
		}
	}
	if status >= 500 && len(onInternalError) > 0 && onInternalError[0] != nil {
		onInternalError[0](err)
	}
	if status == 401 {
		c.Set("WWW-Authenticate", "Bearer realm=api")
	}
	return c.Status(status).JSON(map[string]any{"error": map[string]any{"code": code, "message": message}})
}

func categoryToHTTPStatus(cat intent.Category) int {
	switch cat {
	case intent.CategoryInvalidInput:
		return 422
	case intent.CategoryNotFound:
		return 404
	case intent.CategoryConflict:
		return 409
	case intent.CategoryPermission:
		return 403
	case intent.CategoryAuth:
		return 401
	case intent.CategoryRateLimit:
		return 429
	case intent.CategoryUnavailable:
		return 503
	case intent.CategoryTimeout:
		return 504
	default:
		return 500
	}
}
func extractBearer(c fh.Ctx) string {
	auth := c.Get("Authorization")
	if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return ""
}
func extractAPIKey(c fh.Ctx) string {
	if k := c.Get("X-API-Key"); k != "" {
		return k
	}
	return ""
}
