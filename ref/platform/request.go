package platform

import (
	"strings"

	"github.com/oarkflow/fh/ref/invocation"
)

// Request introspection.
//
// An intent is transport-neutral, but some nodes legitimately need the request:
// a REST path parameter, a pagination query string, a correlation header. Rather
// than leaking the transport into every action, the invocation's metadata is read
// through this one function, which knows how each transport spells "parameter".
//
// A node reading a request value works over HTTP and returns nothing over a
// queue — which is correct: a queued job has no path parameters, and an author
// who depends on one has made a mistake worth surfacing as a missing value
// rather than a panic.

// requestValue reads a path parameter, query parameter or header from the
// invocation that is running. Lookups are case-insensitive for headers, because
// transports disagree about canonicalisation.
func requestValue(ctx *ActionContext, from, name string) (string, bool) {
	if ctx.Invocation == nil {
		return "", false
	}
	switch meta := ctx.Invocation.Metadata.(type) {
	case invocation.HTTPMeta:
		switch from {
		case "path":
			value, ok := meta.Params[name]
			return value, ok
		case "query":
			if values, ok := meta.Query[name]; ok && len(values) > 0 {
				return values[0], true
			}
			return "", false
		default:
			value := headerValue(meta.Headers, name)
			return value, value != ""
		}
	case invocation.QueueMeta:
		// A queued job carries headers only; a "query" or "path" read has no
		// meaning here and reports absent rather than guessing.
		if from != "header" {
			return "", false
		}
		for key, value := range meta.Headers {
			if strings.EqualFold(key, name) {
				return value, true
			}
		}
		return "", false
	case invocation.GRPCMeta:
		if from != "header" {
			return "", false
		}
		value := headerValue(meta.Metadata, name)
		return value, value != ""
	case invocation.CLIMeta:
		if from != "header" {
			value, ok := meta.Env[name]
			return value, ok
		}
		return "", false
	default:
		return "", false
	}
}

// requestParams returns every path parameter, for actions that forward the whole
// set rather than naming one.
func requestParams(ctx *ActionContext) map[string]string {
	if ctx.Invocation == nil {
		return nil
	}
	if meta, ok := ctx.Invocation.Metadata.(invocation.HTTPMeta); ok {
		return meta.Params
	}
	return nil
}
