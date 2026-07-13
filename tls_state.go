package fh

import (
	"context"
	"crypto/tls"
)

type tlsStateContextKey struct{}

// WithTLSState attaches an immutable TLS connection-state snapshot to a
// request context. Servers normally do this automatically; the exported helper
// is useful for adapters and tests that construct contexts themselves.
func WithTLSState(ctx context.Context, state tls.ConnectionState) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, tlsStateContextKey{}, state)
}

// TLSStateFromContext returns the TLS connection-state snapshot attached to a
// context. It is available for both HTTP/1.1 and HTTP/2 requests accepted by a
// TLS listener.
func TLSStateFromContext(ctx context.Context) (tls.ConnectionState, bool) {
	if ctx == nil {
		return tls.ConnectionState{}, false
	}
	state, ok := ctx.Value(tlsStateContextKey{}).(tls.ConnectionState)
	return state, ok
}

// RequestTLSState returns the TLS state for c. Plaintext requests return false.
func RequestTLSState(c Ctx) (tls.ConnectionState, bool) {
	if c == nil {
		return tls.ConnectionState{}, false
	}
	return TLSStateFromContext(c.Context())
}
