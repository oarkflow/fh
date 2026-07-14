package fh

import (
	"net"
	"strings"
)

// SetClientIP replaces the request's effective client address after a trusted
// proxy middleware has validated the forwarding chain. It returns false for an
// invalid address or a custom Ctx implementation that does not support an
// override.
func SetClientIP(c Ctx, value string) bool {
	ip := net.ParseIP(strings.TrimSpace(value))
	if ip == nil || c == nil {
		return false
	}
	setter, ok := c.(interface{ SetClientIP(string) })
	if !ok {
		return false
	}
	setter.SetClientIP(ip.String())
	return true
}

// SetClientIP updates the effective request address. Applications should use
// the package-level SetClientIP helper or mw/realip so input is validated.
func (c *DefaultCtx) SetClientIP(value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	c.cachedIP = value
}
