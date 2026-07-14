package fh

import (
	"bytes"
	"strings"
)

// DeleteResponseHeader removes a response header when the concrete context
// supports mutation. It keeps the public Ctx interface stable for adapters.
func DeleteResponseHeader(c Ctx, name string) bool {
	if c == nil {
		return false
	}
	deleter, ok := c.(interface{ DelResponseHeader(string) })
	if !ok {
		return false
	}
	deleter.DelResponseHeader(name)
	return true
}

// DelResponseHeader removes every pending response value with the given name.
// Set-Cookie is intentionally not removed by this method; cookies are managed
// through SetCookie/DelCookie so a middleware cannot accidentally erase an
// authentication transition while hiding application response metadata.
func (c *DefaultCtx) DelResponseHeader(name string) {
	if strings.EqualFold(name, HeaderContentTypeStr) {
		c.contentType = nil
	}
	if strings.EqualFold(name, HeaderSetCookieStr) {
		return
	}
	needle := []byte(name)
	write := 0
	for i := 0; i < c.chCount; i++ {
		if bytes.EqualFold(c.customHeaders[i].Key, needle) {
			continue
		}
		if write != i {
			c.customHeaders[write] = c.customHeaders[i]
		}
		write++
	}
	clear(c.customHeaders[write:c.chCount])
	c.chCount = write

	write = 0
	for i := range c.extraHeaders {
		if bytes.EqualFold(c.extraHeaders[i].Key, needle) {
			continue
		}
		if write != i {
			c.extraHeaders[write] = c.extraHeaders[i]
		}
		write++
	}
	clear(c.extraHeaders[write:])
	c.extraHeaders = c.extraHeaders[:write]
}
