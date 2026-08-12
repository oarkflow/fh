package fh

import (
	"fmt"
	"hash/fnv"
	"strings"
)

// AutoETag enables automatic ETag calculation and RFC 9110 conditional 304 response handling for the current request.
func (c *DefaultCtx) AutoETag() Ctx {
	c.flags |= ctxFlagAutoETag
	return c
}

func calculateETag(body []byte) string {
	h := fnv.New64a()
	_, _ = h.Write(body)
	return fmt.Sprintf("W/\"%x-%x\"", len(body), h.Sum64())
}

func evaluateAutoETag(c *DefaultCtx, body []byte) ([]byte, bool) {
	if (c.flags&ctxFlagAutoETag) == 0 || (c.status != 0 && c.status != 200) {
		return body, false
	}

	etag := calculateETag(body)
	c.Set("ETag", etag)

	ifNoneMatch := c.Get("If-None-Match")
	if ifNoneMatch != "" {
		if ifNoneMatch == "*" || strings.Contains(ifNoneMatch, etag) || strings.Contains(ifNoneMatch, strings.TrimPrefix(etag, "W/")) {
			c.status = 304
			c.Header.ContentType = nil
			return nil, true
		}
	}
	return body, false
}
