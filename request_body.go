package fh

import (
	"bytes"
	"io"
)

// ReplaceRequestBodyReader exposes an authenticated streaming body to the
// remainder of the current middleware chain. The caller retains ownership of
// the underlying framed request reader and must verify its terminal record.
func ReplaceRequestBodyReader(c Ctx, reader io.Reader) bool {
	if c == nil || reader == nil {
		return false
	}
	setter, ok := c.(interface{ SetRequestBodyReader(io.Reader) })
	if !ok {
		return false
	}
	setter.SetRequestBodyReader(reader)
	return true
}

func (c *DefaultCtx) SetRequestBodyReader(reader io.Reader) {
	c.requestBodyOverride = reader
	c.requestBodyLimit = nil
	c.body = nil
	c.bodyParserMapPtr = 0
	c.bodyParserRawJSON = nil
}

// ReplaceRequestBody installs a middleware-produced request body for the
// remainder of the current request. It is intended for bounded decompression,
// decryption, and normalization middleware.
func ReplaceRequestBody(c Ctx, body []byte) bool {
	if c == nil {
		return false
	}
	if setter, ok := c.(interface{ TrySetRequestBody([]byte) bool }); ok {
		return setter.TrySetRequestBody(body)
	}
	setter, ok := c.(interface{ SetRequestBody([]byte) })
	if !ok {
		return false
	}
	setter.SetRequestBody(body)
	return true
}

// SetRequestBody replaces the decoded body visible through Body and BodyParser.
func (c *DefaultCtx) SetRequestBody(body []byte) {
	_ = c.TrySetRequestBody(body)
}

// TrySetRequestBody reports whether the replacement passed the configured
// size limit and was installed.
func (c *DefaultCtx) TrySetRequestBody(body []byte) bool {
	if c.server != nil && c.server.cfg.MaxRequestBodySize > 0 && len(body) > c.server.cfg.MaxRequestBodySize {
		return false
	}
	c.body = body
	c.requestBodyOverride = bytes.NewReader(body)
	c.requestBodyLimit = nil
	c.Header.ContentLength = len(body)
	c.Header.HasContentLength = true
	c.bodyParserMapPtr = 0
	c.bodyParserRawJSON = nil
	return true
}
