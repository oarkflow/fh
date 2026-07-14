package fh

// ReplaceRequestBody installs a middleware-produced request body for the
// remainder of the current request. It is intended for bounded decompression,
// decryption, and normalization middleware.
func ReplaceRequestBody(c Ctx, body []byte) bool {
	if c == nil {
		return false
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
	if c.server != nil && c.server.cfg.MaxRequestBodySize > 0 && len(body) > c.server.cfg.MaxRequestBodySize {
		return
	}
	c.body = body
	c.Header.ContentLength = len(body)
	c.Header.HasContentLength = true
	c.bodyParserMapPtr = 0
	c.bodyParserRawJSON = nil
}
