package fh

import "errors"

var ErrResponseBodyTooLarge = errors.New("fh: buffered response exceeds configured limit")

// reserveBufferedBody accounts for request bytes retained in memory, including
// unread HTTP/2 streaming chunks. Bytes are released when consumed or when
// their stream ends.
func (a *App) reserveBufferedBody(n int64) bool {
	if a.tryReserveBufferedBody(n) {
		return true
	}
	if n > 0 && a.cfg.MaxBufferedRequestBytes > 0 {
		a.bodyBudgetRejections.Add(1)
	}
	return false
}

func (a *App) tryReserveBufferedBody(n int64) bool {
	limit := a.cfg.MaxBufferedRequestBytes
	if limit <= 0 || n <= 0 {
		return true
	}
	for {
		current := a.bufferedRequestBytes.Load()
		if n > limit-current {
			return false
		}
		if a.bufferedRequestBytes.CompareAndSwap(current, current+n) {
			return true
		}
	}
}

func (a *App) reserveBufferedResponse(n int64) bool {
	limit := a.cfg.MaxBufferedResponseBytes
	if limit <= 0 || n <= 0 {
		return true
	}
	for {
		current := a.bufferedResponseBytes.Load()
		if n > limit-current {
			a.bodyBudgetRejections.Add(1)
			return false
		}
		if a.bufferedResponseBytes.CompareAndSwap(current, current+n) {
			return true
		}
	}
}

func (a *App) releaseBufferedResponse(n int64) {
	if a.cfg.MaxBufferedResponseBytes > 0 && n > 0 {
		a.bufferedResponseBytes.Add(-n)
	}
}

func (c *DefaultCtx) reserveResponseBody(n int64) error {
	if c.server == nil || n <= c.bufferedResponseReserved {
		return nil
	}
	delta := n - c.bufferedResponseReserved
	if !c.server.reserveBufferedResponse(delta) {
		return ErrResponseBodyTooLarge
	}
	c.bufferedResponseReserved = n
	return nil
}

func (c *DefaultCtx) releaseResponseBody() {
	if c.server != nil && c.bufferedResponseReserved > 0 {
		c.server.releaseBufferedResponse(c.bufferedResponseReserved)
		c.bufferedResponseReserved = 0
	}
}

// reserveRequestBody accounts for bytes retained by a context after consuming
// a streaming or middleware-provided reader. HTTP/1's prebuffered body and
// unread HTTP/2 frames are accounted for by their transport owners instead.
func (c *DefaultCtx) reserveRequestBody(n int64) bool {
	if c.server == nil || n <= 0 {
		return true
	}
	return c.server.reserveBufferedBody(n)
}

func (c *DefaultCtx) releaseRequestBody() {
	if c.server != nil && c.requestBodyBufferedReserved > 0 {
		c.server.releaseBufferedBody(c.requestBodyBufferedReserved)
	}
	c.requestBodyBufferedReserved = 0
}

func (a *App) releaseBufferedBody(n int64) {
	if a.cfg.MaxBufferedRequestBytes > 0 && n > 0 {
		a.bufferedRequestBytes.Add(-n)
	}
}
