package fh

import "errors"

var ErrResponseBodyTooLarge = errors.New("fh: buffered response exceeds configured limit")

// reserveBufferedBody accounts for request bytes retained in memory, including
// unread HTTP/2 streaming chunks. Bytes are released when consumed or when
// their stream ends.
func (a *App) reserveBufferedBody(n int64) bool {
	limit := a.cfg.MaxBufferedRequestBytes
	if limit <= 0 || n <= 0 {
		return true
	}
	for {
		current := a.bufferedRequestBytes.Load()
		if n > limit-current {
			a.bodyBudgetRejections.Add(1)
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

func (a *App) releaseBufferedBody(n int64) {
	if a.cfg.MaxBufferedRequestBytes > 0 && n > 0 {
		a.bufferedRequestBytes.Add(-n)
	}
}
