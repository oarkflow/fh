package fh

// reserveBufferedBody accounts for bytes before allocating or appending a
// buffered request body. Streaming request bodies are intentionally excluded.
func (a *App) reserveBufferedBody(n int64) bool {
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

func (a *App) releaseBufferedBody(n int64) {
	if a.cfg.MaxBufferedRequestBytes > 0 && n > 0 {
		a.bufferedRequestBytes.Add(-n)
	}
}
