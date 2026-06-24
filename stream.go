package fh

import (
	"io"
	"net"
	"time"
)

// StreamWriter writes an HTTP response incrementally. HTTP/1.1 uses chunked
// transfer encoding; HTTP/1.0 falls back to a close-delimited body.
type StreamWriter struct {
	ctx      *Ctx
	chunked  bool
	discard  bool
	closed   bool
	h2       bool
	buffered *[]byte
}

// Stream starts a streaming response and invokes fn synchronously. The final
// chunk is always written, including when fn returns an error.
func (c *Ctx) Stream(fn func(*StreamWriter) error) error {
	if c.responded {
		return nil
	}
	if c.bodyTransform != nil {
		body := make([]byte, 0, 4096)
		w := &StreamWriter{ctx: c, buffered: &body}
		if err := fn(w); err != nil {
			return err
		}
		return c.writeResponse(body)
	}
	w, err := c.beginStream()
	if err != nil {
		return err
	}
	callErr := fn(w)
	closeErr := w.Close()
	if callErr != nil {
		return callErr
	}
	return closeErr
}

// SendStream copies r to a streamed response using a fixed scratch buffer.
func (c *Ctx) SendStream(r io.Reader) error {
	return c.Stream(func(w *StreamWriter) error {
		var scratch [32 << 10]byte
		_, err := io.CopyBuffer(w, r, scratch[:])
		return err
	})
}

func (c *Ctx) beginStream() (*StreamWriter, error) {
	if err := c.runBeforeResponse(); err != nil {
		return nil, err
	}
	if c.h2 != nil {
		if err := c.h2.beginStream(c); err != nil {
			return nil, err
		}
		c.responded = true
		return &StreamWriter{ctx: c, h2: true, discard: !responseBodyAllowed(c.status) || bytesEqualFold(c.Header.Method, MethodHEADBytes)}, nil
	}
	c.responded = true
	bodyAllowed := responseBodyAllowed(c.status) && !bytesEqualFold(c.Header.Method, MethodHEADBytes)
	chunked := string(c.Header.Proto) == "HTTP/1.1" && bodyAllowed
	if bodyAllowed && !chunked {
		c.forceClose = true
	}
	if c.writeBuf == nil {
		c.writeBuf = getBytes()
	}
	buf := (*c.writeBuf)[:0]
	buf = appendStatusLine(buf, c.status)
	buf = append(buf, cachedDate()...)
	if c.contentType != nil {
		buf = append(buf, "Content-Type: "...)
		buf = append(buf, c.contentType...)
		buf = append(buf, '\r', '\n')
	}
	for i := 0; i < c.chCount; i++ {
		h := &c.customHeaders[i]
		buf = append(buf, h.Key...)
		buf = append(buf, ':', ' ')
		buf = append(buf, h.Value...)
		buf = append(buf, '\r', '\n')
	}
	buf = appendExtraHeaders(buf, c.extraHeaders)
	for i := range c.responseCookies {
		if value := c.responseCookies[i].String(); value != "" {
			buf = append(buf, "Set-Cookie: "...)
			buf = append(buf, value...)
			buf = append(buf, '\r', '\n')
		}
	}
	if chunked {
		buf = append(buf, "Transfer-Encoding: chunked\r\n"...)
	}
	if len(c.responseTrailers) > 0 {
		buf = append(buf, "Trailer: "...)
		for i, t := range c.responseTrailers {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, t.Key...)
		}
		buf = append(buf, '\r', '\n')
	}
	if c.forceClose {
		buf = append(buf, "Connection: close\r\n"...)
	} else {
		buf = append(buf, "Connection: keep-alive\r\n"...)
	}
	buf = append(buf, '\r', '\n')
	*c.writeBuf = buf
	if err := writeAll(c.conn, buf); err != nil {
		return nil, err
	}
	return &StreamWriter{ctx: c, chunked: chunked, discard: !bodyAllowed}, nil
}

func (w *StreamWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if w.buffered != nil {
		*w.buffered = append(*w.buffered, p...)
		return len(p), nil
	}
	if w.discard {
		return len(p), nil
	}
	if w.h2 {
		if err := w.ctx.h2.writeData(p, false); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	if timeout := w.ctx.server.cfg.WriteTimeout; timeout > 0 {
		_ = w.ctx.conn.SetWriteDeadline(time.Now().Add(timeout))
	}
	if !w.chunked {
		if err := writeAll(w.ctx.conn, p); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	var prefix [24]byte
	b := appendHex(prefix[:0], len(p))
	b = append(b, '\r', '\n')
	if err := writeAll(w.ctx.conn, b); err != nil {
		return 0, err
	}
	if err := writeAll(w.ctx.conn, p); err != nil {
		return 0, err
	}
	if err := writeAll(w.ctx.conn, []byte("\r\n")); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *StreamWriter) Flush() error {
	if w.closed || w.discard || w.buffered != nil {
		return nil
	}
	if w.h2 {
		return nil
	}
	if tc, ok := w.ctx.conn.(*net.TCPConn); ok {
		if err := tc.SetNoDelay(false); err != nil {
			return err
		}
		return tc.SetNoDelay(true)
	}
	return nil
}

func (w *StreamWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.buffered != nil {
		return nil
	}
	if w.h2 {
		return w.ctx.h2.writeData(nil, true)
	}
	if timeout := w.ctx.server.cfg.WriteTimeout; timeout > 0 {
		_ = w.ctx.conn.SetWriteDeadline(time.Now().Add(timeout))
	}
	if w.chunked {
		buf := make([]byte, 0, 256)
		buf = append(buf, "0\r\n"...)
		for _, t := range w.ctx.responseTrailers {
			buf = append(buf, t.Key...)
			buf = append(buf, ':', ' ')
			buf = append(buf, t.Value...)
			buf = append(buf, '\r', '\n')
		}
		buf = append(buf, '\r', '\n')
		return writeAll(w.ctx.conn, buf)
	}
	return nil
}

func appendHex(dst []byte, n int) []byte {
	const digits = "0123456789abcdef"
	var scratch [16]byte
	i := len(scratch)
	for n > 0 {
		i--
		scratch[i] = digits[n&15]
		n >>= 4
	}
	if i == len(scratch) {
		return append(dst, '0')
	}
	return append(dst, scratch[i:]...)
}
