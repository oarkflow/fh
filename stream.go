package fh

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// StreamWriter writes an HTTP response incrementally. HTTP/1.1 uses chunked
// transfer encoding; HTTP/1.0 falls back to a close-delimited body.
type StreamWriter struct {
	ctx      *DefaultCtx
	chunked  bool
	discard  bool
	closed   bool
	h2       bool
	buffered *[]byte
	expected int64
	written  int64
	bounded  bool
}

// Stream starts a streaming response and invokes fn synchronously. The final
// chunk is always written, including when fn returns an error.
func (c *DefaultCtx) Stream(fn func(*StreamWriter) error) error {
	return c.stream(-1, fn)
}

// StreamLength starts a streaming response with a known body length. HTTP/1.1
// can keep the connection reusable without chunked framing; HTTP/2 continues
// to use native DATA frames. The callback is invoked synchronously.
func (c *DefaultCtx) StreamLength(size int64, fn func(*StreamWriter) error) error {
	if size < 0 {
		return errors.New("fh: negative stream length")
	}
	return c.stream(size, fn)
}

func (c *DefaultCtx) stream(size int64, fn func(*StreamWriter) error) error {
	if c.responded {
		return nil
	}
	if fn == nil {
		return errors.New("fh: nil stream callback")
	}
	if c.bodyTransform != nil {
		body := make([]byte, 0, 4096)
		w := &StreamWriter{ctx: c, buffered: &body, expected: size, bounded: size >= 0}
		if err := fn(w); err != nil {
			return err
		}
		if w.bounded && int64(len(body)) != w.expected {
			return fmt.Errorf("fh: stream wrote %d bytes, expected %d", len(body), w.expected)
		}
		return c.writeResponse(body)
	}
	w, err := c.beginStream(size)
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
func (c *DefaultCtx) SendStream(r io.Reader) error {
	return c.SendStreamLength(r, -1)
}

// SendStreamLength copies r without buffering it in memory and advertises a
// known length when size is non-negative.
func (c *DefaultCtx) SendStreamLength(r io.Reader, size int64) error {
	if r == nil {
		return errors.New("fh: nil stream reader")
	}
	if size < -1 {
		return errors.New("fh: invalid stream length")
	}
	fn := func(w *StreamWriter) error {
		if w.discard {
			return nil
		}
		var scratch [32 << 10]byte
		_, err := io.CopyBuffer(w, r, scratch[:])
		return err
	}
	if size >= 0 {
		return c.StreamLength(size, fn)
	}
	return c.Stream(fn)
}

func (c *DefaultCtx) beginStream(size ...int64) (*StreamWriter, error) {
	if err := c.runBeforeResponse(); err != nil {
		return nil, err
	}
	if c.h2 != nil {
		if err := c.h2.beginStream(c); err != nil {
			return nil, err
		}
		c.responded = true
		return &StreamWriter{ctx: c, h2: true, discard: !responseBodyAllowed(c.status) || bytesEqualFold(c.Header.Method, MethodHEADBytes), expected: streamLength(size), bounded: streamHasLength(size)}, nil
	}
	c.responded = true
	headRequest := bytesEqualFold(c.Header.Method, MethodHEADBytes)
	bodyAllowed := responseBodyAllowed(c.status) && !headRequest
	knownLength := len(size) > 0 && size[0] >= 0
	chunked := string(c.Header.Proto) == "HTTP/1.1" && bodyAllowed && (!knownLength || len(c.responseTrailers) > 0)
	// A known-length HTTP/1.1 stream is safe for keep-alive. Unknown-length
	// streams need chunking or connection close to delimit the body; HTTP/1.0
	// has no chunked framing and therefore still closes after streaming.
	if bodyAllowed && !chunked && (!knownLength || string(c.Header.Proto) != "HTTP/1.1") {
		c.forceClose = true
	}
	if !c.Header.KeepAlive || c.server.cfg.DisableKeepAlive {
		c.forceClose = true
	}
	if c.writeBuf == nil {
		c.writeBuf = getBytes()
	}
	buf := (*c.writeBuf)[:0]
	buf = appendStatusLine(buf, c.status)
	if c.server.cfg.SendDateHeader {
		buf = append(buf, cachedDate()...)
	}
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
	} else if knownLength && (bodyAllowed || headRequest) {
		buf = append(buf, "Content-Length: "...)
		buf = strconv.AppendInt(buf, size[0], 10)
		buf = append(buf, '\r', '\n')
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
	} else if c.server.cfg.SendKeepAliveHeader {
		buf = append(buf, "Connection: keep-alive\r\n"...)
	}
	buf = append(buf, '\r', '\n')
	*c.writeBuf = buf
	if err := writeAll(c.conn, buf); err != nil {
		return nil, err
	}
	return &StreamWriter{ctx: c, chunked: chunked, discard: !bodyAllowed, expected: streamLength(size), bounded: streamHasLength(size)}, nil
}

func streamHasLength(size []int64) bool { return len(size) > 0 && size[0] >= 0 }

func streamLength(size []int64) int64 {
	if !streamHasLength(size) {
		return 0
	}
	return size[0]
}

func (w *StreamWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if w.discard {
		return len(p), nil
	}
	if w.bounded && int64(len(p)) > w.expected-w.written {
		p = p[:w.expected-w.written]
		if len(p) == 0 {
			w.ctx.forceClose = true
			return 0, io.ErrShortWrite
		}
		defer func() { w.ctx.forceClose = true }()
	}
	if w.buffered != nil {
		*w.buffered = append(*w.buffered, p...)
		w.written += int64(len(p))
		return len(p), nil
	}
	if w.discard {
		return len(p), nil
	}
	if w.h2 {
		if err := w.ctx.h2.writeData(p, false); err != nil {
			return 0, err
		}
		w.written += int64(len(p))
		return len(p), nil
	}
	if timeout := w.ctx.server.cfg.WriteTimeout; timeout > 0 {
		_ = w.ctx.conn.SetWriteDeadline(time.Now().Add(timeout))
	}
	if !w.chunked {
		if err := writeAll(w.ctx.conn, p); err != nil {
			return 0, err
		}
		w.written += int64(len(p))
		return len(p), nil
	}
	var prefix [24]byte
	b := appendHex(prefix[:0], len(p))
	b = append(b, '\r', '\n')
	if err := writeBuffers(w.ctx.conn, b, p, []byte("\r\n")); err != nil {
		return 0, err
	}
	w.written += int64(len(p))
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
	if w.discard {
		return nil
	}
	if w.buffered != nil {
		if w.bounded && w.written != w.expected {
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	if w.h2 {
		if w.bounded && w.written != w.expected {
			w.ctx.forceClose = true
			return io.ErrUnexpectedEOF
		}
		return w.ctx.h2.writeData(nil, true)
	}
	if w.bounded && w.written != w.expected {
		w.ctx.forceClose = true
		return io.ErrUnexpectedEOF
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
