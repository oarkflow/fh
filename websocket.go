package fh

import (
	"errors"
	"io"
	"net"
	"time"
)

var (
	ErrInvalidUpgrade = errors.New("invalid connection upgrade request")
	ErrHijackHTTP2    = errors.New("connection hijacking is unavailable on HTTP/2 streams")
)

func WriteAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}

		b = b[n:]
	}

	return nil
}

// Hijack runs handler synchronously on the raw HTTP/1.x connection.
// Reads first consume bytes already buffered after the request.
// The handler owns the protocol conversation until it returns.
// The provided ResponseConn has WriteHeader, SetHeader, and Write methods for
// clean HTTP response construction — use StatusText and header constants.
func (c *DefaultCtx) Hijack(handler func(*ResponseConn) error) error {
	if c.h2 != nil {
		return ErrHijackHTTP2
	}

	if c.responded || c.upgraded || handler == nil {
		return ErrInvalidUpgrade
	}

	if err := c.runBeforeResponse(); err != nil {
		return err
	}

	c.responded = true
	c.upgraded = true
	c.forceClose = true

	_ = c.conn.SetDeadline(time.Time{})

	return handler(&ResponseConn{
		Conn:   c.conn,
		prefix: c.upgradeBuffered,
	})
}

// Upgrade switches an HTTP/1.1 connection to another protocol.
func (c *DefaultCtx) Upgrade(protocol string, handler func(net.Conn) error) error {
	if c.h2 != nil {
		return ErrHijackHTTP2
	}

	if c.responded ||
		len(protocol) == 0 ||
		!validToken([]byte(protocol)) ||
		!hasHeaderToken(c.Header.Peek(HeaderConnectionBytes), "upgrade") ||
		!strEqFold(trimOWS(c.Header.Peek([]byte("Upgrade"))), protocol) ||
		handler == nil {
		return ErrInvalidUpgrade
	}

	if err := c.runBeforeResponse(); err != nil {
		return err
	}

	c.responded = true
	c.upgraded = true
	c.forceClose = true

	if c.writeBuf == nil {
		c.writeBuf = getBytes()
	}

	buf := (*c.writeBuf)[:0]

	buf = append(buf, "HTTP/1.1 101 Switching Protocols\r\n"...)
	buf = append(buf, "Connection: Upgrade\r\n"...)
	buf = append(buf, "Upgrade: "...)
	buf = append(buf, protocol...)
	buf = append(buf, "\r\n"...)

	for i := 0; i < c.chCount; i++ {
		h := &c.customHeaders[i]

		buf = append(buf, h.Key...)
		buf = append(buf, ": "...)
		buf = append(buf, h.Value...)
		buf = append(buf, "\r\n"...)
	}

	buf = appendExtraHeaders(buf, c.extraHeaders)

	for i := range c.responseCookies {
		if value := c.responseCookies[i].String(); value != "" {
			buf = append(buf, "Set-Cookie: "...)
			buf = append(buf, value...)
			buf = append(buf, "\r\n"...)
		}
	}

	buf = append(buf, "\r\n"...)

	*c.writeBuf = buf

	if err := WriteAll(c.conn, buf); err != nil {
		return err
	}

	_ = c.conn.SetDeadline(time.Time{})

	return handler(&prefixedConn{
		Conn:   c.conn,
		prefix: c.upgradeBuffered,
	})
}

type prefixedConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixedConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}

	return c.Conn.Read(p)
}
