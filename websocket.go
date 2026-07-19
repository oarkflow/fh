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

// Upgrade switches an HTTP/1.1 connection to another protocol. Over HTTP/2 it
// transparently uses an RFC 8441 extended CONNECT tunnel instead (see
// upgradeH2) when the request negotiated one, so the same handler works
// unmodified for both HTTP/1.1 and HTTP/2 clients.
func (c *DefaultCtx) Upgrade(protocol string, handler func(net.Conn) error) error {
	if c.h2 != nil {
		return c.upgradeH2(protocol, handler)
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

// upgradeH2 confirms an RFC 8441 extended CONNECT tunnel and hands the caller
// a net.Conn backed by the HTTP/2 stream. Unlike the HTTP/1.1 Upgrade above,
// there is no "101 Switching Protocols" concept in HTTP/2 — a normal 2xx
// HEADERS response with no END_STREAM is itself the tunnel confirmation
// (RFC 8441 §5), and the underlying TCP/TLS connection keeps multiplexing
// other streams independently, so forceClose is not set.
func (c *DefaultCtx) upgradeH2(protocol string, handler func(net.Conn) error) error {
	if c.responded || len(protocol) == 0 || handler == nil || c.h2.stream.connectConn == nil ||
		!strEqFold(s2b(c.h2.stream.protocol), protocol) {
		return ErrInvalidUpgrade
	}

	if err := c.runBeforeResponse(); err != nil {
		return err
	}

	c.responded = true
	c.upgraded = true

	if err := c.h2.beginStream(c); err != nil {
		return err
	}

	sc := c.h2.stream.connectConn
	err := handler(sc)
	// Unlike HTTP/1.1, where returning from an upgraded handler lets the whole
	// TCP connection get torn down by the caller, an HTTP/2 stream shares its
	// connection with others still in flight — always end this one stream
	// (idempotent: a no-op if the handler already closed it).
	_ = sc.Close()
	return err
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
