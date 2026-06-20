package fasthttp

import (
	"errors"
	"net"
	"time"
)

var ErrInvalidUpgrade = errors.New("invalid connection upgrade request")
var ErrHijackHTTP2 = errors.New("connection hijacking is unavailable on HTTP/2 streams")

// Hijack runs handler synchronously on the raw HTTP/1.x connection. Reads see
// bytes that were already buffered after the request. The handler is
// responsible for writing any protocol response; the server closes the
// connection when handler returns.
func (c *Ctx) Hijack(handler func(net.Conn) error) error {
	if c.h2 != nil {
		return ErrHijackHTTP2
	}
	if c.responded || c.upgraded || handler == nil {
		return ErrInvalidUpgrade
	}
	c.responded, c.upgraded, c.forceClose = true, true, true
	_ = c.conn.SetDeadline(time.Time{})
	return handler(&prefixedConn{Conn: c.conn, prefix: c.upgradeBuffered})
}

// Upgrade switches an HTTP/1.1 connection to protocol and calls handler with
// a connection that first returns any bytes already buffered by the server.
// The handler owns the protocol conversation for the duration of the call;
// the server closes the connection when it returns.
func (c *Ctx) Upgrade(protocol string, handler func(net.Conn) error) error {
	if c.responded || len(protocol) == 0 || !validToken([]byte(protocol)) ||
		!hasHeaderToken(c.Header.Peek(headerConnection), "upgrade") ||
		!strEqFold(trimOWS(c.Header.Peek([]byte("Upgrade"))), protocol) {
		return ErrInvalidUpgrade
	}
	c.responded, c.upgraded, c.forceClose = true, true, true
	if c.writeBuf == nil {
		c.writeBuf = getBytes()
	}
	buf := (*c.writeBuf)[:0]
	buf = append(buf, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: "...)
	buf = append(buf, protocol...)
	buf = append(buf, '\r', '\n')
	for i := 0; i < c.chCount; i++ {
		h := &c.customHeaders[i]
		buf = append(buf, h.Key...)
		buf = append(buf, ':', ' ')
		buf = append(buf, h.Value...)
		buf = append(buf, '\r', '\n')
	}
	buf = appendExtraHeaders(buf, c.extraHeaders)
	buf = append(buf, '\r', '\n')
	*c.writeBuf = buf
	if err := writeAll(c.conn, buf); err != nil {
		return err
	}
	_ = c.conn.SetDeadline(time.Time{})
	return handler(&prefixedConn{Conn: c.conn, prefix: c.upgradeBuffered})
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
