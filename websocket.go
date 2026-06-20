package fasthttp

import (
	"crypto/sha1" // WebSocket handshake is defined in terms of SHA-1.
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"unicode/utf8"
)

const (
	WSContinuation = byte(0x0)
	WSText         = byte(0x1)
	WSBinary       = byte(0x2)
	WSClose        = byte(0x8)
	WSPing         = byte(0x9)
	WSPong         = byte(0xa)
)

var (
	ErrWebSocketHandshake = errors.New("invalid websocket handshake")
	ErrWebSocketProtocol  = errors.New("websocket protocol error")
	ErrWebSocketTooLarge  = errors.New("websocket message too large")
)

// WebSocket returns a route handler that performs an RFC 6455 handshake and
// runs handler until it returns. Origin/authentication policy belongs in
// middleware registered before this handler.
func WebSocket(handler func(*WSConn) error) HandlerFunc {
	return func(c *Ctx) error {
		if !bytesEqualFold(c.Header.Method, methodGET) ||
			c.Header.ContentLength != 0 || c.Header.Chunked ||
			!strEqFold(trimOWS(c.Header.Peek([]byte("Upgrade"))), "websocket") ||
			!hasHeaderToken(c.Header.Peek(headerConnection), "upgrade") ||
			!strEqFold(trimOWS(c.Header.Peek([]byte("Sec-WebSocket-Version"))), "13") {
			return ErrWebSocketHandshake
		}
		key := trimOWS(c.Header.Peek([]byte("Sec-WebSocket-Key")))
		decoded, err := base64.StdEncoding.DecodeString(string(key))
		if err != nil || len(decoded) != 16 {
			return ErrWebSocketHandshake
		}
		h := sha1.New()
		_, _ = h.Write(key)
		_, _ = h.Write([]byte("258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		accept := base64.StdEncoding.EncodeToString(h.Sum(nil))
		c.Set("Sec-WebSocket-Accept", accept)
		maxMessage := c.server.cfg.MaxRequestBodySize
		return c.Upgrade("websocket", func(conn net.Conn) error {
			ws := &WSConn{conn: conn, maxMessage: maxMessage}
			defer ws.Close()
			return handler(ws)
		})
	}
}

// WSConn is a minimal, concurrency-safe RFC 6455 connection. One goroutine
// may read while another writes. Client frames must be masked.
type WSConn struct {
	conn       net.Conn
	writeMu    sync.Mutex
	maxMessage int
	closed     bool
}

func (c *WSConn) ReadMessage() (opcode byte, payload []byte, err error) {
	firstOpcode := byte(0)
	for {
		fin, op, frame, frameErr := c.readFrame()
		if frameErr != nil {
			return 0, nil, frameErr
		}
		switch op {
		case WSPing:
			if err := c.WriteMessage(WSPong, frame); err != nil {
				return 0, nil, err
			}
			continue
		case WSPong:
			continue
		case WSClose:
			if !validClosePayload(frame) {
				return 0, nil, ErrWebSocketProtocol
			}
			_ = c.WriteMessage(WSClose, frame)
			return WSClose, frame, io.EOF
		case WSText, WSBinary:
			if firstOpcode != 0 {
				return 0, nil, ErrWebSocketProtocol
			}
			firstOpcode = op
		case WSContinuation:
			if firstOpcode == 0 {
				return 0, nil, ErrWebSocketProtocol
			}
		default:
			return 0, nil, ErrWebSocketProtocol
		}
		if len(payload)+len(frame) > c.maxMessage {
			return 0, nil, ErrWebSocketTooLarge
		}
		payload = append(payload, frame...)
		if fin {
			if firstOpcode == WSText && !utf8.Valid(payload) {
				return 0, nil, ErrWebSocketProtocol
			}
			return firstOpcode, payload, nil
		}
	}
}

func (c *WSConn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	var head [2]byte
	if _, err = io.ReadFull(c.conn, head[:]); err != nil {
		return
	}
	fin, opcode = head[0]&0x80 != 0, head[0]&0x0f
	if head[0]&0x70 != 0 || head[1]&0x80 == 0 {
		err = ErrWebSocketProtocol
		return
	}
	length := uint64(head[1] & 0x7f)
	if length == 126 {
		var ext [2]byte
		if _, err = io.ReadFull(c.conn, ext[:]); err != nil {
			return
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
		if length < 126 {
			err = ErrWebSocketProtocol
			return
		}
	} else if length == 127 {
		var ext [8]byte
		if _, err = io.ReadFull(c.conn, ext[:]); err != nil {
			return
		}
		length = binary.BigEndian.Uint64(ext[:])
		if length>>63 != 0 || length <= 65535 {
			err = ErrWebSocketProtocol
			return
		}
	}
	control := opcode >= 0x8
	if control && (!fin || length > 125) {
		err = ErrWebSocketProtocol
		return
	}
	if length > uint64(c.maxMessage) || length > uint64(^uint(0)>>1) {
		err = ErrWebSocketTooLarge
		return
	}
	var mask [4]byte
	if _, err = io.ReadFull(c.conn, mask[:]); err != nil {
		return
	}
	payload = make([]byte, int(length))
	if _, err = io.ReadFull(c.conn, payload); err != nil {
		return
	}
	for i := range payload {
		payload[i] ^= mask[i&3]
	}
	return
}

func (c *WSConn) WriteMessage(opcode byte, payload []byte) error {
	if opcode != WSText && opcode != WSBinary && opcode != WSClose && opcode != WSPing && opcode != WSPong {
		return ErrWebSocketProtocol
	}
	if opcode >= 0x8 && len(payload) > 125 {
		return ErrWebSocketProtocol
	}
	if opcode == WSText && !utf8.Valid(payload) {
		return ErrWebSocketProtocol
	}
	if opcode == WSClose && !validClosePayload(payload) {
		return ErrWebSocketProtocol
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed {
		if opcode == WSClose {
			return nil
		}
		return net.ErrClosed
	}
	var head [10]byte
	head[0] = 0x80 | opcode
	n := 2
	switch {
	case len(payload) < 126:
		head[1] = byte(len(payload))
	case len(payload) <= 65535:
		head[1] = 126
		binary.BigEndian.PutUint16(head[2:4], uint16(len(payload)))
		n = 4
	default:
		head[1] = 127
		binary.BigEndian.PutUint64(head[2:10], uint64(len(payload)))
		n = 10
	}
	if err := writeAll(c.conn, head[:n]); err != nil {
		return err
	}
	if err := writeAll(c.conn, payload); err != nil {
		return err
	}
	if opcode == WSClose {
		c.closed = true
	}
	return nil
}

func (c *WSConn) Close() error {
	_ = c.WriteMessage(WSClose, nil)
	return c.conn.Close()
}

func (c *WSConn) NetConn() net.Conn { return c.conn }

func validClosePayload(payload []byte) bool {
	if len(payload) == 0 {
		return true
	}
	if len(payload) == 1 {
		return false
	}
	code := binary.BigEndian.Uint16(payload[:2])
	if code < 1000 || code == 1004 || code == 1005 || code == 1006 || code == 1015 || (code >= 1016 && code < 3000) || code > 4999 {
		return false
	}
	return utf8.Valid(payload[2:])
}
