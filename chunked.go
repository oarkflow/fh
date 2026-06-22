package fh

import (
	"errors"
	"io"
	"net"
	"time"
)

var (
	ErrInvalidChunkedBody = errors.New("invalid chunked request body")
	ErrBodyTooLarge       = errors.New("request body too large")
)

const maxChunkLine = 4096

// readChunkedBody decodes one RFC 9112 chunked body. Initial may also contain
// bytes from the next pipelined request; those are returned as leftover.
func readChunkedBody(conn net.Conn, initial []byte, maxBody int, timeout time.Duration) (body, leftover []byte, trailers []Header, err error) {
	wire := append(make([]byte, 0, len(initial)+4096), initial...)
	pos := 0
	body = make([]byte, 0, minInt(len(initial), maxBody))

	readMore := func() error {
		var scratch [4096]byte
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		n, readErr := conn.Read(scratch[:])
		if n > 0 {
			wire = append(wire, scratch[:n]...)
			return nil
		}
		if readErr != nil {
			return readErr
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		return nil
	}

	readLine := func() ([]byte, error) {
		start := pos
		for {
			for i := pos; i+1 < len(wire); i++ {
				if wire[i] == '\r' && wire[i+1] == '\n' {
					line := wire[start:i]
					pos = i + 2
					return line, nil
				}
			}
			if len(wire)-start > maxChunkLine {
				return nil, ErrInvalidChunkedBody
			}
			if err := readMore(); err != nil {
				return nil, err
			}
		}
	}

	ensure := func(n int) error {
		for len(wire)-pos < n {
			if err := readMore(); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		line, lineErr := readLine()
		if lineErr != nil {
			return nil, nil, nil, lineErr
		}
		if semi := indexByte(line, ';'); semi >= 0 {
			line = line[:semi]
		}
		size, ok := parseHex(trimOWS(line))
		if !ok {
			return nil, nil, nil, ErrInvalidChunkedBody
		}
		if size == 0 {
			break
		}
		if size > maxBody-len(body) {
			return nil, nil, nil, ErrBodyTooLarge
		}
		if err := ensure(size + 2); err != nil {
			return nil, nil, nil, err
		}
		body = append(body, wire[pos:pos+size]...)
		pos += size
		if wire[pos] != '\r' || wire[pos+1] != '\n' {
			return nil, nil, nil, ErrInvalidChunkedBody
		}
		pos += 2
	}

	for {
		line, lineErr := readLine()
		if lineErr != nil {
			return nil, nil, nil, lineErr
		}
		if len(line) == 0 {
			break
		}
		colon := indexByte(line, ':')
		if colon <= 0 || !validToken(line[:colon]) || len(trailers) >= maxHeaders {
			return nil, nil, nil, ErrInvalidChunkedBody
		}
		if bytesEqualFold(line[:colon], HeaderContentLengthBytes) || bytesEqualFold(line[:colon], HeaderTransferEncodingBytes) || bytesEqualFold(line[:colon], HeaderHostBytes) {
			return nil, nil, nil, ErrInvalidChunkedBody
		}
		key := append([]byte(nil), line[:colon]...)
		value := append([]byte(nil), trimOWS(line[colon+1:])...)
		trailers = append(trailers, Header{Key: key, Value: value})
	}
	leftover = append(leftover, wire[pos:]...)
	return body, leftover, trailers, nil
}

func parseHex(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	n, maxInt := 0, int(^uint(0)>>1)
	for _, c := range b {
		var d int
		switch {
		case c >= '0' && c <= '9':
			d = int(c - '0')
		case c >= 'a' && c <= 'f':
			d = int(c-'a') + 10
		case c >= 'A' && c <= 'F':
			d = int(c-'A') + 10
		default:
			return 0, false
		}
		if n > (maxInt-d)/16 {
			return 0, false
		}
		n = n*16 + d
	}
	return n, true
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
