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
	ErrBufferedBodyBudget = errors.New("buffered request body budget exceeded")
)

const maxChunkLine = 4096
const defaultMaxTrailerBytes = 64 << 10

// readChunkedBodyWithLimits decodes one RFC 9112 chunked body. Initial may
// also contain bytes from the next pipelined request; those are returned as
// leftover.
func readChunkedBodyWithLimits(conn net.Conn, initial []byte, maxBody, maxTrailerBytes int, timeout time.Duration, reserve func(int) bool) (body, leftover []byte, trailers []Header, err error) {
	if maxTrailerBytes <= 0 {
		maxTrailerBytes = defaultMaxTrailerBytes
	}
	// Arm one absolute body deadline. Resetting it for every chunk would let a
	// slow sender keep the connection alive indefinitely by periodically sending
	// a byte. A zero timeout intentionally leaves the caller's deadline intact.
	if timeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, nil, nil, err
		}
	}
	wire := append(make([]byte, 0, len(initial)+4096), initial...)
	pos := 0
	// Grow the decoded body only after reserve has admitted each chunk.
	// The wire buffer holds framing bytes and is bounded by the request limits.
	body = nil

	readMore := func() error {
		var scratch [4096]byte
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
	compactWire := func() {
		if pos == 0 {
			return
		}
		remaining := copy(wire, wire[pos:])
		wire = wire[:remaining]
		pos = 0
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

	trailerBytes := 0
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
		// Discard consumed size lines and extensions periodically. Without
		// compaction, a bounded decoded body could retain unbounded wire framing
		// from a client that sends many small chunks or oversized extensions.
		if pos >= 4096 {
			compactWire()
		}
		if size == 0 {
			break
		}
		// Leave room for the chunk's trailing CRLF before adding it to size.
		// A peer can send a syntactically valid hexadecimal size up to MaxInt;
		// size+2 would wrap and skip ensure's read before the body slice below.
		if size > int(^uint(0)>>1)-2 {
			return nil, nil, nil, ErrBodyTooLarge
		}
		if size > maxBody-len(body) {
			return nil, nil, nil, ErrBodyTooLarge
		}
		if reserve != nil && !reserve(size) {
			return nil, nil, nil, ErrBufferedBodyBudget
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
		if pos >= 4096 {
			compactWire()
		}
	}

	for {
		line, lineErr := readLine()
		if lineErr != nil {
			return nil, nil, nil, lineErr
		}
		if len(line) == 0 {
			break
		}
		lineBytes := len(line) + 2 // include the field's CRLF on the wire
		if lineBytes > maxTrailerBytes-trailerBytes {
			return nil, nil, nil, ErrInvalidChunkedBody
		}
		trailerBytes += lineBytes
		colon := indexByte(line, ':')
		if colon <= 0 || !validToken(line[:colon]) || len(trailers) >= maxHeaders {
			return nil, nil, nil, ErrInvalidChunkedBody
		}
		if bytesEqualFold(line[:colon], HeaderContentLengthBytes) || bytesEqualFold(line[:colon], HeaderTransferEncodingBytes) || bytesEqualFold(line[:colon], HeaderHostBytes) {
			return nil, nil, nil, ErrInvalidChunkedBody
		}
		valueBytes := trimOWS(line[colon+1:])
		if !validHeaderValueBytes(valueBytes) {
			return nil, nil, nil, ErrInvalidChunkedBody
		}
		key := append([]byte(nil), line[:colon]...)
		value := append([]byte(nil), valueBytes...)
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
