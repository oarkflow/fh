package fh

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
)

// requestBodyStream consumes exactly one HTTP/1 request body. It never reads
// beyond the body boundary, so bytes belonging to a pipelined request remain
// available to the connection loop.
type requestBodyStream struct {
	conn            net.Conn
	prefix          []byte
	remaining       int64
	chunked         bool
	max             int64
	maxTrailerBytes int
	trailerBytes    int
	total           int64
	chunkLeft       int64
	needCRLF        bool
	done            bool
	err             error
	leftover        []byte
	trailers        []Header
	lineBuf         [32]byte
	line            []byte
}

func newRequestBodyStream(conn net.Conn, prefix []byte, length int, chunked bool, max, maxTrailerBytes int) *requestBodyStream {
	r := &requestBodyStream{
		conn:            conn,
		prefix:          prefix,
		remaining:       int64(length),
		chunked:         chunked,
		max:             int64(max),
		maxTrailerBytes: maxTrailerBytes,
	}
	if chunked {
		r.line = r.lineBuf[:0]
	}
	return r
}

func (r *requestBodyStream) Read(p []byte) (int, error) {
	if r.done {
		if r.err != nil {
			return 0, r.err
		}
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if !r.chunked {
		return r.readFixed(p)
	}
	return r.readChunked(p)
}

func (r *requestBodyStream) readFixed(p []byte) (int, error) {
	if r.remaining == 0 {
		r.done = true
		r.leftover = r.prefix
		r.prefix = nil
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n := r.readSome(p)
	r.remaining -= int64(n)
	r.total += int64(n)
	if n == 0 && r.err != nil {
		return 0, r.err
	}
	if r.remaining == 0 {
		r.done = true
		r.leftover = r.prefix
		r.prefix = nil
		return n, io.EOF
	}
	return n, nil
}

func (r *requestBodyStream) readChunked(p []byte) (int, error) {
	for {
		if r.chunkLeft > 0 {
			if int64(len(p)) > r.chunkLeft {
				p = p[:r.chunkLeft]
			}
			n := r.readSome(p)
			r.chunkLeft -= int64(n)
			r.total += int64(n)
			if n == 0 && r.err != nil {
				return 0, r.err
			}
			if r.chunkLeft == 0 {
				r.needCRLF = true
			}
			return n, nil
		}
		if r.needCRLF {
			if err := r.expectCRLF(); err != nil {
				return 0, err
			}
			r.needCRLF = false
		}
		line, err := r.readLine()
		if err != nil {
			return 0, err
		}
		sizeText := line
		if semi := bytes.IndexByte(line, ';'); semi >= 0 {
			sizeText = line[:semi]
		}
		size, ok := parseHex(trimOWS(sizeText))
		if !ok {
			return 0, r.fail(ErrMalformedRequest)
		}
		if int64(size) > r.max-r.total {
			return 0, r.fail(ErrBodyTooLarge)
		}
		if size == 0 {
			if err := r.readTrailers(); err != nil {
				return 0, err
			}
			r.done = true
			return 0, io.EOF
		}
		r.chunkLeft = int64(size)
	}
}

func (r *requestBodyStream) readSome(p []byte) int {
	if len(r.prefix) > 0 {
		n := copy(p, r.prefix)
		r.prefix = r.prefix[n:]
		return n
	}
	// A Reader may return data and a terminal error together. The caller gets
	// the data first; keep that error and never issue another socket read after
	// it has been observed.
	if r.err != nil {
		return 0
	}
	n, err := r.conn.Read(p)
	if err != nil {
		r.err = err
	}
	return n
}

func (r *requestBodyStream) readByte() (byte, error) {
	if len(r.prefix) > 0 {
		b := r.prefix[0]
		r.prefix = r.prefix[1:]
		return b, nil
	}
	if r.err != nil {
		return 0, r.fail(r.err)
	}
	var one [1]byte
	n, err := r.conn.Read(one[:])
	if err != nil {
		r.err = err
	}
	if n == 1 {
		return one[0], nil
	}
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	return 0, r.fail(err)
}

func (r *requestBodyStream) readLine() ([]byte, error) {
	r.line = r.line[:0]
	for len(r.line) <= maxChunkLine {
		b, err := r.readByte()
		if err != nil {
			return nil, err
		}
		r.line = append(r.line, b)
		if b == '\n' {
			if len(r.line) < 2 || r.line[len(r.line)-2] != '\r' {
				return nil, r.fail(ErrMalformedRequest)
			}
			return r.line[:len(r.line)-2], nil
		}
	}
	return nil, r.fail(ErrInvalidChunkedBody)
}

func (r *requestBodyStream) expectCRLF() error {
	a, err := r.readByte()
	if err != nil {
		return err
	}
	b, err := r.readByte()
	if err != nil {
		return err
	}
	if a != '\r' || b != '\n' {
		return r.fail(ErrMalformedRequest)
	}
	return nil
}

func (r *requestBodyStream) readTrailers() error {
	for {
		line, err := r.readLine()
		if err != nil {
			return err
		}
		if len(line) == 0 {
			return nil
		}
		lineBytes := len(line) + 2 // include the field's CRLF on the wire
		if r.maxTrailerBytes > 0 && lineBytes > r.maxTrailerBytes-r.trailerBytes {
			return r.fail(ErrInvalidChunkedBody)
		}
		r.trailerBytes += lineBytes
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 || !validToken(line[:colon]) || len(r.trailers) >= maxHeaders {
			return r.fail(ErrInvalidChunkedBody)
		}
		if bytesEqualFold(line[:colon], HeaderContentLengthBytes) || bytesEqualFold(line[:colon], HeaderTransferEncodingBytes) || bytesEqualFold(line[:colon], HeaderHostBytes) {
			return r.fail(ErrInvalidChunkedBody)
		}
		value := bytes.TrimSpace(line[colon+1:])
		for _, b := range value {
			if (b < 0x20 && b != '\t') || b == 0x7f {
				return r.fail(ErrMalformedRequest)
			}
		}
		r.trailers = append(r.trailers, Header{Key: append([]byte(nil), line[:colon]...), Value: append([]byte(nil), value...)})
	}
}

func (r *requestBodyStream) fail(err error) error {
	r.err = err
	r.done = true
	return err
}

func (r *requestBodyStream) finish() ([]byte, []Header, error) {
	return r.leftover, r.trailers, r.err
}

// h2RequestBodyReader exposes one HTTP/2 stream's DATA frames to the handler.
// Receive-window credit is returned only after bytes have been consumed,
// providing bounded backpressure while allowing the connection reactor to
// continue processing other streams.
type h2RequestBodyReader struct {
	conn     *h2Conn
	stream   *h2Stream
	ctx      context.Context
	wakeOnce sync.Once
}

func (r *h2RequestBodyReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s := r.stream
	if r.ctx != nil {
		r.wakeOnce.Do(func() {
			context.AfterFunc(r.ctx, func() {
				s.bodyMu.Lock()
				if s.bodyCond != nil {
					s.bodyCond.Broadcast()
				}
				s.bodyMu.Unlock()
			})
		})
	}
	s.bodyMu.Lock()
	for s.bodyQueued == 0 && !s.bodyEOF && s.bodyErr == nil && !s.reset.Load() && (r.ctx == nil || r.ctx.Err() == nil) {
		if s.bodyCond == nil {
			s.bodyMu.Unlock()
			return 0, net.ErrClosed
		}
		s.bodyCond.Wait()
	}
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			s.bodyMu.Unlock()
			return 0, err
		}
	}
	if s.bodyQueued > 0 {
		n := 0
		for n < len(p) && len(s.bodyChunks) > 0 {
			chunk := s.bodyChunks[0]
			copied := copy(p[n:], chunk[s.bodyChunkOffset:])
			n += copied
			s.bodyChunkOffset += copied
			s.bodyQueued -= copied
			if s.bodyChunkOffset == len(chunk) {
				s.bodyChunks[0] = nil
				s.bodyChunks = s.bodyChunks[1:]
				s.bodyChunkOffset = 0
			}
		}
		s.bodyReserved.Add(-int64(n))
		s.bodyMu.Unlock()
		r.conn.app.releaseBufferedBody(int64(n))
		r.conn.consumeH2Body(s, n)
		return n, nil
	}
	err := s.bodyErr
	reset := s.reset.Load()
	eof := s.bodyEOF
	s.bodyMu.Unlock()
	if err != nil {
		return 0, err
	}
	if reset {
		return 0, net.ErrClosed
	}
	if eof {
		r.conn.flushH2BodyCredit(s)
		return 0, io.EOF
	}
	return 0, io.ErrNoProgress
}

func (h *h2Conn) flushH2BodyCredit(s *h2Stream) {
	h.mu.Lock()
	connWU := uint32(h.connRecvWindowAccum)
	streamWU := uint32(s.recvWindowAccum)
	h.connRecvWindowRemaining += int64(connWU)
	s.recvWindow += int64(streamWU)
	h.connRecvWindowAccum = 0
	s.recvWindowAccum = 0
	h.mu.Unlock()
	if connWU > 0 {
		_ = h.sendWindowUpdate(0, connWU)
	}
	if streamWU > 0 {
		_ = h.sendWindowUpdate(s.id, streamWU)
	}
}

func (h *h2Conn) consumeH2Body(s *h2Stream, n int) {
	if n <= 0 {
		return
	}
	h.mu.Lock()
	h.connRecvWindowAccum += int64(n)
	s.recvWindowAccum += int64(n)
	var connWU, streamWU uint32
	if h.connRecvWindowAccum >= windowsUpdateThreshold {
		connWU = uint32(h.connRecvWindowAccum)
		h.connRecvWindowRemaining += h.connRecvWindowAccum
		h.connRecvWindowAccum = 0
	}
	if s.recvWindowAccum >= windowsUpdateThreshold {
		streamWU = uint32(s.recvWindowAccum)
		s.recvWindow += s.recvWindowAccum
		s.recvWindowAccum = 0
	}
	h.mu.Unlock()
	if connWU > 0 {
		_ = h.sendWindowUpdate(0, connWU)
	}
	if streamWU > 0 {
		_ = h.sendWindowUpdate(s.id, streamWU)
	}
}
