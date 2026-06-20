package fasthttp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orgware/fasthttp/internal/hpack"
)

var h2ClientPreface = []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")

const (
	h2Data         = uint8(0)
	h2Headers      = uint8(1)
	h2Priority     = uint8(2)
	h2RSTStream    = uint8(3)
	h2Settings     = uint8(4)
	h2PushPromise  = uint8(5)
	h2Ping         = uint8(6)
	h2GoAway       = uint8(7)
	h2WindowUpdate = uint8(8)
	h2Continuation = uint8(9)

	h2FlagEndStream  = uint8(0x1)
	h2FlagAck        = uint8(0x1)
	h2FlagEndHeaders = uint8(0x4)
	h2FlagPadded     = uint8(0x8)
	h2FlagPriority   = uint8(0x20)

	h2NoError          = uint32(0)
	h2ProtocolError    = uint32(1)
	h2InternalError    = uint32(2)
	h2FlowControlError = uint32(3)
	h2StreamClosed     = uint32(5)
	h2FrameSizeError   = uint32(6)
	h2RefusedStream    = uint32(7)
	h2Cancel           = uint32(8)
	h2CompressionError = uint32(9)
	h2EnhanceYourCalm  = uint32(11)

	h2InitialWindow = int64(65535)
	h2DefaultFrame  = uint32(16384)
	h2MaxWindow     = int64(1<<31 - 1)
)

type h2Frame struct {
	typ, flags uint8
	streamID   uint32
	payload    []byte
}

type h2Conn struct {
	app  *App
	conn net.Conn
	r    io.Reader

	writeMu sync.Mutex
	encBuf  bytes.Buffer
	enc     *hpack.Encoder
	dec     *hpack.Decoder

	mu         sync.Mutex
	streams    map[uint32]*h2Stream
	lastStream uint32
	draining   atomic.Bool
	closed     atomic.Bool

	flowMu            sync.Mutex
	flowCond          *sync.Cond
	connSendWindow    int64
	peerInitialWindow int64
	peerMaxFrame      atomic.Uint32
	peerMaxHeaderList atomic.Uint32
}

type h2Stream struct {
	id               uint32
	method           string
	path             string
	authority        string
	scheme           string
	headers          []hpack.HeaderField
	trailers         []Header
	body             []byte
	sendWindow       int64
	recvWindow       int64
	dispatched       bool
	ended            bool
	reset            atomic.Bool
	ctx              context.Context
	cancel           context.CancelFunc
	hasContentLength bool
	contentLength    int
}

type h2Response struct {
	conn   *h2Conn
	stream *h2Stream
	ended  atomic.Bool
}

func newH2Conn(app *App, conn net.Conn) *h2Conn {
	h := &h2Conn{
		app: app, conn: conn, r: conn,
		streams:           make(map[uint32]*h2Stream),
		connSendWindow:    h2InitialWindow,
		peerInitialWindow: h2InitialWindow,
	}
	h.peerMaxFrame.Store(h2DefaultFrame)
	h.peerMaxHeaderList.Store(^uint32(0))
	h.flowCond = sync.NewCond(&h.flowMu)
	h.enc = hpack.NewEncoder(&h.encBuf)
	h.dec = hpack.NewDecoder(4096, func(hpack.HeaderField) {})
	h.dec.SetMaxStringLength(app.cfg.MaxHeaderListSize)
	return h
}

func (h *h2Conn) serve(initial []byte, prefaceConsumed bool) {
	if len(initial) > 0 {
		h.r = io.MultiReader(bytes.NewReader(initial), h.conn)
	}
	if !prefaceConsumed {
		var preface [24]byte
		if _, err := io.ReadFull(h.r, preface[:]); err != nil || !bytes.Equal(preface[:], h2ClientPreface) {
			return
		}
	}
	if err := h.sendSettings(); err != nil {
		return
	}
	first := true
	for {
		f, err := h.readFrame()
		if err != nil {
			return
		}
		if first {
			first = false
			if f.typ != h2Settings || f.streamID != 0 || f.flags&h2FlagAck != 0 {
				h.connectionError(h2ProtocolError)
				return
			}
		}
		if err := h.handleFrame(f); err != nil {
			var ce h2ConnError
			if errors.As(err, &ce) {
				h.connectionError(ce.code)
			} else {
				h.connectionError(h2InternalError)
			}
			return
		}
	}
}

type h2ConnError struct{ code uint32 }

func (e h2ConnError) Error() string {
	return "http2 connection error " + strconv.FormatUint(uint64(e.code), 10)
}

func (h *h2Conn) readFrame() (h2Frame, error) {
	var head [9]byte
	if _, err := io.ReadFull(h.r, head[:]); err != nil {
		return h2Frame{}, err
	}
	length := int(head[0])<<16 | int(head[1])<<8 | int(head[2])
	if length > int(h2DefaultFrame) {
		return h2Frame{}, h2ConnError{h2FrameSizeError}
	}
	streamID := binary.BigEndian.Uint32(head[5:9])
	if streamID&0x80000000 != 0 {
		return h2Frame{}, h2ConnError{h2ProtocolError}
	}
	f := h2Frame{typ: head[3], flags: head[4], streamID: streamID, payload: make([]byte, length)}
	_, err := io.ReadFull(h.r, f.payload)
	return f, err
}

func (h *h2Conn) handleFrame(f h2Frame) error {
	switch f.typ {
	case h2Settings:
		return h.handleSettings(f)
	case h2Headers:
		return h.handleHeaders(f)
	case h2Data:
		return h.handleData(f)
	case h2WindowUpdate:
		return h.handleWindowUpdate(f)
	case h2Ping:
		if f.streamID != 0 || len(f.payload) != 8 {
			return h2ConnError{h2FrameSizeError}
		}
		if f.flags&h2FlagAck == 0 {
			return h.writeFrame(h2Ping, h2FlagAck, 0, f.payload)
		}
	case h2RSTStream:
		if f.streamID == 0 || len(f.payload) != 4 {
			return h2ConnError{h2FrameSizeError}
		}
		h.mu.Lock()
		_, exists := h.streams[f.streamID]
		idle := f.streamID > h.lastStream
		h.mu.Unlock()
		if !exists && idle {
			return h2ConnError{h2ProtocolError}
		}
		h.resetStream(f.streamID)
	case h2GoAway:
		if f.streamID != 0 || len(f.payload) < 8 {
			return h2ConnError{h2FrameSizeError}
		}
		h.draining.Store(true)
	case h2Priority:
		if f.streamID == 0 || len(f.payload) != 5 {
			return h2ConnError{h2FrameSizeError}
		}
	case h2PushPromise:
		return h2ConnError{h2ProtocolError}
	case h2Continuation:
		return h2ConnError{h2ProtocolError}
	}
	return nil
}

func (h *h2Conn) handleSettings(f h2Frame) error {
	if f.streamID != 0 {
		return h2ConnError{h2ProtocolError}
	}
	if f.flags&h2FlagAck != 0 {
		if len(f.payload) != 0 {
			return h2ConnError{h2FrameSizeError}
		}
		return nil
	}
	if len(f.payload)%6 != 0 {
		return h2ConnError{h2FrameSizeError}
	}
	for i := 0; i < len(f.payload); i += 6 {
		id, value := binary.BigEndian.Uint16(f.payload[i:i+2]), binary.BigEndian.Uint32(f.payload[i+2:i+6])
		switch id {
		case 1:
			h.writeMu.Lock()
			h.enc.SetMaxDynamicTableSize(value)
			h.writeMu.Unlock()
		case 2:
			if value > 1 {
				return h2ConnError{h2ProtocolError}
			}
		case 4:
			if value > uint32(h2MaxWindow) {
				return h2ConnError{h2FlowControlError}
			}
			h.flowMu.Lock()
			delta := int64(value) - h.peerInitialWindow
			h.peerInitialWindow = int64(value)
			h.mu.Lock()
			for _, s := range h.streams {
				s.sendWindow += delta
				if s.sendWindow > h2MaxWindow {
					h.mu.Unlock()
					h.flowMu.Unlock()
					return h2ConnError{h2FlowControlError}
				}
			}
			h.mu.Unlock()
			h.flowCond.Broadcast()
			h.flowMu.Unlock()
		case 5:
			if value < h2DefaultFrame || value > 1<<24-1 {
				return h2ConnError{h2ProtocolError}
			}
			h.peerMaxFrame.Store(value)
		case 6:
			h.peerMaxHeaderList.Store(value)
		}
	}
	return h.writeFrame(h2Settings, h2FlagAck, 0, nil)
}

func (h *h2Conn) handleHeaders(f h2Frame) error {
	if f.streamID == 0 {
		return h2ConnError{h2ProtocolError}
	}
	fragment, err := headerFragment(f)
	if err != nil {
		return err
	}
	block := append([]byte(nil), fragment...)
	for f.flags&h2FlagEndHeaders == 0 {
		next, readErr := h.readFrame()
		if readErr != nil {
			return readErr
		}
		if next.typ != h2Continuation || next.streamID != f.streamID {
			return h2ConnError{h2ProtocolError}
		}
		block = append(block, next.payload...)
		if len(block) > h.app.cfg.MaxHeaderListSize {
			return h2ConnError{h2CompressionError}
		}
		f.flags = next.flags
	}
	fields, err := h.dec.DecodeFull(block)
	if err != nil {
		return h2ConnError{h2CompressionError}
	}
	if headerListSize(fields) > h.app.cfg.MaxHeaderListSize {
		h.sendRST(f.streamID, h2EnhanceYourCalm)
		return nil
	}

	h.mu.Lock()
	s := h.streams[f.streamID]
	if s == nil {
		if f.streamID&1 == 0 || f.streamID <= h.lastStream {
			h.mu.Unlock()
			return h2ConnError{h2ProtocolError}
		}
		h.lastStream = f.streamID
		if h.draining.Load() || uint32(len(h.streams)) >= h.app.cfg.MaxConcurrentStreams {
			h.mu.Unlock()
			h.sendRST(f.streamID, h2RefusedStream)
			return nil
		}
		streamCtx, cancel := context.WithCancel(context.Background())
		s = &h2Stream{id: f.streamID, sendWindow: h.peerInitialWindow, recvWindow: h2InitialWindow, ctx: streamCtx, cancel: cancel}
		if err := validateRequestFields(s, fields); err != nil {
			h.mu.Unlock()
			h.sendRST(f.streamID, h2ProtocolError)
			return nil
		}
		h.streams[f.streamID] = s
	} else {
		if s.dispatched || s.ended || hasPseudo(fields) {
			h.mu.Unlock()
			h.sendRST(f.streamID, h2ProtocolError)
			return nil
		}
		for _, field := range fields {
			s.trailers = append(s.trailers, Header{Key: []byte(field.Name), Value: []byte(field.Value)})
		}
	}
	end := f.flags&h2FlagEndStream != 0
	if end {
		s.ended = true
	}
	h.mu.Unlock()
	if end && !validH2ContentLength(s) {
		h.sendRST(f.streamID, h2ProtocolError)
		return nil
	}
	if end {
		h.dispatch(s)
	}
	return nil
}

func headerFragment(f h2Frame) ([]byte, error) {
	p := f.payload
	pad := 0
	if f.flags&h2FlagPadded != 0 {
		if len(p) == 0 {
			return nil, h2ConnError{h2ProtocolError}
		}
		pad, p = int(p[0]), p[1:]
	}
	if f.flags&h2FlagPriority != 0 {
		if len(p) < 5 {
			return nil, h2ConnError{h2FrameSizeError}
		}
		if binary.BigEndian.Uint32(p[:4])&0x7fffffff == f.streamID {
			return nil, h2ConnError{h2ProtocolError}
		}
		p = p[5:]
	}
	if pad > len(p) {
		return nil, h2ConnError{h2ProtocolError}
	}
	return p[:len(p)-pad], nil
}

func (h *h2Conn) handleData(f h2Frame) error {
	if f.streamID == 0 {
		return h2ConnError{h2ProtocolError}
	}
	p := f.payload
	if f.flags&h2FlagPadded != 0 {
		if len(p) == 0 || int(p[0]) >= len(p) {
			return h2ConnError{h2ProtocolError}
		}
		p = p[1 : len(p)-int(p[0])]
	}
	h.mu.Lock()
	s := h.streams[f.streamID]
	if s == nil {
		idle := f.streamID > h.lastStream
		h.mu.Unlock()
		if idle {
			return h2ConnError{h2ProtocolError}
		}
		h.sendRST(f.streamID, h2StreamClosed)
		return nil
	}
	if s.ended {
		h.mu.Unlock()
		h.sendRST(f.streamID, h2StreamClosed)
		return nil
	}
	if len(s.body)+len(p) > h.app.cfg.MaxRequestBodySize {
		h.mu.Unlock()
		h.sendRST(f.streamID, h2Cancel)
		return nil
	}
	s.body = append(s.body, p...)
	s.recvWindow -= int64(len(f.payload))
	if s.recvWindow < 0 {
		h.mu.Unlock()
		return h2ConnError{h2FlowControlError}
	}
	s.recvWindow += int64(len(f.payload))
	end := f.flags&h2FlagEndStream != 0
	if end {
		s.ended = true
	}
	h.mu.Unlock()
	if len(f.payload) > 0 {
		_ = h.sendWindowUpdate(0, uint32(len(f.payload)))
		_ = h.sendWindowUpdate(f.streamID, uint32(len(f.payload)))
	}
	if end && !validH2ContentLength(s) {
		h.sendRST(f.streamID, h2ProtocolError)
		return nil
	}
	if end {
		h.dispatch(s)
	}
	return nil
}

func (h *h2Conn) handleWindowUpdate(f h2Frame) error {
	if len(f.payload) != 4 {
		return h2ConnError{h2FrameSizeError}
	}
	inc := int64(binary.BigEndian.Uint32(f.payload) & 0x7fffffff)
	if inc == 0 {
		return h2ConnError{h2ProtocolError}
	}
	h.flowMu.Lock()
	defer h.flowMu.Unlock()
	if f.streamID == 0 {
		if h.connSendWindow+inc > h2MaxWindow {
			return h2ConnError{h2FlowControlError}
		}
		h.connSendWindow += inc
	} else {
		h.mu.Lock()
		s := h.streams[f.streamID]
		idle := s == nil && f.streamID > h.lastStream
		if idle {
			h.mu.Unlock()
			return h2ConnError{h2ProtocolError}
		}
		if s != nil {
			if s.sendWindow+inc > h2MaxWindow {
				h.mu.Unlock()
				return h2ConnError{h2FlowControlError}
			}
			s.sendWindow += inc
		}
		h.mu.Unlock()
	}
	h.flowCond.Broadcast()
	return nil
}

func (h *h2Conn) dispatch(s *h2Stream) {
	h.mu.Lock()
	if s.dispatched || s.reset.Load() {
		h.mu.Unlock()
		return
	}
	s.dispatched = true
	h.mu.Unlock()
	go func() {
		ctx := acquireCtx(h.conn, h.app)
		ctx.h2 = &h2Response{conn: h, stream: s}
		ctx.Header.Method = []byte(s.method)
		ctx.Header.URI = []byte(s.path)
		ctx.Header.Proto = []byte("HTTP/2.0")
		ctx.Header.Host = []byte(s.authority)
		ctx.Header.KeepAlive = true
		if cap(ctx.Header.headers) < maxHeaders {
			ctx.Header.headers = make([]Header, maxHeaders)
		} else {
			ctx.Header.headers = ctx.Header.headers[:maxHeaders]
		}
		for _, field := range s.headers {
			if ctx.Header.hcount >= maxHeaders {
				break
			}
			ctx.Header.headers[ctx.Header.hcount] = Header{Key: []byte(field.Name), Value: []byte(field.Value)}
			ctx.Header.hcount++
			if field.Name == "content-type" {
				ctx.Header.ContentType = []byte(field.Value)
			}
		}
		ctx.Header.HasContentLength, ctx.Header.ContentLength = s.hasContentLength, s.contentLength
		ctx.body, ctx.trailers = s.body, s.trailers
		ctx.SetContext(s.ctx)
		h.app.dispatch(ctx)
		if !ctx.responded && !s.reset.Load() {
			_ = ctx.SendStatus(200)
		}
		s.cancel()
		releaseCtx(ctx)
	}()
}

func validateRequestFields(s *h2Stream, fields []hpack.HeaderField) error {
	pseudoDone := false
	seen := make(map[string]bool, 4)
	cookies := ""
	for _, f := range fields {
		if f.Name == "" || strings.ToLower(f.Name) != f.Name || strings.IndexAny(f.Value, "\x00\r\n") >= 0 {
			return errors.New("uppercase header")
		}
		if strings.HasPrefix(f.Name, ":") {
			if pseudoDone || seen[f.Name] {
				return errors.New("invalid pseudo header")
			}
			seen[f.Name] = true
			switch f.Name {
			case ":method":
				s.method = f.Value
			case ":path":
				s.path = f.Value
			case ":authority":
				s.authority = f.Value
			case ":scheme":
				s.scheme = f.Value
			default:
				return errors.New("unknown pseudo header")
			}
			continue
		}
		pseudoDone = true
		if !validToken([]byte(f.Name)) {
			return errors.New("invalid header name")
		}
		switch f.Name {
		case "connection", "proxy-connection", "keep-alive", "upgrade", "transfer-encoding":
			return errors.New("connection header")
		case "te":
			if !strEqFold([]byte(trimSpace(f.Value)), "trailers") {
				return errors.New("invalid te")
			}
		case "content-length":
			n, ok := parseContentLength([]byte(f.Value))
			if !ok || (s.hasContentLength && n != s.contentLength) {
				return errors.New("invalid content-length")
			}
			s.hasContentLength, s.contentLength = true, n
		case "host":
			if seen["host"] {
				return errors.New("duplicate host")
			}
			seen["host"] = true
			if s.authority == "" {
				s.authority = f.Value
			} else if s.authority != f.Value {
				return errors.New("authority mismatch")
			}
		case "cookie":
			if cookies != "" {
				cookies += "; "
			}
			cookies += f.Value
			continue
		}
		s.headers = append(s.headers, f)
	}
	if cookies != "" {
		s.headers = append(s.headers, hpack.HeaderField{Name: "cookie", Value: cookies})
	}
	if s.method == "" || !validToken([]byte(s.method)) || s.authority == "" {
		return errors.New("missing pseudo header")
	}
	if s.method == "CONNECT" {
		if s.scheme != "" || s.path != "" {
			return errors.New("invalid connect pseudo header")
		}
		s.path = s.authority
	} else if s.path == "" || s.scheme == "" {
		return errors.New("missing pseudo header")
	}
	if s.method != "CONNECT" && s.path != "*" && s.path[0] != '/' {
		return errors.New("invalid path")
	}
	if s.path == "*" && s.method != "OPTIONS" {
		return errors.New("invalid asterisk path")
	}
	return nil
}

func validH2ContentLength(s *h2Stream) bool {
	return !s.hasContentLength || s.contentLength == len(s.body)
}

func trimSpace(s string) string { return strings.Trim(s, " \t") }
func hasPseudo(fields []hpack.HeaderField) bool {
	for _, f := range fields {
		if strings.HasPrefix(f.Name, ":") {
			return true
		}
	}
	return false
}
func headerListSize(fields []hpack.HeaderField) int {
	n := 0
	for _, f := range fields {
		n += len(f.Name) + len(f.Value) + 32
	}
	return n
}

func (r *h2Response) writeResponse(c *Ctx, body []byte) error {
	if c.responded || r.ended.Load() {
		return nil
	}
	if err := c.runBeforeResponse(); err != nil { return err }
	if c.bodyTransform != nil {
		var err error
		body, err = c.bodyTransform(body)
		if err != nil {
			return err
		}
	}
	c.responded = true
	bodyAllowed := responseBodyAllowed(c.status) && !bytesEqualFold(c.Header.Method, methodHEAD)
	length := len(body)
	end := !bodyAllowed || length == 0
	var contentLength *int
	if responseBodyAllowed(c.status) {
		contentLength = &length
	}
	if err := r.conn.sendResponseHeaders(r.stream, c, contentLength, end); err != nil {
		r.abort(h2InternalError)
		return err
	}
	if end {
		r.finish()
		return nil
	}
	return r.writeData(body, true)
}

func (r *h2Response) beginStream(c *Ctx) error {
	if r.ended.Load() {
		return net.ErrClosed
	}
	end := !responseBodyAllowed(c.status) || bytesEqualFold(c.Header.Method, methodHEAD)
	if err := r.conn.sendResponseHeaders(r.stream, c, nil, end); err != nil {
		r.abort(h2InternalError)
		return err
	}
	if end {
		r.finish()
	}
	return nil
}

func (r *h2Response) writeData(data []byte, end bool) error {
	if r.ended.Load() {
		return nil
	}
	if err := r.conn.sendData(r.stream, data, end); err != nil {
		r.abort(h2InternalError)
		return err
	}
	if end {
		r.finish()
	}
	return nil
}

func (r *h2Response) abort(code uint32) {
	if r.ended.CompareAndSwap(false, true) {
		r.conn.sendRST(r.stream.id, code)
	}
}

func (r *h2Response) finish() {
	if !r.ended.CompareAndSwap(false, true) {
		return
	}
	r.conn.mu.Lock()
	delete(r.conn.streams, r.stream.id)
	empty, draining := len(r.conn.streams) == 0, r.conn.draining.Load()
	r.conn.mu.Unlock()
	if empty && draining {
		_ = r.conn.conn.Close()
	}
}

func (h *h2Conn) sendResponseHeaders(s *h2Stream, c *Ctx, contentLength *int, end bool) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	h.encBuf.Reset()
	fields := []hpack.HeaderField{{Name: ":status", Value: strconv.Itoa(c.status)}}
	if c.contentType != nil {
		fields = append(fields, hpack.HeaderField{Name: "content-type", Value: string(c.contentType)})
	}
	if contentLength != nil {
		fields = append(fields, hpack.HeaderField{Name: "content-length", Value: strconv.Itoa(*contentLength)})
	}
	for i := 0; i < c.chCount; i++ {
		name := strings.ToLower(string(c.customHeaders[i].Key))
		if name == "connection" || name == "transfer-encoding" || name == "upgrade" || name == "keep-alive" {
			continue
		}
		fields = append(fields, hpack.HeaderField{Name: name, Value: string(c.customHeaders[i].Value)})
	}
	for i := range c.extraHeaders {
		name := strings.ToLower(string(c.extraHeaders[i].Key))
		if name == "connection" || name == "transfer-encoding" || name == "upgrade" || name == "keep-alive" {
			continue
		}
		fields = append(fields, hpack.HeaderField{Name: name, Value: string(c.extraHeaders[i].Value)})
	}
	for i := range c.responseCookies {
		fields = append(fields, hpack.HeaderField{Name: "set-cookie", Value: c.responseCookies[i].String()})
	}
	if headerListSize(fields) > int(h.peerMaxHeaderList.Load()) {
		return h2ConnError{h2InternalError}
	}
	for _, field := range fields {
		if err := h.enc.WriteField(field); err != nil {
			return err
		}
	}
	block := h.encBuf.Bytes()
	flags := h2FlagEndHeaders
	if end {
		flags |= h2FlagEndStream
	}
	return h.writeHeaderBlockLocked(s.id, flags, block)
}

func (h *h2Conn) writeHeaderBlockLocked(streamID uint32, flags uint8, block []byte) error {
	max := int(h.peerMaxFrame.Load())
	first := true
	for first || len(block) > 0 {
		n := minInt(len(block), max)
		part := block[:n]
		block = block[n:]
		typ, frameFlags := h2Continuation, uint8(0)
		if first {
			typ, frameFlags, first = h2Headers, flags&h2FlagEndStream, false
		}
		if len(block) == 0 {
			frameFlags |= h2FlagEndHeaders
		}
		if err := h.writeFrameLocked(typ, frameFlags, streamID, part); err != nil {
			return err
		}
	}
	return nil
}

func (h *h2Conn) sendData(s *h2Stream, data []byte, end bool) error {
	if len(data) == 0 && end {
		return h.writeFrame(h2Data, h2FlagEndStream, s.id, nil)
	}
	for len(data) > 0 {
		h.flowMu.Lock()
		deadline := time.Now().Add(h.app.cfg.WriteTimeout)
		for !h.closed.Load() && (h.connSendWindow <= 0 || s.sendWindow <= 0) {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				h.flowMu.Unlock()
				return os.ErrDeadlineExceeded
			}
			timer := time.AfterFunc(remaining, func() { h.flowMu.Lock(); h.flowCond.Broadcast(); h.flowMu.Unlock() })
			h.flowCond.Wait()
			timer.Stop()
		}
		if h.closed.Load() || s.reset.Load() {
			h.flowMu.Unlock()
			return net.ErrClosed
		}
		n := int64(len(data))
		if n > int64(h.peerMaxFrame.Load()) {
			n = int64(h.peerMaxFrame.Load())
		}
		if n > h.connSendWindow {
			n = h.connSendWindow
		}
		if n > s.sendWindow {
			n = s.sendWindow
		}
		h.connSendWindow -= n
		s.sendWindow -= n
		h.flowMu.Unlock()
		flags := uint8(0)
		if int(n) == len(data) && end {
			flags = h2FlagEndStream
		}
		if err := h.writeFrame(h2Data, flags, s.id, data[:n]); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (h *h2Conn) sendSettings() error {
	var p [18]byte
	binary.BigEndian.PutUint16(p[0:2], 2)
	binary.BigEndian.PutUint32(p[2:6], 0)
	binary.BigEndian.PutUint16(p[6:8], 3)
	binary.BigEndian.PutUint32(p[8:12], h.app.cfg.MaxConcurrentStreams)
	binary.BigEndian.PutUint16(p[12:14], 6)
	binary.BigEndian.PutUint32(p[14:18], uint32(h.app.cfg.MaxHeaderListSize))
	return h.writeFrame(h2Settings, 0, 0, p[:])
}

func (h *h2Conn) writeFrame(typ, flags uint8, streamID uint32, payload []byte) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	return h.writeFrameLocked(typ, flags, streamID, payload)
}

func (h *h2Conn) writeFrameLocked(typ, flags uint8, streamID uint32, payload []byte) error {
	if timeout := h.app.cfg.WriteTimeout; timeout > 0 {
		_ = h.conn.SetWriteDeadline(time.Now().Add(timeout))
	}
	var head [9]byte
	n := len(payload)
	head[0], head[1], head[2] = byte(n>>16), byte(n>>8), byte(n)
	head[3], head[4] = typ, flags
	binary.BigEndian.PutUint32(head[5:9], streamID&0x7fffffff)
	if err := writeAll(h.conn, head[:]); err != nil {
		h.markClosed()
		return err
	}
	if len(payload) > 0 {
		if err := writeAll(h.conn, payload); err != nil {
			h.markClosed()
			return err
		}
	}
	return nil
}

func (h *h2Conn) sendWindowUpdate(streamID, increment uint32) error {
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], increment)
	return h.writeFrame(h2WindowUpdate, 0, streamID, p[:])
}
func (h *h2Conn) sendRST(streamID, code uint32) {
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], code)
	_ = h.writeFrame(h2RSTStream, 0, streamID, p[:])
	h.resetStream(streamID)
}
func (h *h2Conn) resetStream(id uint32) {
	h.mu.Lock()
	s := h.streams[id]
	if s != nil {
		s.reset.Store(true)
		delete(h.streams, id)
		if s.cancel != nil {
			s.cancel()
		}
	}
	h.mu.Unlock()
	h.flowMu.Lock()
	h.flowCond.Broadcast()
	h.flowMu.Unlock()
}

func (h *h2Conn) connectionError(code uint32) { h.sendGoAway(code); h.markClosed(); _ = h.conn.Close() }
func (h *h2Conn) sendGoAway(code uint32) {
	h.mu.Lock()
	last := h.lastStream
	h.mu.Unlock()
	var p [8]byte
	binary.BigEndian.PutUint32(p[0:4], last)
	binary.BigEndian.PutUint32(p[4:8], code)
	_ = h.writeFrame(h2GoAway, 0, 0, p[:])
}
func (h *h2Conn) markClosed() {
	h.closed.Store(true)
	h.flowMu.Lock()
	h.flowCond.Broadcast()
	h.flowMu.Unlock()
}
func (h *h2Conn) startDrain() {
	if !h.draining.CompareAndSwap(false, true) {
		return
	}
	h.sendGoAway(h2NoError)
	h.mu.Lock()
	empty := len(h.streams) == 0
	h.mu.Unlock()
	if empty {
		_ = h.conn.Close()
	}
}
