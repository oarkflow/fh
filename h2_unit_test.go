package fh

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/hpack"
)

type h2discardConn struct{}

func (h2discardConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (h2discardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (h2discardConn) Close() error                     { return nil }
func (h2discardConn) LocalAddr() net.Addr              { return dummyAddr("local") }
func (h2discardConn) RemoteAddr() net.Addr             { return dummyAddr("remote") }
func (h2discardConn) SetDeadline(time.Time) error      { return nil }
func (h2discardConn) SetReadDeadline(time.Time) error  { return nil }
func (h2discardConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }

type h2BufferConn struct {
	bytes.Buffer
}

func (h2BufferConn) Close() error                     { return nil }
func (h2BufferConn) LocalAddr() net.Addr              { return dummyAddr("local") }
func (h2BufferConn) RemoteAddr() net.Addr             { return dummyAddr("remote") }
func (h2BufferConn) SetDeadline(time.Time) error      { return nil }
func (h2BufferConn) SetReadDeadline(time.Time) error  { return nil }
func (h2BufferConn) SetWriteDeadline(time.Time) error { return nil }

func newTestH2Conn(t *testing.T) *h2Conn {
	t.Helper()
	app := &App{}
	app.cfg.MaxConcurrentStreams = 16
	app.cfg.MaxHeaderListSize = 64 << 10
	app.cfg.MaxRequestBodySize = 1 << 20
	app.cfg.WriteTimeout = 250 * time.Millisecond
	return newH2Conn(app, h2discardConn{})
}

func TestH2PushPromiseFragmentation(t *testing.T) {
	conn := &h2BufferConn{}
	app := &App{}
	app.cfg.MaxConcurrentStreams = 16
	h := newH2Conn(app, conn)
	h.peerMaxFrame.Store(16)
	r := &h2Response{conn: h, stream: &h2Stream{id: 1, authority: "example.test"}}

	if !r.pushPromise("/assets/app.js", "GET", map[string]string{
		"x-one": strings.Repeat("a", 32),
		"x-two": strings.Repeat("b", 32),
	}) {
		t.Fatal("push promise was rejected")
	}

	raw := conn.Bytes()
	if len(raw) < 9 {
		t.Fatalf("short frame output: %d bytes", len(raw))
	}
	offset := 0
	frames := 0
	for offset < len(raw) {
		if len(raw)-offset < 9 {
			t.Fatalf("truncated frame header at %d", offset)
		}
		length := int(raw[offset])<<16 | int(raw[offset+1])<<8 | int(raw[offset+2])
		if length > 16 || offset+9+length > len(raw) {
			t.Fatalf("invalid frame length %d at %d", length, offset)
		}
		typ, flags := raw[offset+3], raw[offset+4]
		if frames == 0 {
			if typ != h2PushPromise || length < 4 {
				t.Fatalf("first frame = type %d, length %d", typ, length)
			}
			if binary.BigEndian.Uint32(raw[offset+9:offset+13]) != 2 {
				t.Fatalf("promised stream id = %d", binary.BigEndian.Uint32(raw[offset+9:offset+13]))
			}
			if flags&h2FlagEndHeaders != 0 {
				t.Fatal("fragmented PUSH_PROMISE ended its headers too early")
			}
		} else if typ != h2Continuation {
			t.Fatalf("frame %d type = %d, want CONTINUATION", frames, typ)
		}
		if offset+9+length == len(raw) && flags&h2FlagEndHeaders == 0 {
			t.Fatal("final header fragment omitted END_HEADERS")
		}
		offset += 9 + length
		frames++
	}
	if frames < 2 {
		t.Fatalf("push promise was not fragmented: %d frame(s)", frames)
	}
}

func TestH2PushStreamStateIsReleasedOnFinish(t *testing.T) {
	h := newTestH2Conn(t)
	h.pushState.mu.Lock()
	h.pushState.streams[2] = true
	h.pushState.mu.Unlock()
	h.mu.Lock()
	h.streams[2] = &h2Stream{id: 2}
	h.mu.Unlock()

	(&h2Response{conn: h, stream: &h2Stream{id: 2}}).finish()

	h.pushState.mu.Lock()
	_, retained := h.pushState.streams[2]
	h.pushState.mu.Unlock()
	if retained {
		t.Fatal("completed push stream remained counted")
	}
}

func TestH2PushStreamStateIsReleasedOnReset(t *testing.T) {
	h := newTestH2Conn(t)
	h.pushState.mu.Lock()
	h.pushState.streams[2] = true
	h.pushState.mu.Unlock()
	h.mu.Lock()
	h.streams[2] = &h2Stream{id: 2}
	h.mu.Unlock()

	h.resetStream(2)

	h.pushState.mu.Lock()
	_, retained := h.pushState.streams[2]
	h.pushState.mu.Unlock()
	if retained {
		t.Fatal("reset push stream remained counted")
	}
}

func TestH2PushLimitReservesConcurrentPromise(t *testing.T) {
	h := newTestH2Conn(t)
	h.pushState.maxPush = 1
	h.peerMaxConcurrentStreams.Store(1)
	h.mu.Lock()
	h.streams[1] = &h2Stream{id: 1}
	h.mu.Unlock()

	if got := h.allocatePushID(); got != 2 {
		t.Fatalf("first push stream id = %d, want 2", got)
	}
	if got := h.allocatePushID(); got != 0 {
		t.Fatalf("second concurrent push stream id = %d, want rejection", got)
	}

	h.releasePushReservation()
	if got := h.allocatePushID(); got != 4 {
		t.Fatalf("push stream id after releasing reservation = %d, want 4", got)
	}
	h.releasePushReservation()
}

func mustH2ErrCode(t *testing.T, err error, want uint32) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected h2ConnError code %d, got nil", want)
	}
	var ce h2ConnError
	if !errors.As(err, &ce) {
		t.Fatalf("expected h2ConnError code %d, got %T: %v", want, err, err)
	}
	if ce.code != want {
		t.Fatalf("expected h2ConnError code %d, got %d", want, ce.code)
	}
}

func encodeHeaderBlock(t *testing.T, fields ...hpack.HeaderField) []byte {
	t.Helper()
	var b bytes.Buffer
	enc := hpack.NewEncoder(&b)
	for _, f := range fields {
		if err := enc.WriteField(f); err != nil {
			t.Fatalf("encode header field %q: %v", f.Name, err)
		}
	}
	return b.Bytes()
}

func TestH2HeaderFragmentPaddingAndPriority(t *testing.T) {
	t.Run("plain", func(t *testing.T) {
		got, err := headerFragment(h2Frame{streamID: 1, payload: []byte("abc")})
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "abc" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("padded", func(t *testing.T) {
		got, err := headerFragment(h2Frame{streamID: 1, flags: h2FlagPadded, payload: []byte{2, 'a', 'b', 'x', 'y'}})
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "ab" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("invalid padding", func(t *testing.T) {
		_, err := headerFragment(h2Frame{streamID: 1, flags: h2FlagPadded, payload: []byte{3, 'a'}})
		mustH2ErrCode(t, err, h2ProtocolError)
	})

	t.Run("priority self dependency", func(t *testing.T) {
		var p [5]byte
		binary.BigEndian.PutUint32(p[:4], 1)
		_, err := headerFragment(h2Frame{streamID: 1, flags: h2FlagPriority, payload: p[:]})
		mustH2ErrCode(t, err, h2ProtocolError)
	})
}

func TestH2HeadersOnClosedStreamAreConnectionErrors(t *testing.T) {
	block := encodeHeaderBlock(t,
		hpack.HeaderField{Name: ":method", Value: "GET"},
		hpack.HeaderField{Name: ":path", Value: "/"},
		hpack.HeaderField{Name: ":scheme", Value: "https"},
		hpack.HeaderField{Name: ":authority", Value: "example.test"},
	)

	t.Run("removed stream", func(t *testing.T) {
		h := newTestH2Conn(t)
		h.lastStream = 1
		mustH2ErrCode(t, h.handleHeaders(h2Frame{
			typ: h2Headers, flags: h2FlagEndHeaders, streamID: 1, payload: block,
		}), h2ErrStreamClosed)
	})

	t.Run("closed state", func(t *testing.T) {
		h := newTestH2Conn(t)
		s := &h2Stream{id: 1}
		s.state.Store(int32(stateClosed))
		h.streams[1] = s
		h.lastStream = 1
		mustH2ErrCode(t, h.handleHeaders(h2Frame{
			typ: h2Headers, flags: h2FlagEndHeaders, streamID: 1, payload: block,
		}), h2ErrStreamClosed)
	})
}

func TestH2ContinuationIgnoresEndStreamBit(t *testing.T) {
	if got := continuationFlags(h2FlagEndStream | h2FlagEndHeaders); got != h2FlagEndHeaders {
		t.Fatalf("continuation flags = %#x, want only END_HEADERS", got)
	}
}

func TestH2ValidateRequestFields(t *testing.T) {
	t.Run("valid get", func(t *testing.T) {
		s := &h2Stream{}
		err := validateRequestFields(s, []hpack.HeaderField{
			{Name: ":method", Value: "GET"},
			{Name: ":scheme", Value: "https"},
			{Name: ":authority", Value: "example.com"},
			{Name: ":path", Value: "/hello"},
			{Name: "accept", Value: "*/*"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if s.method != "GET" || s.scheme != "https" || s.authority != "example.com" || s.path != "/hello" {
			t.Fatalf("unexpected stream fields: %+v", s)
		}
	})

	t.Run("uppercase header rejected", func(t *testing.T) {
		s := &h2Stream{}
		err := validateRequestFields(s, []hpack.HeaderField{
			{Name: ":method", Value: "GET"},
			{Name: ":scheme", Value: "https"},
			{Name: ":authority", Value: "example.com"},
			{Name: ":path", Value: "/"},
			{Name: "X-Test", Value: "1"},
		})
		if err == nil {
			t.Fatal("expected uppercase header rejection")
		}
	})

	t.Run("pseudo after regular rejected", func(t *testing.T) {
		s := &h2Stream{}
		err := validateRequestFields(s, []hpack.HeaderField{
			{Name: ":method", Value: "GET"},
			{Name: "accept", Value: "*/*"},
			{Name: ":scheme", Value: "https"},
			{Name: ":authority", Value: "example.com"},
			{Name: ":path", Value: "/"},
		})
		if err == nil {
			t.Fatal("expected pseudo-after-regular rejection")
		}
	})

	t.Run("duplicate content length mismatch rejected", func(t *testing.T) {
		s := &h2Stream{}
		err := validateRequestFields(s, []hpack.HeaderField{
			{Name: ":method", Value: "POST"},
			{Name: ":scheme", Value: "https"},
			{Name: ":authority", Value: "example.com"},
			{Name: ":path", Value: "/"},
			{Name: "content-length", Value: "10"},
			{Name: "content-length", Value: "11"},
		})
		if err == nil {
			t.Fatal("expected content-length mismatch rejection")
		}
	})

	t.Run("cookie coalescing", func(t *testing.T) {
		s := &h2Stream{}
		err := validateRequestFields(s, []hpack.HeaderField{
			{Name: ":method", Value: "GET"},
			{Name: ":scheme", Value: "https"},
			{Name: ":authority", Value: "example.com"},
			{Name: ":path", Value: "/"},
			{Name: "cookie", Value: "a=1"},
			{Name: "cookie", Value: "b=2"},
		})
		if err != nil {
			t.Fatal(err)
		}
		var cookie string
		for _, h := range s.headers {
			if h.Name == "cookie" {
				cookie = h.Value
			}
		}
		if cookie != "a=1; b=2" {
			t.Fatalf("cookie = %q", cookie)
		}
	})

	for _, path := range []string{"/has space", "/fragment#part", "/tab\there"} {
		t.Run("invalid path "+path, func(t *testing.T) {
			s := &h2Stream{}
			err := validateRequestFields(s, []hpack.HeaderField{
				{Name: ":method", Value: "GET"},
				{Name: ":scheme", Value: "https"},
				{Name: ":authority", Value: "example.com"},
				{Name: ":path", Value: path},
			})
			if err == nil {
				t.Fatalf("accepted invalid HTTP/2 path %q", path)
			}
		})
	}
}

func TestH2RequestContextSplitsPathAndQuery(t *testing.T) {
	h := newTestH2Conn(t)
	streamCtx, cancel := h.newStreamContext()
	defer cancel()
	s := &h2Stream{
		id: 1, method: "GET", path: "/search?q=go%20server&empty=", authority: "example.test",
		ctx: streamCtx, cancel: cancel,
	}
	ctx := h.acquireRequestCtx(s)
	defer releaseCtx(ctx)
	if got := ctx.Path(); got != "/search" {
		t.Fatalf("Path() = %q, want /search", got)
	}
	if got := ctx.Query("q"); got != "go server" {
		t.Fatalf("Query(q) = %q, want %q", got, "go server")
	}
	if got := string(ctx.RequestHeader().QueryString); got != "q=go%20server&empty=" {
		t.Fatalf("QueryString = %q", got)
	}
}

func TestH2HandlerTimeoutStartsAtDispatch(t *testing.T) {
	h := newTestH2Conn(t)
	h.app.cfg.HandlerTimeout = 10 * time.Millisecond
	ctx, cancel := h.newStreamContext()
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("handler deadline started before the request body completed")
	}
	time.Sleep(20 * time.Millisecond)
	if err := ctx.Err(); err != nil {
		t.Fatalf("pre-dispatch stream context expired: %v", err)
	}
}

func TestH2ValidateRequestTrailers(t *testing.T) {
	valid, err := validateRequestTrailers([]hpack.HeaderField{{Name: "x-checksum", Value: "abc"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(valid) != 1 || string(valid[0].Key) != "x-checksum" {
		t.Fatalf("unexpected trailers: %+v", valid)
	}

	badCases := [][]hpack.HeaderField{
		{{Name: ":path", Value: "/"}},
		{{Name: "Content-Length", Value: "1"}},
		{{Name: "content-length", Value: "1"}},
		{{Name: "connection", Value: "close"}},
		{{Name: "x-test", Value: "bad\r\nvalue"}},
	}
	for _, tc := range badCases {
		if _, err := validateRequestTrailers(tc); err == nil {
			t.Fatalf("expected invalid trailers rejected: %+v", tc)
		}
	}
}

func TestH2ValidResponseFieldAndTrailerFiltering(t *testing.T) {
	if !validResponseField("x-test", []byte("ok")) {
		t.Fatal("expected valid response field")
	}
	if validResponseField("X-Test", []byte("ok")) {
		t.Fatal("expected uppercase response field rejection")
	}
	if validResponseField("x-test", []byte("bad\nvalue")) {
		t.Fatal("expected bad response value rejection")
	}
	if !forbiddenH2ResponseHeader("connection") {
		t.Fatal("connection must be forbidden")
	}
	if !forbiddenH2Trailer("content-length") {
		t.Fatal("content-length trailer must be forbidden")
	}
}

func TestH2SettingsValidation(t *testing.T) {
	h := newTestH2Conn(t)

	t.Run("invalid enable push", func(t *testing.T) {
		var p [6]byte
		binary.BigEndian.PutUint16(p[0:2], 2)
		binary.BigEndian.PutUint32(p[2:6], 2)
		mustH2ErrCode(t, h.applySettings(p[:]), h2ProtocolError)
	})

	t.Run("invalid initial window", func(t *testing.T) {
		var p [6]byte
		binary.BigEndian.PutUint16(p[0:2], 4)
		binary.BigEndian.PutUint32(p[2:6], uint32(h2MaxWindow+1))
		mustH2ErrCode(t, h.applySettings(p[:]), h2FlowControlError)
	})

	t.Run("invalid max frame size", func(t *testing.T) {
		var p [6]byte
		binary.BigEndian.PutUint16(p[0:2], 5)
		binary.BigEndian.PutUint32(p[2:6], h2DefaultFrame-1)
		mustH2ErrCode(t, h.applySettings(p[:]), h2ProtocolError)
	})
}

func TestH2FrameValidation(t *testing.T) {
	h := newTestH2Conn(t)

	t.Run("priority self dependency", func(t *testing.T) {
		var p [5]byte
		binary.BigEndian.PutUint32(p[:4], 3)
		mustH2ErrCode(t, h.handleFrame(h2Frame{typ: h2Priority, streamID: 3, payload: p[:]}), h2ProtocolError)
	})

	t.Run("window update zero increment", func(t *testing.T) {
		var p [4]byte
		mustH2ErrCode(t, h.handleFrame(h2Frame{typ: h2WindowUpdate, streamID: 0, payload: p[:]}), h2ProtocolError)
	})

	t.Run("unknown extension ignored", func(t *testing.T) {
		if err := h.handleFrame(h2Frame{typ: 99, streamID: 0, payload: []byte("ignored")}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("push promise rejected", func(t *testing.T) {
		mustH2ErrCode(t, h.handleFrame(h2Frame{typ: h2PushPromise, streamID: 1}), h2ProtocolError)
	})
}

func TestH2ReadFrame(t *testing.T) {
	var raw bytes.Buffer
	payload := []byte("hello")
	var head [9]byte
	head[0], head[1], head[2] = 0, 0, byte(len(payload))
	head[3], head[4] = h2Data, h2FlagEndStream
	binary.BigEndian.PutUint32(head[5:9], 1)
	raw.Write(head[:])
	raw.Write(payload)

	h := newTestH2Conn(t)
	h.r = &raw
	f, err := h.readFrame()
	if err != nil {
		t.Fatal(err)
	}
	if f.typ != h2Data || f.flags != h2FlagEndStream || f.streamID != 1 || string(f.payload) != "hello" {
		t.Fatalf("bad frame: %+v", f)
	}
}

func TestH2ReadFrameRejectsOversizedPayload(t *testing.T) {
	var raw bytes.Buffer
	var head [9]byte
	length := int(h2DefaultFrame) + 1
	head[0], head[1], head[2] = byte(length>>16), byte(length>>8), byte(length)
	head[3] = h2Data
	binary.BigEndian.PutUint32(head[5:9], 1)
	raw.Write(head[:])

	h := newTestH2Conn(t)
	h.r = &raw
	_, err := h.readFrame()
	mustH2ErrCode(t, err, h2FrameSizeError)
}

func TestH2HandleHeadersCreatesStream(t *testing.T) {
	h := newTestH2Conn(t)
	block := encodeHeaderBlock(t,
		hpack.HeaderField{Name: ":method", Value: "POST"},
		hpack.HeaderField{Name: ":scheme", Value: "https"},
		hpack.HeaderField{Name: ":authority", Value: "example.com"},
		hpack.HeaderField{Name: ":path", Value: "/submit"},
		hpack.HeaderField{Name: "content-length", Value: "5"},
	)
	err := h.handleHeaders(h2Frame{typ: h2Headers, flags: h2FlagEndHeaders, streamID: 1, payload: block})
	if err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	s := h.streams[1]
	h.mu.Unlock()
	if s == nil {
		t.Fatal("stream not created")
	}
	if s.method != "POST" || s.path != "/submit" || !s.hasContentLength || s.contentLength != 5 {
		t.Fatalf("unexpected stream: %+v", s)
	}
}

func TestH2ContentLengthValidation(t *testing.T) {
	s := &h2Stream{hasContentLength: true, contentLength: 3, body: []byte("abc")}
	if !validH2ContentLength(s) {
		t.Fatal("expected valid content length")
	}
	s.body = []byte("abcd")
	if validH2ContentLength(s) {
		t.Fatal("expected invalid content length")
	}
}

func TestH2LowerHeaderName(t *testing.T) {
	if got := lowerHeaderName([]byte("content-type")); got != "content-type" {
		t.Fatalf("got %q", got)
	}
	if got := lowerHeaderName([]byte("Content-Type")); got != "content-type" {
		t.Fatalf("got %q", got)
	}
}

func TestH2CloseAllStreamsCancelsContexts(t *testing.T) {
	h := newTestH2Conn(t)
	ctx, cancel := context.WithCancel(context.Background())
	h.streams[1] = &h2Stream{id: 1, ctx: ctx, cancel: cancel}
	h.closeAllStreams()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("stream context was not cancelled")
	}
}

func TestH2HeaderListSize(t *testing.T) {
	fields := []hpack.HeaderField{{Name: "x", Value: strings.Repeat("a", 10)}}
	if got := headerListSize(fields); got != 43 {
		t.Fatalf("got %d", got)
	}
}
