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

func newTestH2Conn(t *testing.T) *h2Conn {
	t.Helper()
	app := &App{}
	app.cfg.MaxConcurrentStreams = 16
	app.cfg.MaxHeaderListSize = 64 << 10
	app.cfg.MaxRequestBodySize = 1 << 20
	app.cfg.WriteTimeout = 250 * time.Millisecond
	return newH2Conn(app, h2discardConn{})
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
