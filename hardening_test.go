package fh

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestRouterStaticPrecedenceAndFallback(t *testing.T) {
	r := newRouter()
	called := ""
	static := func(*Ctx) error { called = "static"; return nil }
	param := func(*Ctx) error { called = "param"; return nil }
	r.Add("GET", "/files/:name/edit", param)
	r.Add("GET", "/files/new", static)

	var params []Param
	if got := r.FindBytes([]byte("GET"), []byte("/files/new"), &params); got == nil {
		t.Fatal("static route was not found")
	} else if _ = got(nil); called != "static" {
		t.Fatal("static route did not take precedence over parameter route")
	}
	params = params[:0]
	if got := r.FindBytes([]byte("GET"), []byte("/files/new/edit"), &params); got == nil {
		t.Fatal("parameter route was not found")
	} else if _ = got(nil); called != "param" {
		t.Fatal("router did not fall back to parameter route")
	}
	if len(params) != 1 || params[0].Value != "new" {
		t.Fatalf("unexpected params: %#v", params)
	}
}

func TestRouterKeepsEndpointParameterNames(t *testing.T) {
	r := newRouter()
	r.Add("GET", "/items/:first/one", func(*Ctx) error { return nil })
	r.Add("GET", "/items/:second/two", func(*Ctx) error { return nil })
	for path, key := range map[string]string{"/items/a/one": "first", "/items/b/two": "second"} {
		var params []Param
		if r.Find("GET", path, &params) == nil {
			t.Fatalf("route %s not found", path)
		}
		if len(params) != 1 || params[0].Key != key {
			t.Fatalf("route %s params %#v", path, params)
		}
	}
}

func TestParserRejectsAmbiguousContentLength(t *testing.T) {
	var h RequestHeader
	h.reset()
	src := []byte("Content-Length: 4\r\nContent-Length: 5\r\n\r\n")
	if _, err := parseHeaders(src, &h); !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("expected malformed request, got %v", err)
	}
}

func TestParserConnectionTokens(t *testing.T) {
	var h RequestHeader
	h.reset()
	h.Proto = strHTTP11
	h.KeepAlive = true
	if _, err := parseHeaders([]byte("Host: local\r\nConnection: upgrade\r\n\r\n"), &h); err != nil {
		t.Fatal(err)
	}
	if !h.KeepAlive {
		t.Fatal("HTTP/1.1 upgrade token unexpectedly disabled keep-alive")
	}

	h.reset()
	h.Proto = strHTTP11
	h.KeepAlive = true
	if _, err := parseHeaders([]byte("Host: local\r\nConnection: Upgrade, close\r\n\r\n"), &h); err != nil {
		t.Fatal(err)
	}
	if h.KeepAlive {
		t.Fatal("close token did not disable keep-alive")
	}
}

type shortWriteConn struct {
	data []byte
	max  int
}

func (c *shortWriteConn) Write(p []byte) (int, error) {
	if len(p) > c.max {
		p = p[:c.max]
	}
	c.data = append(c.data, p...)
	return len(p), nil
}
func (*shortWriteConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (*shortWriteConn) Close() error                     { return nil }
func (*shortWriteConn) LocalAddr() net.Addr              { return testAddr("local") }
func (*shortWriteConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (*shortWriteConn) SetDeadline(time.Time) error      { return nil }
func (*shortWriteConn) SetReadDeadline(time.Time) error  { return nil }
func (*shortWriteConn) SetWriteDeadline(time.Time) error { return nil }

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }

func TestWriteAllHandlesShortWrites(t *testing.T) {
	c := &shortWriteConn{max: 3}
	want := []byte("abcdefghij")
	if err := writeAll(c, want); err != nil {
		t.Fatal(err)
	}
	if string(c.data) != string(want) {
		t.Fatalf("got %q, want %q", c.data, want)
	}
}

// ── HTTP/1.1 Compliance (RFC 9112) ─────────────────────────────────────────

func TestParseRequestLineAbsoluteForm(t *testing.T) {
	tests := []struct {
		line string
		ok   bool
	}{
		{"GET http://host/path HTTP/1.1\r\n", true},
		{"GET http://host:8080/path?q=v HTTP/1.1\r\n", true},
		{"GET https://user@host/path HTTP/1.1\r\n", true},
		{"GET /origin-form HTTP/1.1\r\n", true},
		{"CONNECT server:443 HTTP/1.1\r\n", true},
		{"OPTIONS * HTTP/1.1\r\n", true},
	}
	for _, tt := range tests {
		var h RequestHeader
		h.reset()
		_, err := parseRequestLine([]byte(tt.line), &h, 8192)
		if tt.ok && err != nil {
			t.Errorf("expected OK for %q, got %v", tt.line, err)
		} else if !tt.ok && err == nil {
			t.Errorf("expected error for %q", tt.line)
		}
	}
}

func TestRequestLineTooLarge(t *testing.T) {
	var h RequestHeader
	h.reset()
	line := make([]byte, 8193+4)
	copy(line, "GET ")
	for i := 4; i < len(line)-2; i++ {
		line[i] = 'x'
	}
	line[len(line)-2] = '\r'
	line[len(line)-1] = '\n'
	_, err := parseRequestLine(line, &h, 8192)
	if !errors.Is(err, ErrRequestLineTooLarge) {
		t.Fatalf("expected ErrRequestLineTooLarge, got %v", err)
	}
}

func TestParseRequestLineUnknownVersion(t *testing.T) {
	var h RequestHeader
	h.reset()
	_, err := parseRequestLine([]byte("GET / HTTP/2.0\r\n"), &h, 8192)
	if err == nil {
		t.Fatal("expected error for HTTP/2.0")
	}
}

func TestParseHeadersRejectsLineFolding(t *testing.T) {
	var h RequestHeader
	h.reset()
	// Obsolete line folding: space after newline (RFC 9112 §5.5)
	src := []byte("Host: local\r\nContent-Type: text/plain\r\n X-Extra: value\r\n\r\n")
	if _, err := parseHeaders(src, &h); err == nil {
		t.Fatal("expected error for obsolete line folding")
	}
}

func TestHTTP11RequiresHost(t *testing.T) {
	var h RequestHeader
	h.reset()
	h.Proto = strHTTP11
	src := []byte("Content-Length: 0\r\n\r\n")
	if _, err := parseHeaders(src, &h); !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("expected ErrMalformedRequest for missing Host, got %v", err)
	}
}

func TestRejectsContentLengthAndChunked(t *testing.T) {
	var h RequestHeader
	h.reset()
	h.Proto = strHTTP11
	src := []byte("Host: local\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n")
	if _, err := parseHeaders(src, &h); !errors.Is(err, ErrMalformedRequest) {
		t.Fatalf("expected ErrMalformedRequest, got %v", err)
	}
}

func TestResponseBodyNotAllowedFor204(t *testing.T) {
	if responseBodyAllowed(204) {
		t.Fatal("204 should not allow body")
	}
	if responseBodyAllowed(304) {
		t.Fatal("304 should not allow body")
	}
	if responseBodyAllowed(100) {
		t.Fatal("1xx should not allow body")
	}
	if !responseBodyAllowed(200) {
		t.Fatal("200 should allow body")
	}
}

// ── HTTP/2 Unit Compliance (RFC 9113) ──────────────────────────────────────

func TestH2FrameSizeValidation(t *testing.T) {
	h := newTestH2Conn(t)
	// Frame payload exceeds peerMaxFrame (16384 default)
	f := h2Frame{
		typ:      h2Data,
		flags:    0,
		streamID: 1,
		payload:  make([]byte, 16385),
	}
	if err := h.handleFrame(f); err == nil {
		t.Fatal("expected frame size error")
	}
}

func TestH2DataOnStreamZero(t *testing.T) {
	h := newTestH2Conn(t)
	f := h2Frame{typ: h2Data, flags: 0, streamID: 0, payload: []byte("data")}
	if err := h.handleFrame(f); err == nil {
		t.Fatal("expected error for DATA on stream 0")
	}
}

func TestH2HeadersOnStreamZero(t *testing.T) {
	h := newTestH2Conn(t)
	f := h2Frame{typ: h2Headers, flags: h2FlagEndHeaders, streamID: 0}
	if err := h.handleFrame(f); err == nil {
		t.Fatal("expected error for HEADERS on stream 0")
	}
}

func TestH2PushPromiseRejected(t *testing.T) {
	h := newTestH2Conn(t)
	f := h2Frame{typ: h2PushPromise, flags: 0, streamID: 1, payload: make([]byte, 5)}
	if err := h.handleFrame(f); err == nil {
		t.Fatal("expected PUSH_PROMISE to be rejected")
	}
}

func TestH2ContinuationWithoutHeaders(t *testing.T) {
	h := newTestH2Conn(t)
	f := h2Frame{typ: h2Continuation, flags: h2FlagEndHeaders, streamID: 1}
	if err := h.handleFrame(f); err == nil {
		t.Fatal("expected standalone CONTINUATION to be rejected")
	}
}

func TestH2PrioritySelfDependency(t *testing.T) {
	h := newTestH2Conn(t)
	var p [5]byte
	binary.BigEndian.PutUint32(p[:4], 1) // dep = stream 1 (same as streamID)
	p[4] = 16                            // weight
	f := h2Frame{typ: h2Priority, flags: 0, streamID: 1, payload: p[:]}
	if err := h.handleFrame(f); err == nil {
		t.Fatal("expected self-dependency error")
	}
}

func TestH2SettingsACKTimeoutUsesCorrectError(t *testing.T) {
	err := h2ErrSettingsTimeout
	if err != 4 {
		t.Fatalf("h2ErrSettingsTimeout = %d, want 4", err)
	}
}

// ── H2C Upgrade Compliance ────────────────────────────────────────────────

func TestH2CUpgradeParsing(t *testing.T) {
	raw := "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	if !bytes.Equal([]byte(raw), h2ClientPreface) {
		t.Fatal("h2ClientPreface constant mismatch")
	}
}
