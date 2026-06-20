package fh

import (
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
