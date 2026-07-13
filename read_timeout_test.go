package fh

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestReadHeaderTimeoutIsAbsoluteAfterFirstByte(t *testing.T) {
	app := NewProduction(
		WithReadHeaderTimeout(40*time.Millisecond),
		WithReadTimeout(time.Second),
		WithIdleTimeout(time.Second),
	)
	app.Get("/", func(c Ctx) error { return c.SendString("ok") })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Serve(ln) }()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = io.WriteString(conn, "GET / HTTP/1.1\r\nH"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	response, readErr := io.ReadAll(conn)
	if netErr, ok := readErr.(net.Error); ok && netErr.Timeout() {
		t.Fatal("connection remained open beyond ReadHeaderTimeout")
	}
	if len(response) > 0 && !strings.HasPrefix(string(response), "HTTP/1.1 408 ") {
		t.Fatalf("response=%q", response)
	}
}
