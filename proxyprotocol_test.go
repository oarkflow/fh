package fh

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestProxyProtocolV1(t *testing.T) {
	app := New()
	app.Get("/ip", func(c Ctx) error {
		return c.SendString(c.IP())
	})

	client, server := net.Pipe()
	ln := newPipeListener(server)
	proxyLn := NewProxyProtocolListener(ln)

	go func() {
		_ = app.Serve(proxyLn)
	}()
	defer app.ShutdownWithTimeout(time.Second)

	go func() {
		// Send PROXY v1 header followed by HTTP request
		_, _ = client.Write([]byte("PROXY TCP4 203.0.113.195 198.51.100.1 56324 443\r\n"))
		req := "GET /ip HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
		_, _ = client.Write([]byte(req))
	}()

	respBytes, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if !stringsContains(string(respBytes), "203.0.113.195") {
		t.Errorf("expected response to contain client IP '203.0.113.195', got %q", string(respBytes))
	}
}

func TestProxyProtocolV2(t *testing.T) {
	app := New()
	app.Get("/ip", func(c Ctx) error {
		return c.SendString(c.IP())
	})

	client, server := net.Pipe()
	ln := newPipeListener(server)
	proxyLn := NewProxyProtocolListener(ln)

	go func() {
		_ = app.Serve(proxyLn)
	}()
	defer app.ShutdownWithTimeout(time.Second)

	go func() {
		// Build PROXY v2 header
		var buf bytes.Buffer
		buf.Write(proxyV2Signature)                          // 12 bytes signature
		buf.WriteByte(0x21)                                  // ver=2, cmd=PROXY
		buf.WriteByte(0x11)                                  // family=AF_INET, proto=STREAM (TCP4)
		_ = binary.Write(&buf, binary.BigEndian, uint16(12)) // length=12

		// IPv4 address payload (4 src, 4 dst, 2 src port, 2 dst port)
		buf.Write([]byte{198, 51, 100, 42}) // 198.51.100.42
		buf.Write([]byte{10, 0, 0, 1})      // 10.0.0.1
		_ = binary.Write(&buf, binary.BigEndian, uint16(43210))
		_ = binary.Write(&buf, binary.BigEndian, uint16(80))

		req := "GET /ip HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
		buf.WriteString(req)

		_, _ = client.Write(buf.Bytes())
	}()

	respBytes, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if !stringsContains(string(respBytes), "198.51.100.42") {
		t.Errorf("expected response to contain client IP '198.51.100.42', got %q", string(respBytes))
	}
}

func stringsContains(s, substr string) bool {
	return bytes.Contains([]byte(s), []byte(substr))
}
