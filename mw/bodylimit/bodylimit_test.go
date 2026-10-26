package bodylimit

import (
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestStreamingLimitRejectsChunkedBody(t *testing.T) {
	app := fh.New(fh.WithStreamRequestBody(true))
	app.Use(New(3))
	app.Post("/", func(c fh.Ctx) error {
		if err := c.StreamBody(func(r io.Reader) error { _, err := io.Copy(io.Discard, r); return err }); err != nil {
			return err
		}
		return c.SendString("ok")
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Serve(ln) }()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })
	for _, tc := range []struct {
		body   string
		status int
	}{{"abc", 200}, {"abcd", 413}} {
		req, err := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/", strings.NewReader(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		req.ContentLength = -1
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != tc.status {
			t.Fatalf("body %q: status=%d want=%d", tc.body, resp.StatusCode, tc.status)
		}
	}
}

func TestNewRejectsZeroLimit(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for zero limit")
		}
	}()
	New(0)
}

func TestNewRejectsNegativeLimit(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for negative limit")
		}
	}()
	New(-1)
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, "0 byte"},
		{1, "1 byte"},
		{1023, "1023 byte"},
		{1024, "1 KiB"},
		{2048, "2 KiB"},
		{1048576, "1 MiB"},
	}
	for _, tt := range tests {
		got := formatBytes(tt.n)
		if got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestWithConfigRejectsZeroLimit(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for zero limit in WithConfig")
		}
	}()
	WithConfig(Config{Limit: 0})
}

func TestNewDoesNotPanic(t *testing.T) {
	_ = New(1024)
}

func TestWithConfigDoesNotPanic(t *testing.T) {
	_ = WithConfig(Config{Limit: 1024})
}

func TestBodyLimitSkippedWhenNextReturnsTrue(t *testing.T) {
	handler := WithConfig(Config{
		Limit: 5,
		Next:  func(_ fh.Ctx) bool { return true },
	})
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestItoa(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, "0"},
		{1, "1"},
		{123, "123"},
		{9999, "9999"},
	}
	for _, tt := range tests {
		got := itoa(tt.n)
		if got != tt.want {
			t.Errorf("itoa(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}
