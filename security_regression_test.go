package fh

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/hpack"
)

func TestComplianceExposureWithoutAuthMountsNothing(t *testing.T) {
	app := NewWithConfig(Config{Compliance: ComplianceConfig{ExposeEndpoints: true}})
	for _, route := range app.Routes() {
		if strings.HasPrefix(route.Path, "/_fh/") {
			t.Fatalf("unauthenticated introspection route was mounted: %s %s", route.Method, route.Path)
		}
	}
}

type captureLogger struct {
	mu      sync.Mutex
	records []string
}

func (l *captureLogger) add(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, msg+" "+fmt.Sprint(args...))
}
func (l *captureLogger) Printf(format string, args ...any) { l.add(fmt.Sprintf(format, args...)) }
func (l *captureLogger) Info(msg string, args ...any)      { l.add(msg, args...) }
func (l *captureLogger) Warn(msg string, args ...any)      { l.add(msg, args...) }
func (l *captureLogger) Error(msg string, args ...any)     { l.add(msg, args...) }
func (l *captureLogger) Debug(msg string, args ...any)     { l.add(msg, args...) }
func (l *captureLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.records, "\n")
}

func TestProductionErrorLogsDoNotExposeInternalCause(t *testing.T) {
	logger := &captureLogger{}
	app := New(WithLogger(logger))
	app.Get("/", func(Ctx) error { return fmt.Errorf("database password=hunter2") })
	resp := pipeRequest(t, app, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if strings.Contains(resp, "hunter2") {
		t.Fatalf("response exposed internal cause: %s", resp)
	}
	logs := logger.String()
	if strings.Contains(logs, "hunter2") || strings.Contains(logs, "database password") {
		t.Fatalf("production logs exposed internal cause: %s", logs)
	}

	panicLogger := &captureLogger{}
	panicApp := New(WithLogger(panicLogger))
	panicApp.Get("/", func(Ctx) error { panic("token=panic-secret") })
	_ = pipeRequest(t, panicApp, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	panicLogs := panicLogger.String()
	secretExposed := strings.Contains(panicLogs, "panic-secret")
	serverErrors := strings.Count(panicLogs, "server error request_id")
	if secretExposed || serverErrors != 1 {
		t.Fatalf("panic logging was unsafe or duplicated (secret=%t logs=%d): %s", secretExposed, serverErrors, panicLogs)
	}
}

func TestParserRejectsSmugglingPrimitives(t *testing.T) {
	t.Run("bare LF header", func(t *testing.T) {
		var h RequestHeader
		h.Proto = strHTTP11
		if _, err := parseHeaders([]byte("Host: local\nX-Test: value\r\n\r\n"), &h); err == nil {
			t.Fatal("accepted LF-only header line")
		}
	})

	t.Run("stacked transfer coding", func(t *testing.T) {
		var h RequestHeader
		h.Proto = strHTTP11
		if _, err := parseHeaders([]byte("Host: local\r\nTransfer-Encoding: gzip, chunked\r\n\r\n"), &h); err != nil {
			t.Fatal(err)
		}
		if !h.UnsupportedTransferEncoding || h.Chunked {
			t.Fatalf("stacked transfer coding was accepted: %#v", h)
		}
	})

	t.Run("absolute authority mismatch", func(t *testing.T) {
		var h RequestHeader
		consumed, err := parseRequestLine([]byte("GET http://trusted.example/path HTTP/1.1\r\n"), &h, 8192)
		if err != nil || consumed == 0 {
			t.Fatal(err)
		}
		if _, err := parseHeaders([]byte("Host: attacker.example\r\n\r\n"), &h); err == nil {
			t.Fatal("accepted mismatched absolute authority and Host")
		}
	})
}

func TestHTTP1HeaderListLimitCanGrowAndReject(t *testing.T) {
	makeRequest := func(value string) string {
		return "GET / HTTP/1.1\r\nHost: local\r\nX-Large: " + value + "\r\nConnection: close\r\n\r\n"
	}
	app := New(WithReadBufferSize(512), WithMaxHeaderListSize(4096))
	app.Get("/", func(c Ctx) error { return c.SendString("ok") })
	if resp := pipeRequest(t, app, makeRequest(strings.Repeat("a", 2048))); !strings.Contains(resp, "200 OK") {
		t.Fatalf("valid header within configured limit failed: %s", resp)
	}

	limited := New(WithReadBufferSize(512), WithMaxHeaderListSize(1024))
	limited.Get("/", func(c Ctx) error { return c.SendString("unexpected") })
	if resp := pipeRequest(t, limited, makeRequest(strings.Repeat("a", 1500))); !strings.Contains(resp, "431 Request Header Fields Too Large") {
		t.Fatalf("oversized header was not rejected: %s", resp)
	}
}

func TestDisableH2CRejectsCleartextPreface(t *testing.T) {
	t.Run("prior knowledge", func(t *testing.T) {
		app := New(WithDisableH2C(true))
		client := runPipeApp(t, app)
		go func() { _, _ = client.Write(h2ClientPreface) }()
		resp, err := io.ReadAll(client)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(resp), "101 Switching Protocols") {
			t.Fatalf("h2c was accepted while disabled: %q", resp)
		}
	})

	t.Run("upgrade", func(t *testing.T) {
		app := New(WithDisableH2C(true))
		resp := pipeRequest(t, app, "GET / HTTP/1.1\r\nHost: local\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABk\r\n\r\n")
		if !strings.Contains(resp, "400 Bad Request") || strings.Contains(resp, "101 Switching Protocols") {
			t.Fatalf("h2c upgrade was not rejected: %q", resp)
		}
	})
}

func TestH2AbsoluteBodyTimeoutResetsStream(t *testing.T) {
	h := newTestH2Conn(t)
	h.app.cfg.RequestBodyTimeout = 15 * time.Millisecond
	block := encodeHeaderBlock(t,
		hpack.HeaderField{Name: ":method", Value: "POST"},
		hpack.HeaderField{Name: ":scheme", Value: "https"},
		hpack.HeaderField{Name: ":authority", Value: "example.test"},
		hpack.HeaderField{Name: ":path", Value: "/upload"},
	)
	if err := h.handleHeaders(h2Frame{typ: h2Headers, flags: h2FlagEndHeaders, streamID: 1, payload: block}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		_, exists := h.streams[1]
		h.mu.Unlock()
		if !exists {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("HTTP/2 stream survived its absolute request-body timeout")
}

func TestRequestHeadHandlerRejectsBeforeBody(t *testing.T) {
	called := false
	app := New(WithRequestHeadHandler(func(c Ctx) error {
		called = true
		if c.Param("id") != "42" {
			return fmt.Errorf("route parameters unavailable")
		}
		return c.Status(StatusUnauthorized).SendString("denied")
	}))
	app.Post("/upload/:id", func(c Ctx) error { return c.SendString("unexpected") })
	client := runPipeApp(t, app)
	if _, err := io.WriteString(client, "POST /upload/42 HTTP/1.1\r\nHost: local\r\nContent-Length: 1048576\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !called || !strings.Contains(string(resp), "401 Unauthorized") || strings.Contains(string(resp), "100 Continue") {
		t.Fatalf("request was not rejected before body read: %q", resp)
	}
}

func TestH2RequestHeadHandlerRejectsBeforeData(t *testing.T) {
	h := newTestH2Conn(t)
	h.app.cfg.RequestBodyTimeout = time.Second
	h.app.cfg.ErrorHandler = defaultErrorHandler
	h.app.cfg.RequestHeadHandler = func(c Ctx) error {
		return c.Status(StatusUnauthorized).SendString("denied")
	}
	block := encodeHeaderBlock(t,
		hpack.HeaderField{Name: ":method", Value: "POST"},
		hpack.HeaderField{Name: ":scheme", Value: "https"},
		hpack.HeaderField{Name: ":authority", Value: "example.test"},
		hpack.HeaderField{Name: ":path", Value: "/upload"},
	)
	if err := h.handleHeaders(h2Frame{typ: h2Headers, flags: h2FlagEndHeaders, streamID: 1, payload: block}); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	_, exists := h.streams[1]
	h.mu.Unlock()
	if exists {
		t.Fatal("rejected HTTP/2 stream remained open for DATA")
	}
}

func TestRequestBodyTimeoutReturns408(t *testing.T) {
	app := New(WithRequestBodyTimeout(20 * time.Millisecond))
	app.Post("/upload", func(c Ctx) error { return c.SendString("unexpected") })
	client := runPipeApp(t, app)
	if _, err := io.WriteString(client, "POST /upload HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(resp), "408 Request Timeout") {
		t.Fatalf("body timeout did not return 408: %q", resp)
	}
}

func TestWriteAndHandlerTimeoutsAreIndependent(t *testing.T) {
	var hadDeadline bool
	app := New(WithWriteTimeout(time.Second))
	app.Get("/", func(c Ctx) error {
		_, hadDeadline = c.Deadline()
		return c.SendString("ok")
	})
	_ = pipeRequest(t, app, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if hadDeadline {
		t.Fatal("WriteTimeout unexpectedly became a handler context deadline")
	}

	hadDeadline = false
	bounded := New(WithHandlerTimeout(time.Second))
	bounded.Get("/", func(c Ctx) error {
		_, hadDeadline = c.Deadline()
		return c.SendString("ok")
	})
	_ = pipeRequest(t, bounded, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !hadDeadline {
		t.Fatal("HandlerTimeout did not set a handler context deadline")
	}
}
