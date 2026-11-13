package fh

import (
	"bytes"
	"math"
	"testing"
	"time"
)

func BenchmarkResourceGuardSample(b *testing.B) {
	app := New(WithStartupBannerDisabled(true))
	guard := defaultResourceGuardMiddleware(Config{
		MaxGoroutines:         math.MaxInt,
		MaxHeapBytes:          math.MaxUint64,
		MaxInFlightRequests:   math.MaxInt64,
		ResourceCheckInterval: 250 * time.Millisecond,
	})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		conn := &benchConn{buf: make([]byte, 256)}
		ctx := acquireCtx(conn, app)
		defer releaseCtx(ctx)
		for pb.Next() {
			ctx.reset()
			_ = guard(ctx)
		}
	})
}

func BenchmarkAllowedHost(b *testing.B) {
	allowed := normalizeAllowedHosts([]string{"api.example.com", "admin.example.com", "[2001:db8::1]"})
	for _, host := range []string{"api.example.com", "api.example.com:8443", "API.EXAMPLE.COM:8443"} {
		b.Run(host, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if !allowedHost(host, allowed) {
					b.Fatal("allowed host rejected")
				}
			}
		})
	}
}

func BenchmarkFullRequestPlaintextAllowedHost(b *testing.B) {
	app := New(WithStartupBannerDisabled(true), WithAllowedHosts("localhost"))
	app.Get("/plaintext", func(c Ctx) error {
		return c.SendBytes([]byte("Hello, World!"))
	})
	app.router.Freeze()

	conn := &benchConn{buf: make([]byte, 4096)}
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)
	req := []byte("GET /plaintext HTTP/1.1\r\nHost: localhost:8080\r\n\r\n")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		consumed, err := parseRequestLine(req, &ctx.Header, 8192)
		if err != nil {
			b.Fatal(err)
		}
		headEnd := bytes.Index(req[consumed:], []byte("\r\n\r\n"))
		if headEnd < 0 {
			b.Fatal("request headers incomplete")
		}
		if _, err := parseHeadersLimit(req[consumed:consumed+headEnd+4], &ctx.Header, 64); err != nil {
			b.Fatal(err)
		}
		if !allowedHost(b2s(ctx.Header.Host), app.cfg.AllowedHosts) {
			b.Fatal("configured host rejected")
		}
		handler := app.router.FindBytes([]byte("GET"), []byte("/plaintext"), &ctx.params)
		if handler != nil {
			_ = handler(ctx)
		}
	}
}
