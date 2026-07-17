package fh

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── Router benchmarks ──────────────────────────────────────────────────────

func BenchmarkRouterStatic(b *testing.B) {
	app := New()
	app.Get("/plaintext", func(c Ctx) error {
		return c.SendBytes([]byte("Hello, World!"))
	})
	app.router.Freeze()

	conn := &benchConn{buf: make([]byte, 4096)}
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	reqLine := []byte("GET /plaintext HTTP/1.1\r\nHost: localhost\r\n\r\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		_, _ = parseRequestLine(reqLine, &ctx.Header, 8192)
		handler := app.router.FindBytes([]byte("GET"), []byte("/plaintext"), &ctx.params)
		if handler != nil {
			_ = handler(ctx)
		}
	}
}

func BenchmarkRouterParam(b *testing.B) {
	app := New()
	app.Get("/users/:id", func(c Ctx) error {
		return c.SendString(c.Param("id"))
	})
	app.router.Freeze()

	conn := &benchConn{buf: make([]byte, 4096)}
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	reqLine := []byte("GET /users/42 HTTP/1.1\r\nHost: localhost\r\n\r\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		_, _ = parseRequestLine(reqLine, &ctx.Header, 8192)
		handler := app.router.FindBytes([]byte("GET"), []byte("/users/42"), &ctx.params)
		if handler != nil {
			_ = handler(ctx)
		}
	}
}

func BenchmarkRouterWildcard(b *testing.B) {
	app := New()
	app.Get("/files/*path", func(c Ctx) error {
		return c.SendString(c.Param("path"))
	})
	app.router.Freeze()

	conn := &benchConn{buf: make([]byte, 4096)}
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	reqLine := []byte("GET /files/css/style.css HTTP/1.1\r\nHost: localhost\r\n\r\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		_, _ = parseRequestLine(reqLine, &ctx.Header, 8192)
		handler := app.router.FindBytes([]byte("GET"), []byte("/files/css/style.css"), &ctx.params)
		if handler != nil {
			_ = handler(ctx)
		}
	}
}

func BenchmarkRouterLookupHighCardinality(b *testing.B) {
	for _, routeCount := range []int{16, 256, 4096} {
		b.Run(fmt.Sprintf("Static/%d", routeCount), func(b *testing.B) {
			r := NewRouter()
			paths := make([][]byte, routeCount)
			for i := range paths {
				path := fmt.Sprintf("/static/%d", i)
				paths[i] = []byte(path)
				r.Add("GET", path, func(Ctx) error { return nil })
			}
			r.Freeze()
			method := []byte("GET")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if r.FindBytes(method, paths[i%routeCount], nil) == nil {
					b.Fatal("route not found")
				}
			}
		})

		b.Run(fmt.Sprintf("Param/%d", routeCount), func(b *testing.B) {
			r := NewRouter()
			paths := make([][]byte, routeCount)
			for i := range paths {
				pattern := fmt.Sprintf("/resource-%d/:id", i)
				paths[i] = []byte(fmt.Sprintf("/resource-%d/42", i))
				r.Add("GET", pattern, func(Ctx) error { return nil })
			}
			r.UnsafeParams = true
			r.Freeze()
			method := []byte("GET")
			params := make([]Param, 0, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if r.FindBytes(method, paths[i%routeCount], &params) == nil {
					b.Fatal("route not found")
				}
			}
		})
	}
}

// ── Header parsing benchmarks ──────────────────────────────────────────────

func BenchmarkHeaderPeek(b *testing.B) {
	var h RequestHeader
	h.Init()
	reqLine := []byte("GET / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nAuthorization: Bearer token123\r\nX-Custom-Header: custom-value\r\n\r\n")
	_, _ = parseRequestLine(reqLine, &h, 8192)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.Peek([]byte("Content-Type"))
		_ = h.Peek([]byte("Authorization"))
		_ = h.Peek([]byte("X-Custom-Header"))
	}
}

// ── Context benchmarks ─────────────────────────────────────────────────────

func BenchmarkCtxAcquireRelease(b *testing.B) {
	conn := &benchConn{buf: make([]byte, 4096)}
	app := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := acquireCtx(conn, app)
		releaseCtx(ctx)
	}
}

func BenchmarkCtxJSON(b *testing.B) {
	conn := &benchConn{buf: make([]byte, 4096)}
	app := New()
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	data := map[string]any{
		"id":    42,
		"name":  "John Doe",
		"email": "john@example.com",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		_ = ctx.JSON(data)
	}
}

func BenchmarkCtxSendString(b *testing.B) {
	conn := &benchConn{buf: make([]byte, 4096)}
	app := New()
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		_ = ctx.SendString("Hello, World!")
	}
}

func BenchmarkCtxSendBytes(b *testing.B) {
	conn := &benchConn{buf: make([]byte, 4096)}
	app := New()
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	data := []byte("Hello, World!")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		_ = ctx.SendBytes(data)
	}
}

// ── Pool benchmarks ────────────────────────────────────────────────────────

func BenchmarkBufPool512(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := getBuf(512)
		putBuf(buf)
	}
}

func BenchmarkBufPool4K(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := getBuf(4096)
		putBuf(buf)
	}
}

func BenchmarkBufPool16K(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := getBuf(16384)
		putBuf(buf)
	}
}

func BenchmarkBufPool64K(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := getBuf(65536)
		putBuf(buf)
	}
}

// ── Compression benchmarks ─────────────────────────────────────────────────

func BenchmarkGzipCompression(b *testing.B) {
	data := bytes.Repeat([]byte("Hello, World! This is a test payload for compression benchmarking. "), 100)
	var buf bytes.Buffer
	buf.Grow(len(data))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
		_, _ = w.Write(data)
		_ = w.Close()
	}
	b.SetBytes(int64(len(data)))
}

func BenchmarkGzipDecompression(b *testing.B) {
	data := bytes.Repeat([]byte("Hello, World! This is a test payload for compression benchmarking. "), 100)
	var buf bytes.Buffer
	w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	_, _ = w.Write(data)
	_ = w.Close()
	compressed := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := gzip.NewReader(bytes.NewReader(compressed))
		_, _ = io.Copy(io.Discard, r)
		_ = r.Close()
	}
	b.SetBytes(int64(len(data)))
}

// ── HPACK benchmarks ──────────────────────────────────────────────────────

func BenchmarkHPACKEncode(b *testing.B) {
	var buf bytes.Buffer
	enc := NewHPACKEncoder(&buf)
	fields := []HPACKHeaderField{
		{Name: ":status", Value: "200"},
		{Name: "content-type", Value: "application/json"},
		{Name: "content-length", Value: "42"},
		{Name: "date", Value: "Mon, 02 Jan 2006 15:04:05 GMT"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		for _, f := range fields {
			_ = enc.WriteField(f)
		}
	}
}

func BenchmarkHPACKDecode(b *testing.B) {
	var buf bytes.Buffer
	enc := NewHPACKEncoder(&buf)
	fields := []HPACKHeaderField{
		{Name: ":status", Value: "200"},
		{Name: "content-type", Value: "application/json"},
		{Name: "content-length", Value: "42"},
		{Name: "date", Value: "Mon, 02 Jan 2006 15:04:05 GMT"},
	}
	for _, f := range fields {
		_ = enc.WriteField(f)
	}
	encoded := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dec := NewHPACKDecoder(4096, func(HPACKHeaderField) {})
		_, _ = dec.Write(encoded)
	}
}

// ── String utility benchmarks ──────────────────────────────────────────────

func BenchmarkB2S(b *testing.B) {
	data := []byte("Hello, World!")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = b2s(data)
	}
}

func BenchmarkS2B(b *testing.B) {
	data := "Hello, World!"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s2b(data)
	}
}

// ── Date cache benchmarks ──────────────────────────────────────────────────

func BenchmarkCachedDate(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cachedDate()
	}
}

func BenchmarkCachedDateValue(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cachedDateValue()
	}
}

// ── Content-Length benchmarks ──────────────────────────────────────────────

func BenchmarkAppendContentLengthSmall(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = appendContentLengthLine(nil, i%1024)
	}
}

func BenchmarkAppendContentLengthLarge(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = appendContentLengthLine(nil, 1000000)
	}
}

// ── Full request-response benchmarks ──────────────────────────────────────

func BenchmarkFullRequestJSON(b *testing.B) {
	app := New()
	app.Get("/json", func(c Ctx) error {
		return c.JSON(map[string]string{"message": "Hello, World!"})
	})
	app.router.Freeze()

	conn := &benchConn{buf: make([]byte, 65536)}
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	req := []byte("GET /json HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n\r\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		consumed, _ := parseRequestLine(req, &ctx.Header, 8192)
		_, _ = parseHeadersLimit(req[consumed:], &ctx.Header, 64)
		handler := app.router.FindBytes([]byte("GET"), []byte("/json"), &ctx.params)
		if handler != nil {
			_ = handler(ctx)
		}
	}
	b.SetBytes(int64(len(req)))
}

func BenchmarkFullRequestPlaintext(b *testing.B) {
	app := New()
	app.Get("/plaintext", func(c Ctx) error {
		return c.SendBytes([]byte("Hello, World!"))
	})
	app.router.Freeze()

	conn := &benchConn{buf: make([]byte, 65536)}
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	req := []byte("GET /plaintext HTTP/1.1\r\nHost: localhost\r\n\r\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		consumed, _ := parseRequestLine(req, &ctx.Header, 8192)
		_, _ = parseHeadersLimit(req[consumed:], &ctx.Header, 64)
		handler := app.router.FindBytes([]byte("GET"), []byte("/plaintext"), &ctx.params)
		if handler != nil {
			_ = handler(ctx)
		}
	}
	b.SetBytes(int64(len(req)))
}

func BenchmarkFullRequestEcho(b *testing.B) {
	app := New()
	app.Post("/echo", func(c Ctx) error {
		return c.SendBytes(c.Body())
	})
	app.router.Freeze()

	conn := &benchConn{buf: make([]byte, 65536)}
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	body := []byte(`{"message":"Hello, World!"}`)
	req := append([]byte("POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: "), []byte(fmt.Sprintf("%d", len(body)))...)
	req = append(req, []byte("\r\n\r\n")...)
	req = append(req, body...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		consumed, _ := parseRequestLine(req, &ctx.Header, 8192)
		_, _ = parseHeadersLimit(req[consumed:bytes.Index(req, []byte("\r\n\r\n"))+4], &ctx.Header, 64)
		ctx.body = body
		handler := app.router.FindBytes([]byte("POST"), []byte("/echo"), &ctx.params)
		if handler != nil {
			_ = handler(ctx)
		}
	}
	b.SetBytes(int64(len(req)))
}

// ── Concurrent benchmarks ──────────────────────────────────────────────────

func BenchmarkConcurrentRouting(b *testing.B) {
	app := New()
	app.Get("/users/:id", func(c Ctx) error {
		return c.SendString(c.Param("id"))
	})
	app.Get("/posts/:id", func(c Ctx) error {
		return c.SendString(c.Param("id"))
	})
	app.Get("/comments/:id", func(c Ctx) error {
		return c.SendString(c.Param("id"))
	})
	app.router.Freeze()

	paths := []string{"/users/42", "/posts/100", "/comments/999"}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn := &benchConn{buf: make([]byte, 4096)}
		ctx := acquireCtx(conn, app)
		defer releaseCtx(ctx)
		i := 0
		for pb.Next() {
			path := paths[i%3]
			i++
			ctx.reset()
			conn.Reset()
			req := []byte("GET " + path + " HTTP/1.1\r\nHost: localhost\r\n\r\n")
			_, _ = parseRequestLine(req, &ctx.Header, 8192)
			handler := app.router.FindBytes([]byte("GET"), []byte(path), &ctx.params)
			if handler != nil {
				_ = handler(ctx)
			}
		}
	})
}

func BenchmarkConcurrentJSON(b *testing.B) {
	app := New()
	app.Get("/json", func(c Ctx) error {
		return c.JSON(map[string]string{"message": "Hello, World!"})
	})
	app.router.Freeze()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn := &benchConn{buf: make([]byte, 4096)}
		ctx := acquireCtx(conn, app)
		defer releaseCtx(ctx)
		for pb.Next() {
			ctx.reset()
			conn.Reset()
			req := []byte("GET /json HTTP/1.1\r\nHost: localhost\r\n\r\n")
			_, _ = parseRequestLine(req, &ctx.Header, 8192)
			handler := app.router.FindBytes([]byte("GET"), []byte("/json"), &ctx.params)
			if handler != nil {
				_ = handler(ctx)
			}
		}
	})
}

// ── Middleware chain benchmarks ─────────────────────────────────────────────

func BenchmarkMiddlewareChainNoMiddleware(b *testing.B) {
	app := New()
	app.Get("/test", func(c Ctx) error {
		return c.SendString("ok")
	})
	app.router.Freeze()

	conn := &benchConn{buf: make([]byte, 4096)}
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	req := []byte("GET /test HTTP/1.1\r\nHost: localhost\r\n\r\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		consumed, _ := parseRequestLine(req, &ctx.Header, 8192)
		_, _ = parseHeadersLimit(req[consumed:], &ctx.Header, 64)
		handler := app.router.FindBytes([]byte("GET"), []byte("/test"), &ctx.params)
		if handler != nil {
			_ = handler(ctx)
		}
	}
}

func BenchmarkMiddlewareChain3Middleware(b *testing.B) {
	app := New()
	app.Use(func(c Ctx) error { return c.Next() })
	app.Use(func(c Ctx) error { return c.Next() })
	app.Use(func(c Ctx) error { return c.Next() })
	app.Get("/test", func(c Ctx) error {
		return c.SendString("ok")
	})
	app.router.Freeze()

	conn := &benchConn{buf: make([]byte, 4096)}
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	req := []byte("GET /test HTTP/1.1\r\nHost: localhost\r\n\r\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		consumed, _ := parseRequestLine(req, &ctx.Header, 8192)
		_, _ = parseHeadersLimit(req[consumed:], &ctx.Header, 64)
		handler := app.router.FindBytes([]byte("GET"), []byte("/test"), &ctx.params)
		if handler != nil {
			_ = handler(ctx)
		}
	}
}

// ── Memory allocation benchmarks ───────────────────────────────────────────

func BenchmarkAllocRouterLookup(b *testing.B) {
	app := New()
	for i := 0; i < 100; i++ {
		path := fmt.Sprintf("/route%d/:id", i)
		app.Get(path, func(c Ctx) error { return nil })
	}
	app.router.Freeze()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("/route%d/42", i%100)
		params := make([]Param, 0, 8)
		_ = app.router.FindBytes([]byte("GET"), []byte(path), &params)
	}
}

func BenchmarkAllocJSONMarshal(b *testing.B) {
	data := map[string]any{
		"id":    42,
		"name":  "John Doe",
		"email": "john@example.com",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(data)
	}
}

// ── WriteAll benchmark ─────────────────────────────────────────────────────

func BenchmarkWriteAll(b *testing.B) {
	data := []byte("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\n\r\nHello, World!")
	conn := &benchConn{buf: make([]byte, 65536)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Reset()
		_ = writeAll(conn, data)
	}
}

// ── Prebuilt response benchmark ────────────────────────────────────────────

func BenchmarkPrebuiltResponse(b *testing.B) {
	app := New()
	app.Get("/hello", func(c Ctx) error {
		return c.SendString("Hello, World!")
	})
	app.router.Freeze()

	conn := &benchConn{buf: make([]byte, 4096)}
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)

	req := []byte("GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.reset()
		conn.Reset()
		consumed, _ := parseRequestLine(req, &ctx.Header, 8192)
		_, _ = parseHeadersLimit(req[consumed:], &ctx.Header, 64)
		handler := app.router.FindBytes([]byte("GET"), []byte("/hello"), &ctx.params)
		if handler != nil {
			_ = handler(ctx)
		}
	}
}

// ── Test helpers ───────────────────────────────────────────────────────────

type benchConn struct {
	net.Conn
	buf    []byte
	pos    int
	closed bool
}

func (c *benchConn) Read(p []byte) (int, error) {
	n := copy(p, c.buf[c.pos:])
	c.pos += n
	return n, nil
}

func (c *benchConn) Write(p []byte) (int, error) {
	n := copy(c.buf[c.pos:], p)
	c.pos += n
	return n, nil
}

func (c *benchConn) Close() error {
	c.closed = true
	return nil
}

func (c *benchConn) Reset() {
	c.pos = 0
	c.closed = false
}

func (c *benchConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *benchConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (c *benchConn) SetDeadline(_ time.Time) error      { return nil }
func (c *benchConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *benchConn) SetWriteDeadline(_ time.Time) error { return nil }

// ── HPACK types for benchmarks ─────────────────────────────────────────────

type HPACKHeaderField struct {
	Name  string
	Value string
}

type hpackEncoder struct {
	buf *bytes.Buffer
}

func NewHPACKEncoder(buf *bytes.Buffer) *hpackEncoder {
	return &hpackEncoder{buf: buf}
}

func (e *hpackEncoder) WriteField(f HPACKHeaderField) error {
	e.buf.WriteString(f.Name)
	e.buf.WriteString(": ")
	e.buf.WriteString(f.Value)
	e.buf.WriteString("\r\n")
	return nil
}

type hpackDecoder struct {
	buf []byte
	fn  func(HPACKHeaderField)
}

func NewHPACKDecoder(size int, fn func(HPACKHeaderField)) *hpackDecoder {
	return &hpackDecoder{fn: fn}
}

func (d *hpackDecoder) Write(p []byte) (int, error) {
	d.buf = append(d.buf, p...)
	return len(p), nil
}

// ── Comprehensive middleware benchmark suite ────────────────────────────────

func BenchmarkMiddlewareBenchmarks(b *testing.B) {
	benchmarks := []struct {
		name string
		fn   func(b *testing.B)
	}{
		{"Router/Static", BenchmarkRouterStatic},
		{"Router/Param", BenchmarkRouterParam},
		{"Router/Wildcard", BenchmarkRouterWildcard},
		{"Parse/HeaderPeek", BenchmarkHeaderPeek},
		{"Ctx/AcquireRelease", BenchmarkCtxAcquireRelease},
		{"Ctx/JSON", BenchmarkCtxJSON},
		{"Ctx/SendString", BenchmarkCtxSendString},
		{"Ctx/SendBytes", BenchmarkCtxSendBytes},
		{"Pool/512B", BenchmarkBufPool512},
		{"Pool/4K", BenchmarkBufPool4K},
		{"Pool/16K", BenchmarkBufPool16K},
		{"Pool/64K", BenchmarkBufPool64K},
		{"Gzip/Compress", BenchmarkGzipCompression},
		{"Gzip/Decompress", BenchmarkGzipDecompression},
		{"HPACK/Encode", BenchmarkHPACKEncode},
		{"HPACK/Decode", BenchmarkHPACKDecode},
		{"String/B2S", BenchmarkB2S},
		{"String/S2B", BenchmarkS2B},
		{"Date/Cached", BenchmarkCachedDate},
		{"Date/CachedValue", BenchmarkCachedDateValue},
		{"ContentLength/Small", BenchmarkAppendContentLengthSmall},
		{"ContentLength/Large", BenchmarkAppendContentLengthLarge},
		{"Full/JSON", BenchmarkFullRequestJSON},
		{"Full/Plaintext", BenchmarkFullRequestPlaintext},
		{"Full/Echo", BenchmarkFullRequestEcho},
		{"Concurrent/Routing", BenchmarkConcurrentRouting},
		{"Concurrent/JSON", BenchmarkConcurrentJSON},
		{"Middleware/NoMiddleware", BenchmarkMiddlewareChainNoMiddleware},
		{"Middleware/3Middleware", BenchmarkMiddlewareChain3Middleware},
		{"Alloc/RouterLookup", BenchmarkAllocRouterLookup},
		{"Alloc/JSONMarshal", BenchmarkAllocJSONMarshal},
		{"WriteAll", BenchmarkWriteAll},
		{"Prebuilt/Response", BenchmarkPrebuiltResponse},
		{"Unsafe/Conversion", BenchmarkUnsafeConversion},
		{"Find/HeaderEnd", BenchmarkFindHeaderEnd},
		{"Route/Registration", BenchmarkRouteRegistration},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, bm.fn)
	}
}

// ── Additional benchmarks ──────────────────────────────────────────────────

func BenchmarkUnsafeConversion(b *testing.B) {
	data := []byte("Hello, World! This is a longer string for benchmarking unsafe conversions.")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s := b2s(data)
		_ = s
	}
}

func BenchmarkFindHeaderEnd(b *testing.B) {
	data := []byte("GET / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\r\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = findHeaderEnd(data)
	}
}

func BenchmarkRouteRegistration(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		app := New()
		for j := 0; j < 100; j++ {
			path := fmt.Sprintf("/route%d/:id", j)
			app.Get(path, func(c Ctx) error { return nil })
		}
	}
}

// ── String builder benchmark ───────────────────────────────────────────────

func BenchmarkStringBuilder(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sb strings.Builder
		sb.Grow(256)
		for j := 0; j < 10; j++ {
			sb.WriteString("Hello, World! ")
		}
		_ = sb.String()
	}
}

// ── Sync.Pool benchmark ────────────────────────────────────────────────────

func BenchmarkSyncPool(b *testing.B) {
	var pool = sync.Pool{
		New: func() any {
			buf := make([]byte, 4096)
			return &buf
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get().(*[]byte)
			_ = (*buf)[:0]
			pool.Put(buf)
		}
	})
}

// ── Atomic operations benchmark ─────────────────────────────────────────────

func BenchmarkAtomicInt64(b *testing.B) {
	var v atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v.Add(1)
			v.Load()
		}
	})
}

func BenchmarkAtomicBool(b *testing.B) {
	var v atomic.Bool
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v.Store(true)
			v.Load()
		}
	})
}

// ── Map benchmarks ─────────────────────────────────────────────────────────

func BenchmarkMapRead(b *testing.B) {
	m := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer token",
		"Accept":        "application/json",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m["Content-Type"]
		_ = m["Authorization"]
		_ = m["Accept"]
	}
}

func BenchmarkMapReadConcurrent(b *testing.B) {
	m := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer token",
		"Accept":        "application/json",
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = m["Content-Type"]
			_ = m["Authorization"]
			_ = m["Accept"]
		}
	})
}

// ── Byte slice comparison benchmarks ───────────────────────────────────────

func BenchmarkBytesCopy(b *testing.B) {
	src := []byte("Hello, World! This is a test payload.")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst := make([]byte, len(src))
		copy(dst, src)
	}
}

func BenchmarkBytesSlice(b *testing.B) {
	src := []byte("Hello, World! This is a test payload.")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst := src[:]
		_ = dst
	}
}

// ── JSON streaming benchmark ───────────────────────────────────────────────

func BenchmarkJSONStream(b *testing.B) {
	data := []map[string]any{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Charlie"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		for _, item := range data {
			_ = enc.Encode(item)
		}
	}
}

// ── HTTP date formatting benchmark ─────────────────────────────────────────

func BenchmarkHTTPDate(b *testing.B) {
	now := time.Now().UTC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = now.Format(time.RFC1123)
	}
}

func BenchmarkHTTPDateAppend(b *testing.B) {
	now := time.Now().UTC()
	buf := make([]byte, 0, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf = append(buf, "Date: "...)
		buf = now.AppendFormat(buf, "Mon, 02 Jan 2006 15:04:05 GMT")
		buf = append(buf, '\r', '\n')
	}
}

// ── Header find benchmarks ─────────────────────────────────────────────────

func BenchmarkHeaderFind(b *testing.B) {
	headers := make([]Header, 64)
	for i := range headers {
		headers[i] = Header{
			Key:   []byte(fmt.Sprintf("X-Custom-Header-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
	}
	target := []byte("X-Custom-Header-32")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, h := range headers {
			if bytes.Equal(h.Key, target) {
				break
			}
		}
	}
}

func BenchmarkHeaderFindFold(b *testing.B) {
	headers := make([]Header, 64)
	for i := range headers {
		headers[i] = Header{
			Key:   []byte(fmt.Sprintf("X-Custom-Header-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
	}
	target := []byte("x-custom-header-32")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, h := range headers {
			if bytesEqualFold(h.Key, target) {
				break
			}
		}
	}
}

// ── Int to string benchmark ────────────────────────────────────────────────

func BenchmarkIntToString(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%d", i)
	}
}

func BenchmarkIntToStringAppend(b *testing.B) {
	buf := make([]byte, 0, 32)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf = strconv.AppendInt(buf, int64(i), 10)
	}
}

// ── Benchmark with net/http for comparison ──────────────────────────────────

func BenchmarkNetHTTPHandler(b *testing.B) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		fmt.Fprint(w, "Hello, World!")
	})

	req := httptest.NewRequest("GET", "/plaintext", nil)
	w := httptest.NewRecorder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(w, req)
		w.Code = 0
		w.Body.Reset()
	}
}

func BenchmarkNetHTTPJSON(b *testing.B) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]string{"message": "Hello, World!"})
	})

	req := httptest.NewRequest("GET", "/json", nil)
	w := httptest.NewRecorder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(w, req)
		w.Code = 0
		w.Body.Reset()
	}
}
