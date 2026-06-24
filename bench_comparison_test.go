package fh_test

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// ── In-memory pipe-based listener (no TCP stack, low noise) ───────────────

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

type pipeListener struct {
	ch   chan net.Conn
	done chan struct{}
	once sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		ch:   make(chan net.Conn, 1),
		done: make(chan struct{}),
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return pipeAddr{} }

// deadlinePipe wraps net.Pipe and ignores deadline calls.
type deadlinePipe struct{ net.Conn }

func (d *deadlinePipe) SetReadDeadline(t time.Time) error  { return nil }
func (d *deadlinePipe) SetWriteDeadline(t time.Time) error { return nil }
func (d *deadlinePipe) SetDeadline(t time.Time) error      { return nil }

func pipePair() (client, server net.Conn) {
	c, s := net.Pipe()
	return &deadlinePipe{c}, &deadlinePipe{s}
}

// ── fasthttp pipe benchmarks ──────────────────────────────────────────────

func BenchmarkFH_HelloWorld(b *testing.B) {
	app := fh.New()
	app.Get("/bench", func(ctx fh.Ctx) error {
		return ctx.SendString("hello")
	})

	ln := newPipeListener()
	go app.Serve(ln)
	defer app.Shutdown()

	client, server := pipePair()
	ln.ch <- server
	defer client.Close()

	req := []byte("GET /bench HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
	buf := make([]byte, 4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		client.Write(req)
		client.Read(buf)
	}
}

func BenchmarkFH_ParallelRequests(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		app := fh.New()
		app.Get("/bench", func(ctx fh.Ctx) error {
			return ctx.SendString("hello")
		})
		ln := newPipeListener()
		go app.Serve(ln)
		defer app.Shutdown()

		client, server := pipePair()
		ln.ch <- server
		defer client.Close()

		req := []byte("GET /bench HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
		buf := make([]byte, 4096)
		for pb.Next() {
			client.Write(req)
			client.Read(buf)
		}
	})
}

func BenchmarkFH_RouteWithParams(b *testing.B) {
	app := fh.New()
	app.Get("/users/:id/posts/:post", func(ctx fh.Ctx) error {
		return ctx.SendString(ctx.Param("id") + ctx.Param("post"))
	})

	ln := newPipeListener()
	go app.Serve(ln)
	defer app.Shutdown()

	client, server := pipePair()
	ln.ch <- server
	defer client.Close()

	req := []byte("GET /users/42/posts/7 HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
	buf := make([]byte, 4096)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		client.Write(req)
		client.Read(buf)
	}
}

var _ = io.Discard
