package recover_test

import (
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	rec "github.com/oarkflow/fh/mw/recover"
)

type singleListener struct {
	conn net.Conn
	done chan struct{}
	once sync.Once
}

func newSingleListener(c net.Conn) *singleListener {
	return &singleListener{conn: c, done: make(chan struct{})}
}

func (l *singleListener) Accept() (net.Conn, error) {
	if l.conn != nil {
		c := l.conn
		l.conn = nil
		return c, nil
	}
	<-l.done
	return nil, net.ErrClosed
}

func (l *singleListener) Close() error { l.once.Do(func() { close(l.done) }); return nil }
func (*singleListener) Addr() net.Addr { return &net.IPAddr{} }

func pipeReq(t *testing.T, app *fh.App, request string) string {
	t.Helper()
	client, server := net.Pipe()
	ln := newSingleListener(server)
	go func() { _ = app.Serve(ln) }()
	go func() {
		_, _ = io.WriteString(client, request)
	}()
	res, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(); _ = app.ShutdownWithTimeout(time.Second) })
	return string(res)
}

func TestRecoverRFC9457(t *testing.T) {
	app := fh.New()
	app.Use(rec.New())
	app.Get("/panic", func(c fh.Ctx) error {
		panic("boom")
	})

	resp := pipeReq(t, app, "GET /panic HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "500 Internal Server Error") || !strings.Contains(resp, "application/problem+json") || !strings.Contains(resp, "An unexpected panic occurred: boom") {
		t.Fatalf("unexpected panic recover response: %s", resp)
	}
}
