package override_test

import (
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/override"
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
	return nil, io.EOF
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

func TestMethodOverride(t *testing.T) {
	app := fh.New()
	app.Use(override.New())
	app.Put("/users/42", func(c fh.Ctx) error {
		return c.SendString("updated user 42")
	})

	req := "POST /users/42 HTTP/1.1\r\nHost: local\r\nX-HTTP-Method-Override: PUT\r\nConnection: close\r\n\r\n"
	resp := pipeReq(t, app, req)
	if !strings.Contains(resp, "200 OK") || !strings.Contains(resp, "updated user 42") {
		t.Fatalf("unexpected method override response: %s", resp)
	}
}
