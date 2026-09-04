package fh_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestServeContextGracefullyStops(t *testing.T) {
	app := fh.New(fh.WithStartupBannerDisabled(true))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- app.ServeContext(ctx, ln) }()
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("ServeContext() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ServeContext did not stop after context cancellation")
	}
}

func TestServeContextRejectsNilContext(t *testing.T) {
	app := fh.New()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := app.ServeContext(nil, ln); err == nil {
		t.Fatal("ServeContext(nil, listener) returned nil")
	}
}
