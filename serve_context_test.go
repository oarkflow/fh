package fh_test

import (
	"context"
	"io"
	"net"
	"net/http/httptest"
	"strings"
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

func TestShutdownWithContextRejectsNilContext(t *testing.T) {
	if err := fh.New().ShutdownWithContext(nil); err == nil {
		t.Fatal("ShutdownWithContext(nil) returned nil")
	}
}

func TestListenContextGracefullyStops(t *testing.T) {
	app := fh.New(fh.WithStartupBannerDisabled(true))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- app.ListenContext(ctx, "127.0.0.1:0") }()
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("ListenContext() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ListenContext did not stop after context cancellation")
	}
}

func TestServeContextDrainsInFlightRequest(t *testing.T) {
	app := fh.New(fh.WithStartupBannerDisabled(true), fh.WithShutdownTimeout(time.Second))
	started := make(chan struct{})
	finish := make(chan struct{})
	app.Get("/", func(c fh.Ctx) error {
		close(started)
		<-finish
		return c.SendString("completed")
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- app.ServeContext(ctx, ln) }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	close(finish)
	response, err := io.ReadAll(conn)
	_ = conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(response), "completed") {
		t.Fatalf("in-flight response was not drained: %q", response)
	}
	if err := <-serveErr; err != nil {
		t.Fatalf("ServeContext() error = %v", err)
	}
}

func TestConnContextFlowsIntoRequestContext(t *testing.T) {
	type contextKey string
	const key contextKey = "connection"
	const baseKey contextKey = "base"
	app := fh.New(
		fh.WithBaseContext(func(net.Listener) context.Context {
			return context.WithValue(context.Background(), baseKey, "app")
		}),
		fh.WithConnContext(func(ctx context.Context, _ net.Conn) context.Context {
			if ctx.Value(baseKey) != "app" {
				return nil
			}
			return context.WithValue(ctx, key, "tenant-a")
		}),
	)
	app.Get("/", func(c fh.Ctx) error {
		value, _ := c.Context().Value(key).(string)
		base, _ := c.Context().Value(baseKey).(string)
		return c.SendString(base + ":" + value)
	})
	resp, err := app.Test(httptest.NewRequest("GET", "http://local/", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "app:tenant-a" {
		t.Fatalf("request context value = %q, want app:tenant-a", body)
	}
}
