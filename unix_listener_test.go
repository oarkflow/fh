package fh_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestListenUnixContextServesAndCleansSocket(t *testing.T) {
	// Unix-domain socket paths are limited to roughly 104 bytes on macOS.
	// testing.T.TempDir can exceed that once the test name and random suffix
	// are included, so use a deliberately short system-temp directory.
	dir, err := os.MkdirTemp("", "fh-unix-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "fh.sock")
	app := fh.New(fh.WithStartupBannerDisabled(true))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("unix") })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- app.ListenUnixContext(ctx, path) }()

	var conn net.Conn
	err = nil
	for i := 0; i < 100; i++ {
		conn, err = net.Dial("unix", path)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial Unix socket: %v", err)
	}
	_, _ = conn.Write([]byte("GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"))
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("ListenUnixContext() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Unix server did not stop")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("Unix socket still exists after shutdown: %v", err)
	}
}
