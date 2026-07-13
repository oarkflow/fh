package coalesce

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func testServer(t *testing.T, app *fh.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.Shutdown() })
	go app.Serve(ln)
	time.Sleep(10 * time.Millisecond)
	return ln.Addr().String()
}

func doRequest(t *testing.T, addr, method, path string, headers map[string]string) (statusCode int, respBody string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: localhost\r\n", method, path)
	for k, v := range headers {
		req += k + ": " + v + "\r\n"
	}
	req += "\r\n"

	conn.Write([]byte(req))
	conn.(*net.TCPConn).CloseWrite()

	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(resp), "\r\n", 2)
	if len(parts) < 1 {
		t.Fatal("empty response")
	}
	var proto, status string
	fmt.Sscan(parts[0], &proto, &status)
	fmt.Sscan(status, &statusCode)

	idx := strings.Index(string(resp), "\r\n\r\n")
	if idx >= 0 {
		respBody = string(resp)[idx+4:]
	}
	return
}

// TestDistinctQueryStringsAreNotCoalesced proves two concurrent requests to
// the same path differing only by query string get their own response
// instead of one waiting for and receiving the other's.
func TestDistinctQueryStringsAreNotCoalesced(t *testing.T) {
	var wg sync.WaitGroup
	release := make(chan struct{})
	app := fh.New()
	app.Use(New())
	app.Get("/search", func(c fh.Ctx) error {
		<-release
		return c.SendString("result:" + c.Query("q"))
	})
	addr := testServer(t, app)

	results := make([]string, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, results[0] = doRequest(t, addr, "GET", "/search?q=foo", nil)
	}()
	go func() {
		defer wg.Done()
		_, results[1] = doRequest(t, addr, "GET", "/search?q=bar", nil)
	}()
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()

	if results[0] != "result:foo" {
		t.Fatalf("expected result:foo, got %q", results[0])
	}
	if results[1] != "result:bar" {
		t.Fatalf("expected result:bar (not coalesced with the foo request), got %q", results[1])
	}
}

// TestAuthenticatedRequestsAreNeverCoalesced proves two concurrent
// authenticated requests to the same path never fan out one caller's
// response to the other.
func TestAuthenticatedRequestsAreNeverCoalesced(t *testing.T) {
	var calls atomic.Int32
	var wg sync.WaitGroup
	release := make(chan struct{})
	app := fh.New()
	app.Use(New())
	app.Get("/me", func(c fh.Ctx) error {
		calls.Add(1)
		<-release
		return c.SendString("profile:" + c.Get("Authorization"))
	})
	addr := testServer(t, app)

	results := make([]string, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, results[0] = doRequest(t, addr, "GET", "/me", map[string]string{"Authorization": "victim"})
	}()
	go func() {
		defer wg.Done()
		_, results[1] = doRequest(t, addr, "GET", "/me", map[string]string{"Authorization": "attacker"})
	}()
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls.Load() != 2 {
		t.Fatalf("expected handler invoked once per authenticated caller, got %d", calls.Load())
	}
	if results[0] != "profile:victim" {
		t.Fatalf("expected profile:victim, got %q", results[0])
	}
	if results[1] != "profile:attacker" {
		t.Fatalf("expected profile:attacker (not victim's coalesced response), got %q", results[1])
	}
}

// TestConcurrentIdenticalUnauthenticatedRequestsAreCoalesced proves the
// intended thundering-herd protection still works for plain, unauthenticated
// concurrent requests to the exact same URL.
func TestConcurrentIdenticalUnauthenticatedRequestsAreCoalesced(t *testing.T) {
	var calls atomic.Int32
	var wg sync.WaitGroup
	release := make(chan struct{})
	app := fh.New()
	app.Use(New())
	app.Get("/shared", func(c fh.Ctx) error {
		calls.Add(1)
		<-release
		return c.SendString("shared-result")
	})
	addr := testServer(t, app)

	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			doRequest(t, addr, "GET", "/shared", nil)
		}()
	}
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("expected exactly one handler execution for coalesced identical requests, got %d", calls.Load())
	}
}
