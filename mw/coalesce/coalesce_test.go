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

func TestCompletedResponsesAreNotReused(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(New())
	app.Get("/fresh", func(c fh.Ctx) error {
		return c.SendString(fmt.Sprintf("call:%d", calls.Add(1)))
	})
	addr := testServer(t, app)

	_, first := doRequest(t, addr, "GET", "/fresh", nil)
	_, second := doRequest(t, addr, "GET", "/fresh", nil)
	if first != "call:1" || second != "call:2" {
		t.Fatalf("completed response was reused: first=%q second=%q", first, second)
	}
}

func TestCustomKeyPartitionsCookieRequests(t *testing.T) {
	var calls atomic.Int32
	var wg sync.WaitGroup
	release := make(chan struct{})
	app := fh.New()
	app.Use(New(Config{KeyFunc: func(c fh.Ctx) string { return c.Get("X-Identity") + c.OriginalURL() }}))
	app.Get("/profile", func(c fh.Ctx) error {
		calls.Add(1)
		<-release
		return c.SendString("profile:" + c.Get("X-Identity"))
	})
	addr := testServer(t, app)

	results := make([]string, 3)
	identities := []string{"alice", "alice", "bob"}
	wg.Add(len(identities))
	for i, identity := range identities {
		i, identity := i, identity
		go func() {
			defer wg.Done()
			_, results[i] = doRequest(t, addr, "GET", "/profile", map[string]string{"Cookie": "session=x", "X-Identity": identity})
		}()
	}
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls.Load() != 2 {
		t.Fatalf("handler calls = %d, want one per identity", calls.Load())
	}
	if results[0] != "profile:alice" || results[1] != "profile:alice" || results[2] != "profile:bob" {
		t.Fatalf("unexpected identity responses: %v", results)
	}
}

func TestLargeResponsesAreNotShared(t *testing.T) {
	var calls atomic.Int32
	var wg sync.WaitGroup
	release := make(chan struct{})
	app := fh.New()
	app.Use(New(Config{MaxBodySize: 4}))
	app.Get("/large", func(c fh.Ctx) error {
		calls.Add(1)
		<-release
		return c.SendString("large-response")
	})
	addr := testServer(t, app)

	results := make([]string, 2)
	wg.Add(2)
	for i := range results {
		i := i
		go func() {
			defer wg.Done()
			_, results[i] = doRequest(t, addr, "GET", "/large", nil)
		}()
	}
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls.Load() != 2 {
		t.Fatalf("large response handler calls = %d, want 2", calls.Load())
	}
	for _, result := range results {
		if result != "large-response" {
			t.Fatalf("large response = %q, want full response", result)
		}
	}
}

func TestConfiguredVaryHeadersPartitionRequests(t *testing.T) {
	var calls atomic.Int32
	var wg sync.WaitGroup
	release := make(chan struct{})
	app := fh.New()
	app.Use(New(Config{VaryHeaders: []string{"Accept-Language"}}))
	app.Get("/localized", func(c fh.Ctx) error {
		calls.Add(1)
		<-release
		return c.SendString(c.Get("Accept-Language"))
	})
	addr := testServer(t, app)

	results := make([]string, 2)
	languages := []string{"en", "fr"}
	wg.Add(len(languages))
	for i, language := range languages {
		i, language := i, language
		go func() {
			defer wg.Done()
			_, results[i] = doRequest(t, addr, "GET", "/localized", map[string]string{"Accept-Language": language})
		}()
	}
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls.Load() != 2 || results[0] != "en" || results[1] != "fr" {
		t.Fatalf("vary results = %v; calls = %d, want independent locale responses", results, calls.Load())
	}
}
