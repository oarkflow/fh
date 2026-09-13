package servertiming

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
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

func rawGet(t *testing.T, addr string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return string(resp)
}

// serverTimingHeader extracts the value of the Server-Timing response
// header from a raw HTTP response, or "" if absent.
func serverTimingHeader(resp string) string {
	for _, line := range strings.Split(resp, "\r\n") {
		if k, v, ok := strings.Cut(line, ":"); ok && strings.EqualFold(strings.TrimSpace(k), "Server-Timing") {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// TestGetReturnsNilWithoutMiddleware guards against a nil-pointer panic:
// handlers that call servertiming.Get(c) without the middleware installed
// (e.g. in a unit test, or a route not covered by app.Use) must get a nil
// *Timings back, not a type-assertion panic.
func TestGetReturnsNilWithoutMiddleware(t *testing.T) {
	app := fh.New()
	app.Get("/", func(c fh.Ctx) error {
		if Get(c) != nil {
			t.Error("expected nil Timings when middleware is not installed")
		}
		return c.SendString("ok")
	})
	addr := testServer(t, app)
	resp := rawGet(t, addr)
	if !strings.Contains(resp, "200") {
		t.Fatalf("unexpected response: %s", resp)
	}
}

// TestDefaultAddsTotalMetricFirst proves the automatic "total" metric is
// present and ordered first, ahead of any handler-recorded spans, matching
// documented behavior and the RFC 8638 usage example.
func TestDefaultAddsTotalMetricFirst(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error {
		t := Get(c)
		t.Start("db")
		time.Sleep(2 * time.Millisecond)
		t.Stop("db")
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	header := serverTimingHeader(rawGet(t, addr))
	if header == "" {
		t.Fatal("expected a Server-Timing header")
	}
	entries := strings.Split(header, ",")
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (total, db), got %d: %q", len(entries), header)
	}
	if !strings.HasPrefix(strings.TrimSpace(entries[0]), "total;dur=") {
		t.Fatalf("expected first entry to be total;dur=..., got %q", entries[0])
	}
	if !strings.HasPrefix(strings.TrimSpace(entries[1]), "db;dur=") {
		t.Fatalf("expected second entry to be db;dur=..., got %q", entries[1])
	}
}

// TestAddMetricWithoutDurationOmitsDurParam proves a metric added with no
// duration argument does not emit a bogus "dur=" parameter. (AddTotal
// cannot be disabled through New()'s current merge logic, so the automatic
// "total" entry is still expected ahead of the custom metric.)
func TestAddMetricWithoutDurationOmitsDurParam(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error {
		Get(c).AddMetric("cache", "hit")
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	header := serverTimingHeader(rawGet(t, addr))
	entries := strings.Split(header, ",")
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (total, cache), got %d: %q", len(entries), header)
	}
	if entries[1] != `cache;desc="hit"` {
		t.Fatalf("second entry = %q, want %q", entries[1], `cache;desc="hit"`)
	}
}

// TestAddBytesFormatsExtraParam proves AddBytes emits a plain bytes=N
// parameter with no duration.
func TestAddBytesFormatsExtraParam(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error {
		Get(c).AddBytes("payload", 1234)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	header := serverTimingHeader(rawGet(t, addr))
	entries := strings.Split(header, ",")
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (total, payload), got %d: %q", len(entries), header)
	}
	if entries[1] != "payload;bytes=1234" {
		t.Fatalf("second entry = %q, want %q", entries[1], "payload;bytes=1234")
	}
}

// TestMetricWithoutDurationHasSingleSeparator is a regression test for a
// malformed-header bug: a metric with no duration (as produced by
// AddMetric/AddBytes, the common case) must render as "name;desc=..." with
// a single semicolon, not "name;;desc=..." with a stray empty parameter.
func TestMetricWithoutDurationHasSingleSeparator(t *testing.T) {
	timings := &Timings{started: time.Now()}
	timings.AddMetric("cache", "hit")
	got := buildHeader(timings, DefaultConfig)
	if got != `cache;desc="hit"` {
		t.Fatalf("buildHeader = %q, want %q", got, `cache;desc="hit"`)
	}
}

// TestMaxMetricsTruncatesHeader proves MaxMetrics is actually enforced: a
// handler that records more metrics than the configured cap must not have
// them all serialized into the response header (unbounded metric counts
// coming from request-influenced code paths could otherwise grow the
// response header without bound).
func TestMaxMetricsTruncatesHeader(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{MaxMetrics: 2}))
	app.Get("/", func(c fh.Ctx) error {
		timings := Get(c)
		for i := 0; i < 10; i++ {
			timings.AddMetric(fmt.Sprintf("m%d", i), "")
		}
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	header := serverTimingHeader(rawGet(t, addr))
	entries := strings.Split(header, ",")
	if len(entries) != 2 {
		t.Fatalf("expected header truncated to 2 entries, got %d: %q", len(entries), header)
	}
}

// TestDescriptionIsEscaped proves a description value containing a quote or
// backslash cannot break out of the quoted desc="..." parameter and inject
// additional Server-Timing parameters/entries.
func TestDescriptionIsEscaped(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error {
		Get(c).AddMetric("m", `x"; injected="1`)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	header := serverTimingHeader(rawGet(t, addr))
	entries := strings.Split(header, ",")
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (total, m), got %d: %q", len(entries), header)
	}
	want := `m;desc="x\"; injected=\"1"`
	if entries[1] != want {
		t.Fatalf("second entry = %q, want %q", entries[1], want)
	}
	// Sanity: appendEscaped in isolation.
	if got := string(appendEscaped(nil, `a"b\c`)); got != `a\"b\\c` {
		t.Fatalf("appendEscaped = %q, want %q", got, `a\"b\\c`)
	}
}

// TestOpaqueDoesNotLeakDurationValue is a regression test for a fail-open
// information-disclosure bug: the package README states "use Opaque where
// duration disclosure is undesirable", so when Opaque is enabled the actual
// millisecond timing value must not be sent to the client at all -- only a
// marker that timing information is being withheld. Previously, Opaque only
// appended an ";err" flag next to the real "dur=<value>", meaning the exact
// (potentially sensitive, e.g. backend/DB latency revealing internal
// topology) duration was disclosed regardless of the Opaque setting.
func TestOpaqueDoesNotLeakDurationValue(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{Opaque: true}))
	app.Get("/", func(c fh.Ctx) error {
		timings := Get(c)
		timings.Start("db")
		time.Sleep(5 * time.Millisecond)
		timings.Stop("db")
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	header := serverTimingHeader(rawGet(t, addr))
	if header == "" {
		t.Fatal("expected a Server-Timing header")
	}
	if strings.Contains(header, "dur=") {
		t.Fatalf("Opaque mode leaked a duration value in header %q", header)
	}
	if !strings.Contains(header, "err") {
		t.Fatalf("expected an opaque/err marker in header %q", header)
	}
}

// TestConcurrentTimingsAreRaceSafe simulates multiple goroutines within a
// single request (e.g. fan-out calls to several backends) recording spans
// and metrics on the same *Timings concurrently. Run with -race.
func TestConcurrentTimingsAreRaceSafe(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error {
		timings := Get(c)
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				name := fmt.Sprintf("span%d", i)
				timings.Start(name)
				timings.Stop(name)
				timings.AddMetric(fmt.Sprintf("metric%d", i), "")
				timings.AddBytes(fmt.Sprintf("bytes%d", i), int64(i))
			}(i)
		}
		wg.Wait()
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	header := serverTimingHeader(rawGet(t, addr))
	if header == "" {
		t.Fatal("expected a non-empty Server-Timing header")
	}
}

// TestStopWithoutMatchingStartIsNoop proves calling Stop for a span name
// that was never Start-ed does not panic and does not fabricate a metric.
func TestStopWithoutMatchingStartIsNoop(t *testing.T) {
	timings := &Timings{started: time.Now()}
	timings.Stop("never-started")
	if len(timings.metrics) != 0 {
		t.Fatalf("expected no metrics recorded, got %d", len(timings.metrics))
	}
}

// TestSetNameOnEmptyMetricsDoesNotPanic proves SetName is safe to call
// before any metric (including the automatic total) has been recorded.
func TestSetNameOnEmptyMetricsDoesNotPanic(t *testing.T) {
	timings := &Timings{started: time.Now()}
	timings.SetName("custom-total") // must not panic
	if len(timings.metrics) != 0 {
		t.Fatalf("expected no metrics recorded, got %d", len(timings.metrics))
	}
}

// TestNextSkipsMiddlewareEntirely proves a configured Next/skip function
// bypasses timing collection (and thus the header) altogether for matching
// requests.
func TestNextSkipsMiddlewareEntirely(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{Next: func(c fh.Ctx) bool { return c.Path() == "/skip" }}))
	app.Get("/skip", func(c fh.Ctx) error { return c.SendString("skip") })
	app.Get("/track", func(c fh.Ctx) error { return c.SendString("track") })
	addr := testServer(t, app)

	skipConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(skipConn, "GET /skip HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	skipResp, _ := io.ReadAll(skipConn)
	skipConn.Close()
	if serverTimingHeader(string(skipResp)) != "" {
		t.Fatalf("expected no Server-Timing header for skipped route, got %q", string(skipResp))
	}

	trackConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(trackConn, "GET /track HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	trackResp, _ := io.ReadAll(trackConn)
	trackConn.Close()
	if serverTimingHeader(string(trackResp)) == "" {
		t.Fatal("expected a Server-Timing header for non-skipped route")
	}
}
