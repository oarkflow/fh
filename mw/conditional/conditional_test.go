package conditional

import (
	"fmt"
	"io"
	"net"
	"strings"
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

func doRequest(t *testing.T, addr, method, path string, headers map[string]string) (statusCode int, respHeaders map[string]string, body string) {
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
	req += "Connection: close\r\n\r\n"

	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	raw := string(resp)
	head, rest, found := strings.Cut(raw, "\r\n\r\n")
	if !found {
		head = raw
	} else {
		body = rest
	}
	lines := strings.Split(head, "\r\n")
	if len(lines) > 0 {
		var proto, status string
		fmt.Sscan(lines[0], &proto, &status)
		fmt.Sscan(status, &statusCode)
	}
	respHeaders = make(map[string]string)
	for _, line := range lines[1:] {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		respHeaders[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
	}
	return
}

// TestETagMatchReturnsNotModified proves a matching If-None-Match short
// circuits with 304 and still sets the ETag response header, without
// running the handler body.
func TestETagMatchReturnsNotModified(t *testing.T) {
	handlerRan := false
	app := fh.New()
	app.Use(New(Config{ETag: func(fh.Ctx) string { return `"v1"` }}))
	app.Get("/", func(c fh.Ctx) error {
		handlerRan = true
		return c.SendString("body")
	})
	addr := testServer(t, app)

	status, headers, _ := doRequest(t, addr, "GET", "/", map[string]string{"If-None-Match": `"v1"`})
	if status != fh.StatusNotModified {
		t.Fatalf("status = %d, want 304", status)
	}
	if headers["etag"] != `"v1"` {
		t.Fatalf("etag header = %q, want %q", headers["etag"], `"v1"`)
	}
	if handlerRan {
		t.Fatal("handler should not run on a 304 short-circuit")
	}
}

// TestETagMismatchRunsHandler proves a non-matching If-None-Match lets the
// request through to the handler while still setting the current ETag.
func TestETagMismatchRunsHandler(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{ETag: func(fh.Ctx) string { return `"v2"` }}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("body") })
	addr := testServer(t, app)

	status, headers, body := doRequest(t, addr, "GET", "/", map[string]string{"If-None-Match": `"v1"`})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if headers["etag"] != `"v2"` {
		t.Fatalf("etag header = %q, want %q", headers["etag"], `"v2"`)
	}
	if body != "body" {
		t.Fatalf("body = %q, want %q", body, "body")
	}
}

// TestWildcardIfNoneMatchAlwaysMatches proves "*" is treated as matching any
// current representation, per RFC 7232.
func TestWildcardIfNoneMatchAlwaysMatches(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{ETag: func(fh.Ctx) string { return `"anything"` }}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("body") })
	addr := testServer(t, app)

	status, _, _ := doRequest(t, addr, "GET", "/", map[string]string{"If-None-Match": "*"})
	if status != fh.StatusNotModified {
		t.Fatalf("status = %d, want 304 for a wildcard If-None-Match", status)
	}
}

// TestMultipleETagsInHeaderAreAllChecked proves a comma-separated
// If-None-Match list is matched entry-by-entry, not just against the whole
// raw header string.
func TestMultipleETagsInHeaderAreAllChecked(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{ETag: func(fh.Ctx) string { return `"v2"` }}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("body") })
	addr := testServer(t, app)

	status, _, _ := doRequest(t, addr, "GET", "/", map[string]string{"If-None-Match": `"v1", "v2", "v3"`})
	if status != fh.StatusNotModified {
		t.Fatalf("status = %d, want 304 when the current tag appears anywhere in a comma-separated list", status)
	}
}

// TestWeakComparisonMatchesWithPrefixDifference proves WeakCompare treats
// "W/\"v1\"" and "\"v1\"" as equivalent, while the default (strict) mode
// treats them as distinct.
func TestWeakComparisonMatchesWithPrefixDifference(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{ETag: func(fh.Ctx) string { return `"v1"` }, WeakCompare: true}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("body") })
	addr := testServer(t, app)

	status, _, _ := doRequest(t, addr, "GET", "/", map[string]string{"If-None-Match": `W/"v1"`})
	if status != fh.StatusNotModified {
		t.Fatalf("status = %d, want 304 under weak comparison", status)
	}
}

// TestStrictComparisonRejectsWeakTagMismatch proves that without
// WeakCompare, a weak validator in the request does not match a strong
// current tag, so the handler still runs.
func TestStrictComparisonRejectsWeakTagMismatch(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{ETag: func(fh.Ctx) string { return `"v1"` }, WeakCompare: false}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("body") })
	addr := testServer(t, app)

	status, _, _ := doRequest(t, addr, "GET", "/", map[string]string{"If-None-Match": `W/"v1"`})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200: weak validator must not satisfy a strict comparison", status)
	}
}

// TestEmptyETagFromFuncDoesNotSetHeaderOrShortCircuit proves that when the
// ETag func returns "" (e.g. the resource has no known tag yet), the
// middleware skips ETag handling entirely rather than emitting a blank
// header or panicking.
func TestEmptyETagFromFuncDoesNotSetHeaderOrShortCircuit(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{ETag: func(fh.Ctx) string { return "" }}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("body") })
	addr := testServer(t, app)

	status, headers, _ := doRequest(t, addr, "GET", "/", map[string]string{"If-None-Match": "*"})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 when the ETag func has nothing to report", status)
	}
	if _, ok := headers["etag"]; ok {
		t.Fatal("expected no ETag response header when the ETag func returns empty")
	}
}

// TestLastModifiedNotModifiedWhenUnchanged proves that when the resource's
// timestamp is not after the client's If-Modified-Since, the response is a
// 304 and the handler does not run.
func TestLastModifiedNotModifiedWhenUnchanged(t *testing.T) {
	handlerRan := false
	lm := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	app := fh.New()
	app.Use(New(Config{LastModified: func(fh.Ctx) time.Time { return lm }}))
	app.Get("/", func(c fh.Ctx) error {
		handlerRan = true
		return c.SendString("body")
	})
	addr := testServer(t, app)

	status, headers, _ := doRequest(t, addr, "GET", "/", map[string]string{"If-Modified-Since": lm.Format(time.RFC1123)})
	if status != fh.StatusNotModified {
		t.Fatalf("status = %d, want 304", status)
	}
	if headers["last-modified"] == "" {
		t.Fatal("expected Last-Modified header to be set even on 304")
	}
	if handlerRan {
		t.Fatal("handler should not run on a 304 short-circuit")
	}
}

// TestLastModifiedRunsHandlerWhenResourceIsNewer proves a resource modified
// after the client's cached timestamp is served fresh (200), not
// short-circuited.
func TestLastModifiedRunsHandlerWhenResourceIsNewer(t *testing.T) {
	older := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	newer := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	app := fh.New()
	app.Use(New(Config{LastModified: func(fh.Ctx) time.Time { return newer }}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("fresh") })
	addr := testServer(t, app)

	status, _, body := doRequest(t, addr, "GET", "/", map[string]string{"If-Modified-Since": older.Format(time.RFC1123)})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 when the resource is newer than If-Modified-Since", status)
	}
	if body != "fresh" {
		t.Fatalf("body = %q, want %q", body, "fresh")
	}
}

// TestMalformedIfModifiedSinceIsIgnoredNotFatal proves an unparsable
// If-Modified-Since header does not panic or wrongly short-circuit the
// request — the middleware must fail open to "serve normally" on malformed
// client input for this header, since RFC 7232 treats an invalid date as if
// the header were absent.
func TestMalformedIfModifiedSinceIsIgnoredNotFatal(t *testing.T) {
	lm := time.Now().UTC()
	app := fh.New()
	app.Use(New(Config{LastModified: func(fh.Ctx) time.Time { return lm }}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _, _ := doRequest(t, addr, "GET", "/", map[string]string{"If-Modified-Since": "not-a-valid-http-date"})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 for a malformed If-Modified-Since header", status)
	}
}

// TestZeroLastModifiedIsSkipped proves that when the LastModified func
// returns the zero time (e.g. unknown), the middleware does not set the
// header or attempt the comparison.
func TestZeroLastModifiedIsSkipped(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{LastModified: func(fh.Ctx) time.Time { return time.Time{} }}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, headers, _ := doRequest(t, addr, "GET", "/", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if _, ok := headers["last-modified"]; ok {
		t.Fatal("expected no Last-Modified header for a zero time")
	}
}

// TestETagMismatchStillEvaluatesLastModified proves both checks are
// independently evaluated: an ETag mismatch alone does not skip the
// LastModified check, so a request can still be short-circuited by
// LastModified even after ETag decided not to.
func TestETagMismatchStillEvaluatesLastModified(t *testing.T) {
	lm := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	app := fh.New()
	app.Use(New(Config{
		ETag:         func(fh.Ctx) string { return `"current"` },
		LastModified: func(fh.Ctx) time.Time { return lm },
	}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("body") })
	addr := testServer(t, app)

	status, headers, _ := doRequest(t, addr, "GET", "/", map[string]string{
		"If-None-Match":     `"stale"`,
		"If-Modified-Since": lm.Format(time.RFC1123),
	})
	if status != fh.StatusNotModified {
		t.Fatalf("status = %d, want 304 via the LastModified check even though If-None-Match did not match", status)
	}
	if headers["etag"] != `"current"` {
		t.Fatalf("etag header = %q, want %q (ETag header should still be set)", headers["etag"], `"current"`)
	}
}

// TestNoConditionalConfigRunsHandlerNormally proves a Config with neither
// ETag nor LastModified is a pure no-op.
func TestNoConditionalConfigRunsHandlerNormally(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _, body := doRequest(t, addr, "GET", "/", map[string]string{"If-None-Match": "*"})
	if status != fh.StatusOK || body != "ok" {
		t.Fatalf("status = %d, body = %q, want 200/\"ok\"", status, body)
	}
}

// TestMatchETagDirectly is a focused unit test of the unexported matcher
// covering the comma-splitting, whitespace-trimming, and weak-comparison
// edge cases without going through a full HTTP round trip.
func TestMatchETagDirectly(t *testing.T) {
	cases := []struct {
		header string
		tag    string
		weak   bool
		want   bool
	}{
		{"", `"a"`, false, false},
		{`"a"`, `"a"`, false, true},
		{`"a", "b"`, `"b"`, false, true},
		{` "a" , "b" `, `"b"`, false, true},
		{"*", `"anything"`, false, true},
		{`W/"a"`, `"a"`, true, true},
		{`W/"a"`, `"a"`, false, false},
		{`"a"`, `"b"`, false, false},
	}
	for _, tc := range cases {
		got := matchETag(tc.header, tc.tag, tc.weak)
		if got != tc.want {
			t.Errorf("matchETag(%q, %q, %v) = %v, want %v", tc.header, tc.tag, tc.weak, got, tc.want)
		}
	}
}

// TestStripWeakDirectly is a focused unit test of the weak-prefix stripper.
func TestStripWeakDirectly(t *testing.T) {
	cases := map[string]string{
		`W/"a"`:   `"a"`,
		`"a"`:     `"a"`,
		` W/"a" `: `"a"`,
		"":        "",
	}
	for in, want := range cases {
		if got := stripWeak(in); got != want {
			t.Errorf("stripWeak(%q) = %q, want %q", in, got, want)
		}
	}
}
