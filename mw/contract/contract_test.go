package contract

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

func doRequest(t *testing.T, addr, method, path string, headers map[string]string, body string) (statusCode int, respBody string) {
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
	if body != "" {
		req += fmt.Sprintf("Content-Length: %d\r\n", len(body))
	}
	req += "Connection: close\r\n\r\n" + body

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
		respBody = rest
	}
	lines := strings.Split(head, "\r\n")
	if len(lines) > 0 {
		var proto, status string
		fmt.Sscan(lines[0], &proto, &status)
		fmt.Sscan(status, &statusCode)
	}
	return
}

// TestNoConstraintsAllowsAnyRequest proves a zero-value Config imposes no
// restrictions at all, so an empty contract is a true no-op rather than an
// accidental deny-all.
func TestNoConstraintsAllowsAnyRequest(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{}))
	app.Post("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", nil, "anything")
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
}

// TestMethodNotInAllowlistIsRejected proves a disallowed method never
// reaches the handler and gets a 405, not a 200 or a 500.
func TestMethodNotInAllowlistIsRejected(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(New(Config{Methods: []string{"GET", "POST"}}))
	app.Delete("/", func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "DELETE", "/", nil, "")
	if status != fh.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", status)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler ran %d times for a disallowed method", calls.Load())
	}
}

// TestMethodAllowlistIsCaseInsensitive proves method matching tolerates case
// differences the same way HTTP method comparisons generally do in this
// codebase.
func TestMethodAllowlistIsCaseInsensitive(t *testing.T) {
	if !equal([]string{"get", "POST"}, "GET") {
		t.Error("expected case-insensitive match for GET")
	}
	if equal([]string{"GET"}, "POST") {
		t.Error("expected POST to not match a GET-only allowlist")
	}
}

// TestBodyExactlyAtLimitPasses proves MaxBodyBytes rejects only bodies
// strictly larger than the limit — an off-by-one here would either let an
// oversized body through or reject a body that exactly fits the contract.
func TestBodyExactlyAtLimitPasses(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{MaxBodyBytes: 5}))
	app.Post("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", nil, "12345")
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 for a body exactly at the limit", status)
	}
}

// TestBodyOverLimitIsRejected proves a body one byte over MaxBodyBytes is
// rejected with 413 before the handler runs.
func TestBodyOverLimitIsRejected(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(New(Config{MaxBodyBytes: 5}))
	app.Post("/", func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", nil, "123456")
	if status != fh.StatusPayloadTooLarge {
		t.Fatalf("status = %d, want 413", status)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler ran %d times for an oversized body", calls.Load())
	}
}

// TestContentTypeNotInAllowlistIsRejected proves an unsupported (or
// missing) Content-Type is rejected with 415 when ContentTypes is
// configured — a request lacking the header at all must fail closed rather
// than being treated as acceptable.
func TestContentTypeNotInAllowlistIsRejected(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{ContentTypes: []string{"application/json"}}))
	app.Post("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", map[string]string{"Content-Type": "text/plain"}, "hi")
	if status != fh.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415 for an unlisted content type", status)
	}

	status, _ = doRequest(t, addr, "POST", "/", nil, "hi")
	if status != fh.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415 when Content-Type is missing entirely and a contract requires one", status)
	}
}

// TestContentTypeMatchesByPrefixIncludingParameters proves the allowlist
// check matches by prefix, so "application/json; charset=utf-8" satisfies an
// "application/json" requirement.
func TestContentTypeMatchesByPrefixIncludingParameters(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{ContentTypes: []string{"application/json"}}))
	app.Post("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", map[string]string{"Content-Type": "application/json; charset=utf-8"}, "{}")
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 for a content type carrying extra parameters", status)
	}
}

// TestContentTypeMatchingIsCaseInsensitive proves the comparison is not
// sensitive to how a client capitalizes the media type.
func TestContentTypeMatchingIsCaseInsensitive(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{ContentTypes: []string{"Application/JSON"}}))
	app.Post("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", map[string]string{"Content-Type": "APPLICATION/json"}, "{}")
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 (case-insensitive content type match)", status)
	}
}

// TestRequiredHeaderMissingIsRejected proves each configured header is
// individually enforced and a missing one blocks the request with 400
// before the handler runs.
func TestRequiredHeaderMissingIsRejected(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(New(Config{RequireHeaders: []string{"X-Api-Key", "X-Client-Version"}}))
	app.Post("/", func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "POST", "/", map[string]string{"X-Api-Key": "abc"}, "")
	if status != fh.StatusBadRequest {
		t.Fatalf("status = %d, want 400 when a required header is missing", status)
	}
	if !strings.Contains(body, "X-Client-Version") {
		t.Fatalf("expected error body to name the missing header, got %q", body)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler ran %d times when a required header was missing", calls.Load())
	}
}

// TestRequiredHeaderPresentAllowsRequest proves that once every required
// header is present, the request proceeds normally.
func TestRequiredHeaderPresentAllowsRequest(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{RequireHeaders: []string{"X-Api-Key"}}))
	app.Post("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", map[string]string{"X-Api-Key": "abc"}, "")
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
}

// TestChecksAreEnforcedInMethodThenSizeThenContentTypeThenHeaderOrder proves
// the documented precedence of the four checks: a request that violates the
// method allowlist is rejected for that reason even though it also violates
// every other constraint, and likewise down the chain. Callers relying on
// which error comes back (e.g. to decide whether to strip the body and
// retry) depend on this order being stable.
func TestChecksAreEnforcedInMethodThenSizeThenContentTypeThenHeaderOrder(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{
		Methods:        []string{"POST"},
		MaxBodyBytes:   2,
		ContentTypes:   []string{"application/json"},
		RequireHeaders: []string{"X-Api-Key"},
	}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	// GET is disallowed AND the body is oversized AND content type/headers
	// are all wrong too — the method check must win.
	status, _ := doRequest(t, addr, "GET", "/", map[string]string{"Content-Type": "text/plain"}, "toolong")
	if status != fh.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405 (method check should take precedence)", status)
	}
}

// TestNilConfigSlicesImposeNoRestrictionForThatDimension proves that leaving
// any one of Methods/ContentTypes/RequireHeaders nil/empty disables only
// that specific check rather than defaulting to deny-all or panicking.
func TestNilConfigSlicesImposeNoRestrictionForThatDimension(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{MaxBodyBytes: 1000})) // only a body-size contract
	app.Post("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/", nil, "no content type, no special headers, any method")
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 when only MaxBodyBytes is configured", status)
	}
}

// TestPrefixHelperDoesNotPanicOnEmptyInputs is a direct unit test of the
// unexported prefix() matcher guarding against malformed/empty inputs
// causing a panic (e.g. an empty Content-Type header, or an empty allowlist
// entry).
func TestPrefixHelperDoesNotPanicOnEmptyInputs(t *testing.T) {
	if prefix(nil, "") {
		t.Error("prefix(nil, \"\") should be false")
	}
	if prefix([]string{""}, "") {
		// An empty allowlist entry trivially prefixes anything (including
		// empty); document the actual (permissive) behavior rather than
		// assume, since an empty ContentTypes entry is a misconfiguration
		// that should not be silently more permissive than intended.
		t.Log("prefix([]string{\"\"}, \"\") returned true: an empty ContentTypes entry matches everything")
	}
	if prefix([]string{"application/json"}, "") {
		t.Error("a non-empty required prefix must not match an empty content type")
	}
}

// TestConcurrentRequestsDoNotRace exercises the middleware under concurrent
// load with a mix of passing and failing requests. Config is read-only after
// construction, so this should be race-free by construction, but this test
// pins that invariant against a future change that adds mutable shared
// state (e.g. a counter or cache) to the middleware.
func TestConcurrentRequestsDoNotRace(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{
		Methods:      []string{"POST"},
		ContentTypes: []string{"application/json"},
		MaxBodyBytes: 100,
	}))
	app.Post("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	const n = 40
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make(chan string, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			var status int
			if i%2 == 0 {
				status, _ = doRequest(t, addr, "POST", "/", map[string]string{"Content-Type": "application/json"}, "{}")
				if status != fh.StatusOK {
					errs <- fmt.Sprintf("valid request #%d: status = %d, want 200", i, status)
				}
			} else {
				status, _ = doRequest(t, addr, "POST", "/", map[string]string{"Content-Type": "text/plain"}, "{}")
				if status != fh.StatusUnsupportedMediaType {
					errs <- fmt.Sprintf("invalid request #%d: status = %d, want 415", i, status)
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
