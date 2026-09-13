package timeout

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
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

// doRequest issues a raw HTTP/1.1 request and returns the status code, the
// response headers (lower-cased keys), and the body.
func doRequest(t *testing.T, addr, path string) (statusCode int, headers map[string]string, body string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", path)

	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	raw := string(resp)
	head, rest, found := strings.Cut(raw, "\r\n\r\n")
	if found {
		body = rest
	} else {
		head = raw
	}

	lines := strings.Split(head, "\r\n")
	if len(lines) > 0 {
		var proto, status string
		fmt.Sscan(lines[0], &proto, &status)
		fmt.Sscan(status, &statusCode)
	}

	headers = make(map[string]string)
	for _, line := range lines[1:] {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		headers[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
	}
	return
}

// TestFastHandlerPassesThroughUnmodified proves a handler that finishes well
// inside the deadline gets its own response back untouched, with no timeout
// headers added.
func TestFastHandlerPassesThroughUnmodified(t *testing.T) {
	app := fh.New()
	app.Use(New(50 * time.Millisecond))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("fast-ok") })
	addr := testServer(t, app)

	status, headers, body := doRequest(t, addr, "/")
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body != "fast-ok" {
		t.Fatalf("body = %q, want %q", body, "fast-ok")
	}
	if _, ok := headers["x-timeout"]; ok {
		t.Fatalf("did not expect X-Timeout header on a fast response, got %q", headers["x-timeout"])
	}
}

// TestUncooperativeHandlerResponseIsOverriddenAfterDeadline is the key
// fail-closed check for this middleware: a handler that neither checks
// ctx.Done() nor returns ErrTimeout, but simply runs past the deadline and
// then returns its own success response, must NOT have that late response
// delivered to the client. The middleware inspects the deadline context
// after Next() returns and overrides with the timeout response instead.
func TestUncooperativeHandlerResponseIsOverriddenAfterDeadline(t *testing.T) {
	app := fh.New()
	app.Use(New(15 * time.Millisecond))
	app.Get("/", func(c fh.Ctx) error {
		time.Sleep(60 * time.Millisecond) // ignores ctx.Done() entirely
		return c.SendString("late-response-should-not-be-seen")
	})
	addr := testServer(t, app)

	status, headers, body := doRequest(t, addr, "/")
	if status != defaultStatusCode {
		t.Fatalf("status = %d, want %d", status, defaultStatusCode)
	}
	if body != defaultMessage {
		t.Fatalf("body = %q, want default timeout message %q (leaked late handler body)", body, defaultMessage)
	}
	if headers["x-timeout"] != (15 * time.Millisecond).String() {
		t.Fatalf("x-timeout header = %q, want %q", headers["x-timeout"], (15 * time.Millisecond).String())
	}
}

// TestCooperativeHandlerReturnsPromptlyOnDeadline proves a handler that does
// observe ctx.Context().Done() is unblocked at (approximately) the
// configured deadline rather than running for however long it would
// otherwise take, and still gets the timeout response.
func TestCooperativeHandlerReturnsPromptlyOnDeadline(t *testing.T) {
	app := fh.New()
	app.Use(New(15 * time.Millisecond))
	app.Get("/", func(c fh.Ctx) error {
		select {
		case <-c.Context().Done():
			return c.Context().Err()
		case <-time.After(2 * time.Second):
			return c.SendString("should never get here")
		}
	})
	addr := testServer(t, app)

	start := time.Now()
	status, _, body := doRequest(t, addr, "/")
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("handler took %v to return, want it unblocked near the 15ms deadline", elapsed)
	}
	if status != defaultStatusCode {
		t.Fatalf("status = %d, want %d", status, defaultStatusCode)
	}
	if body != defaultMessage {
		t.Fatalf("body = %q, want %q", body, defaultMessage)
	}
}

// TestExplicitErrTimeoutTriggersTimeoutResponseEvenWithoutDeadline proves a
// handler can proactively signal a timeout (e.g. because some internal,
// shorter-lived operation timed out) by returning ErrTimeout, and the
// middleware treats it as a timeout even though the outer deadline itself
// was never exceeded.
func TestExplicitErrTimeoutTriggersTimeoutResponseEvenWithoutDeadline(t *testing.T) {
	app := fh.New()
	app.Use(New(time.Second))
	app.Get("/", func(c fh.Ctx) error { return ErrTimeout })
	addr := testServer(t, app)

	status, headers, body := doRequest(t, addr, "/")
	if status != defaultStatusCode {
		t.Fatalf("status = %d, want %d", status, defaultStatusCode)
	}
	if body != defaultMessage {
		t.Fatalf("body = %q, want %q", body, defaultMessage)
	}
	if headers["x-timeout"] == "" {
		t.Fatal("expected X-Timeout header to be set")
	}
}

// TestRejectInvalidTimeoutShortCircuitsWithoutCallingHandler proves that
// RejectInvalidTimeout with a non-positive Timeout rejects every request
// with 500 and never invokes the downstream handler at all.
func TestRejectInvalidTimeoutShortCircuitsWithoutCallingHandler(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(NewWithConfig(Config{RejectInvalidTimeout: true}))
	app.Get("/", func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	status, _, body := doRequest(t, addr, "/")
	if status != 500 {
		t.Fatalf("status = %d, want 500", status)
	}
	if body != ErrInvalidTimeout.Error() {
		t.Fatalf("body = %q, want %q", body, ErrInvalidTimeout.Error())
	}
	if calls.Load() != 0 {
		t.Fatalf("downstream handler was invoked %d times, want 0", calls.Load())
	}
}

// TestSkipperBypassesTimeoutEntirely proves a request matched by Skipper
// runs with no deadline enforcement at all, even if it takes far longer
// than the configured timeout.
func TestSkipperBypassesTimeoutEntirely(t *testing.T) {
	app := fh.New()
	app.Use(NewWithConfig(Config{
		Timeout: 10 * time.Millisecond,
		Skipper: func(c fh.Ctx) bool { return c.Path() == "/skip" },
	}))
	app.Get("/skip", func(c fh.Ctx) error {
		time.Sleep(40 * time.Millisecond)
		return c.SendString("skip-ok")
	})
	app.Get("/track", func(c fh.Ctx) error {
		time.Sleep(40 * time.Millisecond)
		return c.SendString("track-ok")
	})
	addr := testServer(t, app)

	if status, _, body := doRequest(t, addr, "/skip"); status != fh.StatusOK || body != "skip-ok" {
		t.Fatalf("skipped route: status=%d body=%q, want 200 skip-ok", status, body)
	}
	if status, _, body := doRequest(t, addr, "/track"); status != defaultStatusCode || body != defaultMessage {
		t.Fatalf("tracked route: status=%d body=%q, want timeout response", status, body)
	}
}

// TestOnErrorTransformsNonTimeoutError proves a non-timeout error returned
// by the handler is routed through OnError, not silently swallowed or
// treated as a timeout.
func TestOnErrorTransformsNonTimeoutError(t *testing.T) {
	boom := errors.New("boom")
	app := fh.New()
	app.Use(NewWithConfig(Config{
		Timeout: 50 * time.Millisecond,
		OnError: func(c fh.Ctx, err error) error {
			return c.Status(fh.StatusTeapot).SendString("handled:" + err.Error())
		},
	}))
	app.Get("/", func(c fh.Ctx) error { return boom })
	addr := testServer(t, app)

	status, _, body := doRequest(t, addr, "/")
	if status != fh.StatusTeapot {
		t.Fatalf("status = %d, want %d", status, fh.StatusTeapot)
	}
	if body != "handled:boom" {
		t.Fatalf("body = %q, want %q", body, "handled:boom")
	}
}

// TestCustomOnTimeoutHandlerOverridesDefaultResponseForCooperativeHandler
// proves a configured OnTimeout callback is used instead of the built-in
// default response when the handler cooperatively observes ctx.Done() and
// returns without writing its own response.
func TestCustomOnTimeoutHandlerOverridesDefaultResponseForCooperativeHandler(t *testing.T) {
	app := fh.New()
	app.Use(NewWithConfig(Config{
		Timeout: 10 * time.Millisecond,
		OnTimeout: func(c fh.Ctx, err error) error {
			return c.Status(fh.StatusGatewayTimeout).JSONString(`{"timed_out":true}`)
		},
	}))
	app.Get("/", func(c fh.Ctx) error {
		<-c.Context().Done()
		return c.Context().Err()
	})
	addr := testServer(t, app)

	status, _, body := doRequest(t, addr, "/")
	if status != fh.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", status, fh.StatusGatewayTimeout)
	}
	if !strings.Contains(body, `"timed_out":true`) {
		t.Fatalf("body = %q, want it to contain timed_out:true", body)
	}
}

// TestCustomOnTimeoutIsNotInvokedForUncooperativeHandler documents a
// deliberate limitation of the fail-closed fix for uncooperative handlers
// (see AddBodyTransform usage in NewWithConfig): since intercepting a
// response that is already mid-write cannot safely re-enter arbitrary
// caller-supplied OnTimeout logic (it may itself call Send*/JSON, which
// would recurse into the write path), an uncooperative handler still gets
// the plain default status/message/header from Config instead of a custom
// OnTimeout's response. The important guarantee -- the handler's own late
// body is never leaked -- still holds.
func TestCustomOnTimeoutIsNotInvokedForUncooperativeHandler(t *testing.T) {
	app := fh.New()
	app.Use(NewWithConfig(Config{
		Timeout: 10 * time.Millisecond,
		OnTimeout: func(c fh.Ctx, err error) error {
			return c.Status(fh.StatusGatewayTimeout).JSONString(`{"timed_out":true}`)
		},
	}))
	app.Get("/", func(c fh.Ctx) error {
		time.Sleep(40 * time.Millisecond)
		return c.SendString("late-response-should-not-be-seen")
	})
	addr := testServer(t, app)

	status, _, body := doRequest(t, addr, "/")
	if status != defaultStatusCode {
		t.Fatalf("status = %d, want default %d", status, defaultStatusCode)
	}
	if body != defaultMessage {
		t.Fatalf("body = %q, want default message %q", body, defaultMessage)
	}
	if strings.Contains(body, "late-response") {
		t.Fatal("leaked the uncooperative handler's own late response body")
	}
}

// TestParentContextRestoredAfterRequest proves that once the timeout
// middleware's handler returns, an outer/enclosing middleware sees the
// original (non-deadline) context again rather than the expired deadline
// context leaking past this middleware's own scope.
func TestParentContextRestoredAfterRequest(t *testing.T) {
	var leaked int32
	app := fh.New()
	app.Use(func(c fh.Ctx) error {
		err := c.Next()
		if _, hasDeadline := c.Context().Deadline(); hasDeadline {
			atomic.StoreInt32(&leaked, 1)
		}
		return err
	})
	app.Use(New(15 * time.Millisecond))
	app.Get("/", func(c fh.Ctx) error {
		time.Sleep(30 * time.Millisecond)
		return c.SendString("late")
	})
	addr := testServer(t, app)

	doRequest(t, addr, "/")
	if atomic.LoadInt32(&leaked) != 0 {
		t.Fatal("deadline context leaked past the timeout middleware's own scope")
	}
}

// TestConcurrentRequestsEnforceIndependentDeadlines is the concurrency
// stress test: many concurrent requests share the same middleware instance,
// some finishing well inside the timeout and some deliberately running
// past it. Every request must be classified purely by its own duration —
// with no cross-request interference — and the whole thing must be race
// free.
func TestConcurrentRequestsEnforceIndependentDeadlines(t *testing.T) {
	const timeout = 20 * time.Millisecond
	app := fh.New()
	app.Use(New(timeout))
	app.Get("/work", func(c fh.Ctx) error {
		ms, _ := strconv.Atoi(c.Query("ms"))
		time.Sleep(time.Duration(ms) * time.Millisecond)
		return c.SendString("done")
	})
	addr := testServer(t, app)

	const n = 40
	type result struct {
		fast   bool
		status int
		body   string
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		fast := i%2 == 0
		ms := 3
		if !fast {
			ms = 60
		}
		go func(i, ms int, fast bool) {
			defer wg.Done()
			status, _, body := doRequest(t, addr, fmt.Sprintf("/work?ms=%d", ms))
			results[i] = result{fast: fast, status: status, body: body}
		}(i, ms, fast)
	}
	wg.Wait()

	for i, r := range results {
		if r.fast {
			if r.status != fh.StatusOK || r.body != "done" {
				t.Errorf("request %d: fast request got status=%d body=%q, want 200 done", i, r.status, r.body)
			}
		} else {
			if r.status != defaultStatusCode || r.body != defaultMessage {
				t.Errorf("request %d: slow request got status=%d body=%q, want timeout response", i, r.status, r.body)
			}
		}
	}
}

// TestNormalizeAppliesDefaultsWhenUnset covers the pure defaulting logic
// directly: an empty Config must end up with the documented defaults.
func TestNormalizeAppliesDefaultsWhenUnset(t *testing.T) {
	cfg := normalize(Config{})
	if cfg.Timeout != defaultTimeout {
		t.Errorf("Timeout = %v, want %v", cfg.Timeout, defaultTimeout)
	}
	if cfg.StatusCode != defaultStatusCode {
		t.Errorf("StatusCode = %d, want %d", cfg.StatusCode, defaultStatusCode)
	}
	if cfg.Message != defaultMessage {
		t.Errorf("Message = %q, want %q", cfg.Message, defaultMessage)
	}
	if cfg.HeaderName != "X-Timeout" {
		t.Errorf("HeaderName = %q, want X-Timeout", cfg.HeaderName)
	}
	if cfg.HeaderValue != defaultTimeout.String() {
		t.Errorf("HeaderValue = %q, want %q", cfg.HeaderValue, defaultTimeout.String())
	}
	if !cfg.PreserveContext {
		t.Error("PreserveContext = false, want true (documented production default)")
	}
}

// TestNormalizePreservesInvalidTimeoutWhenRejectConfigured proves normalize
// does not silently "fix up" Timeout<=0 into the default duration when
// RejectInvalidTimeout is set, since NewWithConfig relies on Timeout still
// being <=0 after normalize to decide whether to reject every request.
func TestNormalizePreservesInvalidTimeoutWhenRejectConfigured(t *testing.T) {
	cfg := normalize(Config{RejectInvalidTimeout: true})
	if cfg.Timeout > 0 {
		t.Fatalf("Timeout = %v, want it left <= 0 so the reject path still triggers", cfg.Timeout)
	}
}

// TestNormalizeForcesPreserveContextTrue documents the deliberate,
// production-safety behavior noted in Config.PreserveContext's doc comment:
// even an explicit PreserveContext:false is forced back to true, since
// letting the deadline context leak past this middleware is considered
// unsafe by default.
func TestNormalizeForcesPreserveContextTrue(t *testing.T) {
	cfg := normalize(Config{PreserveContext: false})
	if !cfg.PreserveContext {
		t.Fatal("PreserveContext = false, want normalize to force it back to true")
	}
}

// TestIsTimedOutRecognizesAllTimeoutSignals is a focused unit test of the
// classification logic that decides whether a response should be replaced
// with the timeout response.
func TestIsTimedOutRecognizesAllTimeoutSignals(t *testing.T) {
	deadlineCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	<-deadlineCtx.Done()

	liveCtx, cancel2 := context.WithTimeout(context.Background(), time.Hour)
	defer cancel2()

	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"nil ctx, nil err", nil, nil, false},
		{"expired deadline ctx", deadlineCtx, nil, true},
		{"live ctx, unrelated err", liveCtx, errors.New("other"), false},
		{"live ctx, ErrTimeout", liveCtx, ErrTimeout, true},
		{"live ctx, wrapped DeadlineExceeded", liveCtx, fmt.Errorf("wrap: %w", context.DeadlineExceeded), true},
	}
	for _, tc := range cases {
		if got := isTimedOut(tc.ctx, tc.err); got != tc.want {
			t.Errorf("%s: isTimedOut() = %v, want %v", tc.name, got, tc.want)
		}
	}
}
