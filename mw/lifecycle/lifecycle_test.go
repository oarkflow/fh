package lifecycle

import (
	"errors"
	"fmt"
	"net"
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

func get(t *testing.T, addr, path string) int {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", path)
	buf := make([]byte, 4096)
	n, _ := conn.Read(buf)
	var proto, status string
	fmt.Sscan(string(buf[:n]), &proto, &status)
	code := 0
	fmt.Sscan(status, &code)
	return code
}

// waitDone blocks until done is closed or fails the test after a timeout.
// The HTTP response can reach the client before the server's connection
// goroutine finishes unwinding the deferred OnRequestEnd hook (this
// middleware writes the response as soon as a handler calls SendString,
// independent of when the surrounding hook chain finishes), so tests must
// not treat "the socket read returned" as proof that all hooks ran; they
// synchronize on a channel closed by the last hook instead.
func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the request's lifecycle hooks to finish")
	}
}

// TestHookOrderRunsStartBeforeHandlerBeforeAfterBeforeEnd proves the
// documented ordering: OnRequestStart, then the route handler (via Next),
// then OnAfterHandler, then OnRequestEnd last, on the success path.
func TestHookOrderRunsStartBeforeHandlerBeforeAfterBeforeEnd(t *testing.T) {
	var order []string
	done := make(chan struct{})
	app := fh.New()
	app.Use(New(Hooks{
		OnRequestStart: func(c fh.Ctx) error { order = append(order, "start"); return nil },
		OnAfterHandler: func(c fh.Ctx) error { order = append(order, "after"); return nil },
		OnRequestEnd: func(c fh.Ctx, err error) error {
			order = append(order, "end")
			close(done)
			return nil
		},
	}))
	app.Get("/", func(c fh.Ctx) error {
		order = append(order, "handler")
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	if code := get(t, addr, "/"); code != fh.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	waitDone(t, done)

	want := []string{"start", "handler", "after", "end"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

// TestOnRequestStartErrorPreventsHandlerFromRunning is a fail-closed
// invariant: if OnRequestStart returns an error, the route handler must
// never execute at all.
func TestOnRequestStartErrorPreventsHandlerFromRunning(t *testing.T) {
	handlerRan := false
	done := make(chan struct{})
	sentinel := errors.New("start failed")
	app := fh.New()
	app.Use(New(Hooks{
		OnRequestStart: func(c fh.Ctx) error { return sentinel },
		OnRequestEnd:   func(c fh.Ctx, err error) error { close(done); return nil },
	}))
	app.Get("/", func(c fh.Ctx) error {
		handlerRan = true
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	get(t, addr, "/")
	waitDone(t, done)
	if handlerRan {
		t.Fatal("handler ran despite OnRequestStart returning an error")
	}
}

// TestOnBeforeHandlerErrorPreventsHandlerFromRunning mirrors the same
// fail-closed invariant for OnBeforeHandler.
func TestOnBeforeHandlerErrorPreventsHandlerFromRunning(t *testing.T) {
	handlerRan := false
	done := make(chan struct{})
	app := fh.New()
	app.Use(New(Hooks{
		OnBeforeHandler: func(c fh.Ctx) error { return errors.New("before failed") },
		OnRequestEnd:    func(c fh.Ctx, err error) error { close(done); return nil },
	}))
	app.Get("/", func(c fh.Ctx) error {
		handlerRan = true
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	get(t, addr, "/")
	waitDone(t, done)
	if handlerRan {
		t.Fatal("handler ran despite OnBeforeHandler returning an error")
	}
}

// TestOnRequestEndAlwaysRunsEvenOnEarlyFailure proves OnRequestEnd is a true
// "finally" — it must still run (via defer) even when OnRequestStart failed
// before the handler ever got a chance to execute.
func TestOnRequestEndAlwaysRunsEvenOnEarlyFailure(t *testing.T) {
	endRan := false
	var endErr error
	done := make(chan struct{})
	app := fh.New()
	app.Use(New(Hooks{
		OnRequestStart: func(c fh.Ctx) error { return errors.New("boom") },
		OnRequestEnd: func(c fh.Ctx, err error) error {
			endRan = true
			endErr = err
			close(done)
			return nil
		},
	}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	get(t, addr, "/")
	waitDone(t, done)
	if !endRan {
		t.Fatal("OnRequestEnd did not run after an early OnRequestStart failure")
	}
	if endErr == nil || endErr.Error() != "boom" {
		t.Fatalf("OnRequestEnd received err = %v, want the OnRequestStart error", endErr)
	}
}

// TestOnAfterHandlerSkippedByDefaultWhenHandlerErrors proves the default
// (RunAfterOnError: false) behavior: when the wrapped handler returns an
// error, OnAfterHandler does not run.
func TestOnAfterHandlerSkippedByDefaultWhenHandlerErrors(t *testing.T) {
	afterRan := false
	done := make(chan struct{})
	app := fh.New()
	app.Use(New(Hooks{
		OnAfterHandler: func(c fh.Ctx) error { afterRan = true; return nil },
		OnRequestEnd:   func(c fh.Ctx, err error) error { close(done); return nil },
	}))
	app.Get("/", func(c fh.Ctx) error { return errors.New("handler failed") })
	addr := testServer(t, app)

	get(t, addr, "/")
	waitDone(t, done)
	if afterRan {
		t.Fatal("OnAfterHandler ran even though RunAfterOnError is false and the handler errored")
	}
}

// TestRunAfterOnErrorTrueRunsAfterHandlerDespiteError proves the opt-in
// RunAfterOnError flag makes OnAfterHandler run even when the handler
// errored (e.g. for cleanup that must happen regardless of outcome).
func TestRunAfterOnErrorTrueRunsAfterHandlerDespiteError(t *testing.T) {
	afterRan := false
	done := make(chan struct{})
	app := fh.New()
	app.Use(NewWithConfig(Config{
		RunAfterOnError: true,
		Hooks: Hooks{
			OnAfterHandler: func(c fh.Ctx) error { afterRan = true; return nil },
			OnRequestEnd:   func(c fh.Ctx, err error) error { close(done); return nil },
		},
	}))
	app.Get("/", func(c fh.Ctx) error { return errors.New("handler failed") })
	addr := testServer(t, app)

	get(t, addr, "/")
	waitDone(t, done)
	if !afterRan {
		t.Fatal("OnAfterHandler did not run despite RunAfterOnError being true")
	}
}

// TestOnErrorCanSwallowErrorWhenConfigured proves SwallowErrorOnHandled
// lets an OnError hook that returns nil suppress the original handler
// error, resulting in a successful response.
func TestOnErrorCanSwallowErrorWhenConfigured(t *testing.T) {
	var seenErr error
	done := make(chan struct{})
	app := fh.New()
	app.Use(NewWithConfig(Config{
		SwallowErrorOnHandled: true,
		Hooks: Hooks{
			OnError: func(c fh.Ctx, err error) error {
				seenErr = err
				c.Status(fh.StatusOK)
				return nil
			},
			OnRequestEnd: func(c fh.Ctx, err error) error { close(done); return nil },
		},
	}))
	app.Get("/", func(c fh.Ctx) error { return errors.New("handled elsewhere") })
	addr := testServer(t, app)

	code := get(t, addr, "/")
	waitDone(t, done)
	if seenErr == nil || seenErr.Error() != "handled elsewhere" {
		t.Fatalf("OnError received %v, want the handler's error", seenErr)
	}
	if code != fh.StatusOK {
		t.Fatalf("status = %d, want 200 once the error was swallowed", code)
	}
}

// TestOnErrorReturningNonNilIsJoinedWithOriginalError proves that when
// OnError itself fails, the returned error carries both the original
// handler error and the OnError failure (via errors.Join / errors.Is), not
// just one or the other.
func TestOnErrorReturningNonNilIsJoinedWithOriginalError(t *testing.T) {
	handlerErr := errors.New("handler error")
	onErrErr := errors.New("onerror error")
	var finalErr error
	done := make(chan struct{})
	app := fh.New()
	app.Use(NewWithConfig(Config{
		Hooks: Hooks{
			OnError: func(c fh.Ctx, err error) error { return onErrErr },
			OnRequestEnd: func(c fh.Ctx, err error) error {
				finalErr = err
				close(done)
				return nil
			},
		},
	}))
	app.Get("/", func(c fh.Ctx) error { return handlerErr })
	addr := testServer(t, app)

	get(t, addr, "/")
	waitDone(t, done)
	if !errors.Is(finalErr, handlerErr) {
		t.Fatalf("final error %v does not wrap the original handler error", finalErr)
	}
	if !errors.Is(finalErr, onErrErr) {
		t.Fatalf("final error %v does not wrap the OnError error", finalErr)
	}
}

// TestOnAfterHandlerErrorIsJoinedIntoResult proves an OnAfterHandler failure
// on the success path is not silently dropped — it is joined into the
// returned/observed error.
func TestOnAfterHandlerErrorIsJoinedIntoResult(t *testing.T) {
	afterErr := errors.New("after failed")
	var finalErr error
	done := make(chan struct{})
	app := fh.New()
	app.Use(NewWithConfig(Config{
		Hooks: Hooks{
			OnAfterHandler: func(c fh.Ctx) error { return afterErr },
			OnRequestEnd: func(c fh.Ctx, err error) error {
				finalErr = err
				close(done)
				return nil
			},
		},
	}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	get(t, addr, "/")
	waitDone(t, done)
	if !errors.Is(finalErr, afterErr) {
		t.Fatalf("final error %v does not wrap the OnAfterHandler error", finalErr)
	}
}

// TestOnRequestEndErrorIsJoinedWithHandlerError proves an OnRequestEnd
// failure is joined onto whatever error already existed rather than
// replacing it, so no information about the primary failure is lost.
func TestOnRequestEndErrorIsJoinedWithHandlerError(t *testing.T) {
	handlerErr := errors.New("handler failed")
	endErr := errors.New("end failed")
	h := NewWithConfig(Config{
		Hooks: Hooks{
			OnRequestEnd: func(c fh.Ctx, err error) error { return endErr },
		},
	})
	// Call the handler directly to inspect the returned error value, since
	// the HTTP layer only exposes a status code.
	got := callDirect(t, h, handlerErr)
	if !errors.Is(got, handlerErr) || !errors.Is(got, endErr) {
		t.Fatalf("returned error %v must wrap both the handler error and the OnRequestEnd error", got)
	}
}

// callDirect invokes the lifecycle-wrapped handler in isolation against a
// real request/response cycle, capturing the error the middleware itself
// returns (rather than observing only the resulting status code). The
// capture happens synchronously in the same call frame that invokes mw, so
// no extra synchronization is needed to read captured afterward.
func callDirect(t *testing.T, mw fh.HandlerFunc, handlerErr error) error {
	t.Helper()
	var captured error
	done := make(chan struct{})
	app := fh.New()
	app.Use(func(c fh.Ctx) error {
		captured = mw(c)
		close(done)
		return nil
	})
	app.Get("/probe", func(c fh.Ctx) error { return handlerErr })
	addr := testServer(t, app)
	get(t, addr, "/probe")
	waitDone(t, done)
	return captured
}

// TestNoHooksConfiguredIsAPureNoOp proves an empty Hooks value changes
// nothing about the handler's normal execution or error propagation.
func TestNoHooksConfiguredIsAPureNoOp(t *testing.T) {
	app := fh.New()
	app.Use(New(Hooks{}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	if code := get(t, addr, "/"); code != fh.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
}

// TestJoinErrHelperDirectly is a focused unit test of the unexported
// joinErr combinator's three cases (nil/nil, one nil, both set).
func TestJoinErrHelperDirectly(t *testing.T) {
	a := errors.New("a")
	b := errors.New("b")

	if got := joinErr(nil, nil); got != nil {
		t.Errorf("joinErr(nil, nil) = %v, want nil", got)
	}
	if got := joinErr(a, nil); got != a {
		t.Errorf("joinErr(a, nil) = %v, want a", got)
	}
	if got := joinErr(nil, b); got != b {
		t.Errorf("joinErr(nil, b) = %v, want b", got)
	}
	got := joinErr(a, b)
	if !errors.Is(got, a) || !errors.Is(got, b) {
		t.Errorf("joinErr(a, b) = %v, want a value wrapping both a and b", got)
	}
}
