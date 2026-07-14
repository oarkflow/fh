package workflow

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
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

func doRequest(t *testing.T, addr, path string) (int, string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", path)
	conn.Write([]byte(req))
	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(resp), "\r\n", 2)
	var proto, status string
	fmt.Sscan(parts[0], &proto, &status)
	var code int
	fmt.Sscan(status, &code)
	return code, string(resp)
}

func run(t *testing.T, wf *Workflow) (int, string) {
	t.Helper()
	app := fh.New()
	app.Get("/", wf.Handler())
	addr := testServer(t, app)
	return doRequest(t, addr, "/")
}

func TestSequentialSteps(t *testing.T) {
	var order []string

	wf := New("seq").
		Use("a", func(c fh.Ctx) error { order = append(order, "a"); return nil }).
		Use("b", func(c fh.Ctx) error { order = append(order, "b"); return c.SendString("ok") })

	code, _ := run(t, wf)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("unexpected order: %v", order)
	}
}

func TestStepConditionSkipsStep(t *testing.T) {
	var ran atomic.Bool

	wf := New("cond").Use("skip-me", func(c fh.Ctx) error {
		ran.Store(true)
		return nil
	}, func(c fh.Ctx) bool { return false })

	run(t, wf)
	if ran.Load() {
		t.Fatal("step should have been skipped")
	}
}

func TestRetrySucceedsAfterFailures(t *testing.T) {
	var attempts atomic.Int32

	wf := New("retry").UseWithOptions("flaky", func(c fh.Ctx) error {
		n := attempts.Add(1)
		if n < 3 {
			return errors.New("transient")
		}
		return c.SendString("ok")
	}, WithRetry(2, time.Millisecond))

	code, _ := run(t, wf)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestRetryExhaustedReturnsError(t *testing.T) {
	var attempts atomic.Int32

	wf := New("retry-fail").UseWithOptions("flaky", func(c fh.Ctx) error {
		attempts.Add(1)
		return errors.New("always fails")
	}, WithRetry(2, time.Millisecond))

	code, _ := run(t, wf)
	if code == 200 {
		t.Fatalf("expected non-200, got %d", code)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestStepTimeout(t *testing.T) {
	wf := New("timeout").UseWithOptions("slow", func(c fh.Ctx) error {
		<-c.Context().Done()
		return c.Context().Err()
	}, WithTimeout(10*time.Millisecond))

	code, _ := run(t, wf)
	if code == 200 {
		t.Fatalf("expected timeout error status, got %d", code)
	}
}

func TestPanicIsConvertedToError(t *testing.T) {
	wf := New("panic").Use("boom", func(c fh.Ctx) error {
		panic("kaboom")
	})

	code, _ := run(t, wf)
	if code == 200 {
		t.Fatalf("expected error status from recovered panic, got %d", code)
	}
}

func TestBranchRunsFirstMatch(t *testing.T) {
	var ran atomic.Value
	ran.Store("")

	yes := New("yes").Condition(func(c fh.Ctx) bool { return true }).
		Use("yes-step", func(c fh.Ctx) error { ran.Store("yes"); return c.SendString("ok") })
	no := New("no").Condition(func(c fh.Ctx) bool { return false }).
		Use("no-step", func(c fh.Ctx) error { ran.Store("no"); return c.SendString("ok") })

	wf := New("branch").Branch("choice", no, yes)

	run(t, wf)
	if ran.Load().(string) != "yes" {
		t.Fatalf("expected yes branch, got %q", ran.Load())
	}
}

func TestParallelFailFast(t *testing.T) {
	ok := New("ok").Use("ok-step", func(c fh.Ctx) error { return nil })
	bad := New("bad").Use("bad-step", func(c fh.Ctx) error { return errors.New("boom") })

	wf := New("parallel").Parallel("fan-out", ok, bad)

	code, _ := run(t, wf)
	if code == 200 {
		t.Fatalf("expected error from failing branch, got %d", code)
	}
}

func TestParallelJoinCollectsAllErrors(t *testing.T) {
	bad1 := New("bad1").Use("s", func(c fh.Ctx) error { return errors.New("err1") })
	bad2 := New("bad2").Use("s", func(c fh.Ctx) error { return errors.New("err2") })

	wf := New("parallel-join").ParallelJoin("fan-out", bad1, bad2)

	code, _ := run(t, wf)
	if code == 200 {
		t.Fatalf("expected joined error status, got %d", code)
	}
}

func TestOnErrorCompensationContinues(t *testing.T) {
	var compensated atomic.Value
	compensated.Store("")
	var ranNext atomic.Bool

	wf := New("compensate").
		OnError(func(step string, err error) error {
			compensated.Store(step)
			return nil // swallow and continue
		}).
		Use("fails", func(c fh.Ctx) error { return errors.New("boom") }).
		Use("next", func(c fh.Ctx) error { ranNext.Store(true); return c.SendString("ok") })

	code, _ := run(t, wf)
	if code != 200 {
		t.Fatalf("expected 200 after compensation, got %d", code)
	}
	if compensated.Load().(string) != "fails" {
		t.Fatalf("expected compensation for 'fails', got %q", compensated.Load())
	}
	if !ranNext.Load() {
		t.Fatal("expected workflow to continue after compensation")
	}
}

func TestObservabilityHooks(t *testing.T) {
	var started, completed atomic.Int32
	var finished atomic.Bool

	wf := New("observed").
		OnStepStart(func(step string) { started.Add(1) }).
		OnStepComplete(func(step string, err error, dur time.Duration) { completed.Add(1) }).
		OnComplete(func(err error, dur time.Duration) { finished.Store(true) }).
		Use("a", func(c fh.Ctx) error { return nil }).
		Use("b", func(c fh.Ctx) error { return c.SendString("ok") })

	code, _ := run(t, wf)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if started.Load() != 2 || completed.Load() != 2 {
		t.Fatalf("expected 2 steps observed, got started=%d completed=%d", started.Load(), completed.Load())
	}
	if !finished.Load() {
		t.Fatal("expected OnComplete to fire")
	}
}
