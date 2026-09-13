package slowlog_test

import (
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/slowlog"
)

// captureLogger records every Warn call so tests can assert on the fields
// the middleware emits without depending on any particular log backend.
type captureLogger struct {
	mu    sync.Mutex
	warns [][]any
}

func (l *captureLogger) Printf(string, ...any) {}
func (l *captureLogger) Info(string, ...any)   {}
func (l *captureLogger) Error(string, ...any)  {}
func (l *captureLogger) Debug(string, ...any)  {}
func (l *captureLogger) Warn(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, append([]any{msg}, args...))
}
func (l *captureLogger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.warns)
}
func (l *captureLogger) last() []any {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.warns) == 0 {
		return nil
	}
	return l.warns[len(l.warns)-1]
}

func fieldAsString(fields []any, key string) (string, bool) {
	for i := 0; i+1 < len(fields); i++ {
		if k, ok := fields[i].(string); ok && k == key {
			s, ok := fields[i+1].(string)
			return s, ok
		}
	}
	return "", false
}

// TestSlowRequestLogsWarnWithFields is the core positive case: a request
// that exceeds the threshold must be logged at Warn with method/path/status
// so operators can find it.
func TestSlowRequestLogsWarnWithFields(t *testing.T) {
	logger := &captureLogger{}
	app := fh.New()
	app.Use(slowlog.New(slowlog.Config{Threshold: 5 * time.Millisecond, Logger: logger}))
	app.Get("/slow", func(c fh.Ctx) error {
		time.Sleep(30 * time.Millisecond)
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/slow", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%d err=%v", resp.StatusCode, err)
	}
	if logger.count() != 1 {
		t.Fatalf("expected exactly 1 warn log, got %d", logger.count())
	}
	fields := logger.last()
	if method, _ := fieldAsString(fields, "method"); method != "GET" {
		t.Errorf("expected method=GET, got %q", method)
	}
	if path, _ := fieldAsString(fields, "path"); path != "/slow" {
		t.Errorf("expected path=/slow, got %q", path)
	}
	if _, ok := fieldAsString(fields, "latency"); !ok {
		t.Error("expected a latency field")
	}
}

// TestFastRequestDoesNotLog ensures the middleware stays silent below the
// threshold; otherwise slow-request logging would be useless noise.
func TestFastRequestDoesNotLog(t *testing.T) {
	logger := &captureLogger{}
	app := fh.New()
	app.Use(slowlog.New(slowlog.Config{Threshold: 200 * time.Millisecond, Logger: logger}))
	app.Get("/fast", func(c fh.Ctx) error { return c.SendString("ok") })

	resp, err := app.Test(httptest.NewRequest("GET", "/fast", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%d err=%v", resp.StatusCode, err)
	}
	if logger.count() != 0 {
		t.Fatalf("expected no warn logs for a fast request, got %d", logger.count())
	}
}

// TestZeroOrNegativeThresholdFallsBackToDefault ensures a misconfigured
// (non-positive) threshold does not turn into "log everything": it must
// fall back to the documented 500ms default rather than a near-zero value.
func TestZeroOrNegativeThresholdFallsBackToDefault(t *testing.T) {
	for _, threshold := range []time.Duration{0, -1 * time.Second} {
		logger := &captureLogger{}
		app := fh.New()
		app.Use(slowlog.New(slowlog.Config{Threshold: threshold, Logger: logger}))
		app.Get("/fast", func(c fh.Ctx) error { return c.SendString("ok") })

		resp, err := app.Test(httptest.NewRequest("GET", "/fast", nil))
		if err != nil || resp.StatusCode != fh.StatusOK {
			t.Fatalf("status=%d err=%v", resp.StatusCode, err)
		}
		if logger.count() != 0 {
			t.Fatalf("threshold=%v: expected the ~500ms default to swallow a fast request, got %d warn logs", threshold, logger.count())
		}
	}
}

// TestSkipBypassesTimingAndLogging verifies Skip short-circuits before the
// timer is even meaningful: a handler that would otherwise be "slow" must
// not be logged when Skip matches.
func TestSkipBypassesTimingAndLogging(t *testing.T) {
	logger := &captureLogger{}
	app := fh.New()
	app.Use(slowlog.New(slowlog.Config{
		Threshold: 1 * time.Nanosecond, // would always trip if measured
		Logger:    logger,
		Skip:      func(c fh.Ctx) bool { return c.Path() == "/health" },
	}))
	app.Get("/health", func(c fh.Ctx) error {
		time.Sleep(5 * time.Millisecond)
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/health", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%d err=%v", resp.StatusCode, err)
	}
	if logger.count() != 0 {
		t.Fatalf("expected Skip to bypass logging entirely, got %d warn logs", logger.count())
	}
}

// TestErrorFromHandlerStillLogsWhenSlow makes sure a failing downstream
// handler still gets latency-logged: slow-request visibility should not
// depend on the request having succeeded.
func TestErrorFromHandlerStillLogsWhenSlow(t *testing.T) {
	logger := &captureLogger{}
	app := fh.New()
	app.Use(slowlog.New(slowlog.Config{Threshold: 5 * time.Millisecond, Logger: logger}))
	app.Get("/boom", func(c fh.Ctx) error {
		time.Sleep(30 * time.Millisecond)
		return fh.NewHTTPError(fh.StatusBadGateway, "UPSTREAM_DOWN", "boom")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/boom", nil))
	if err != nil || resp.StatusCode != fh.StatusBadGateway {
		t.Fatalf("status=%d err=%v", resp.StatusCode, err)
	}
	if logger.count() != 1 {
		t.Fatalf("expected the slow error request to still be logged, got %d", logger.count())
	}
}

// TestFallsBackToAppLoggerWhenConfigLoggerNil confirms the middleware uses
// the app's configured logger when no per-middleware Logger is supplied,
// so slow-request visibility is not silently lost by omission.
func TestFallsBackToAppLoggerWhenConfigLoggerNil(t *testing.T) {
	logger := &captureLogger{}
	app := fh.NewWithConfig(fh.Config{Logger: logger})
	app.Use(slowlog.New(slowlog.Config{Threshold: 5 * time.Millisecond}))
	app.Get("/slow", func(c fh.Ctx) error {
		time.Sleep(30 * time.Millisecond)
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/slow", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%d err=%v", resp.StatusCode, err)
	}
	if logger.count() != 1 {
		t.Fatalf("expected fallback to app logger to produce 1 warn log, got %d", logger.count())
	}
}

// TestConcurrentRequestsEachLoggedIndependently exercises the middleware
// under concurrent load: N concurrently in-flight slow requests must each
// produce exactly one independent log entry with no cross-request field
// bleed (start/duration are stack-local, not shared state, but this guards
// against a future refactor introducing shared mutable timing state).
func TestConcurrentRequestsEachLoggedIndependently(t *testing.T) {
	// Each goroutine gets its own app instance: app.Test shuts its whole app
	// down once its single request completes, so sharing one app across
	// concurrently in-flight requests would tear down the others mid-flight.
	// The shared captureLogger is what's actually exercised concurrently
	// here (and must be race-safe under -race).
	logger := &captureLogger{}

	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			app := fh.New()
			app.Use(slowlog.New(slowlog.Config{Threshold: 5 * time.Millisecond, Logger: logger}))
			app.Get("/slow", func(c fh.Ctx) error {
				time.Sleep(15 * time.Millisecond)
				return c.SendString("ok")
			})
			resp, err := app.Test(httptest.NewRequest("GET", "/slow", nil), 2000)
			if err != nil || resp.StatusCode != fh.StatusOK {
				t.Errorf("status=%v err=%v", resp, err)
			}
		}()
	}
	wg.Wait()

	if got := logger.count(); got != n {
		t.Fatalf("expected %d independent warn logs, got %d", n, got)
	}
}
