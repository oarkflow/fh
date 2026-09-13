package audit_test

import (
	"context"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/audit"
)

// memSink is a minimal fh.AuditSink that records every event in memory so
// tests can assert on exactly what was written, without touching disk.
type memSink struct {
	mu     sync.Mutex
	events []fh.AuditEvent
}

func (s *memSink) WriteAudit(_ context.Context, e fh.AuditEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *memSink) all() []fh.AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]fh.AuditEvent, len(s.events))
	copy(out, s.events)
	return out
}

func newApp(sink fh.AuditSink) *fh.App {
	return fh.NewWithConfig(fh.Config{Audit: fh.AuditConfig{Enabled: true, Sink: sink}})
}

// TestDefaultActionAppliedWhenEmpty ensures an unconfigured Action still
// produces a labeled audit record instead of an empty/unusable action name.
func TestDefaultActionAppliedWhenEmpty(t *testing.T) {
	sink := &memSink{}
	app := newApp(sink)
	app.Use(audit.New(audit.Config{}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(events))
	}
	if events[0].Action != "http.request" {
		t.Fatalf("expected default action %q, got %q", "http.request", events[0].Action)
	}
}

// TestRecordsOnSuccessWithResourceIDAndMetadata checks the happy path: a
// successful request is recorded with the configured action/resource, the
// dynamically computed resource ID, and a "success" result.
func TestRecordsOnSuccessWithResourceIDAndMetadata(t *testing.T) {
	sink := &memSink{}
	app := newApp(sink)
	app.Use(audit.New(audit.Config{
		Action:     "user.update",
		Resource:   "user",
		ResourceID: func(c fh.Ctx) string { return "u-42" },
	}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(events))
	}
	e := events[0]
	if e.Action != "user.update" || e.Resource != "user" || e.ResourceID != "u-42" {
		t.Fatalf("unexpected event: %#v", e)
	}
	if e.Result != "success" {
		t.Fatalf("expected result=success, got %q", e.Result)
	}
	if meta, ok := e.Metadata["result"].(string); !ok || meta != "success" {
		t.Fatalf("expected metadata result=success, got %#v", e.Metadata)
	}
}

// TestNextPredicateSkipsAuditingEntirely verifies the escape hatch: when
// Next returns true the request must bypass auditing altogether (e.g. for
// noisy health checks), not just suppress some fields.
func TestNextPredicateSkipsAuditingEntirely(t *testing.T) {
	sink := &memSink{}
	app := newApp(sink)
	app.Use(audit.New(audit.Config{Next: func(c fh.Ctx) bool { return c.Path() == "/health" }}))
	app.Get("/health", func(c fh.Ctx) error { return c.SendString("ok") })

	resp, err := app.Test(httptest.NewRequest("GET", "/health", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
	if len(sink.all()) != 0 {
		t.Fatalf("expected no audit events for a Next-skipped route, got %d", len(sink.all()))
	}
}

// TestErrorResultNotRecordedByDefault documents an important compliance
// gap in the current design: by default (OnError=false), a request that
// fails downstream produces NO audit record at all. This is the middleware's
// actual, intentional behavior (see the `err == nil || cfg.OnError` guard),
// but it means failed/denied requests are invisible in the audit trail
// unless OnError is explicitly turned on. Locking this in as a test so any
// change to the default is a deliberate, reviewed decision.
func TestErrorResultNotRecordedByDefault(t *testing.T) {
	sink := &memSink{}
	app := newApp(sink)
	app.Use(audit.New(audit.Config{Action: "user.delete"}))
	app.Get("/x", func(c fh.Ctx) error {
		return fh.NewHTTPError(fh.StatusForbidden, "DENIED", "nope")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fh.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
	if got := len(sink.all()); got != 0 {
		t.Fatalf("expected 0 audit events by default for a failed request, got %d", got)
	}
}

// TestErrorResultRecordedWhenOnErrorTrue confirms the opt-in: setting
// OnError:true makes a failing request produce a "error" result record.
func TestErrorResultRecordedWhenOnErrorTrue(t *testing.T) {
	sink := &memSink{}
	app := newApp(sink)
	app.Use(audit.New(audit.Config{Action: "user.delete", OnError: true}))
	app.Get("/x", func(c fh.Ctx) error {
		return fh.NewHTTPError(fh.StatusForbidden, "DENIED", "nope")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fh.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("expected 1 audit event when OnError is true, got %d", len(events))
	}
	if events[0].Result != "error" {
		t.Fatalf("expected result=error, got %q", events[0].Result)
	}
}

// TestOriginalErrorStillPropagatesAfterAuditing ensures the middleware
// never swallows the downstream error (e.g. an auth/validation failure) in
// order to write its audit record.
func TestOriginalErrorStillPropagatesAfterAuditing(t *testing.T) {
	sink := &memSink{}
	app := newApp(sink)
	app.Use(audit.New(audit.Config{OnError: true}))
	app.Get("/x", func(c fh.Ctx) error {
		return fh.NewHTTPError(fh.StatusTeapot, "TEAPOT", "short and stout")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fh.StatusTeapot {
		t.Fatalf("expected the original error's status to reach the client, got %d", resp.StatusCode)
	}
}

// TestConcurrentRequestsAllRecordedExactlyOnce drives many concurrent
// requests through the middleware and a shared sink, verifying no event is
// lost, duplicated, or corrupted under -race.
func TestConcurrentRequestsAllRecordedExactlyOnce(t *testing.T) {
	sink := &memSink{}

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			app := newApp(sink)
			app.Use(audit.New(audit.Config{
				Action:     "resource.hit",
				ResourceID: func(c fh.Ctx) string { return c.Get("X-Req") },
			}))
			app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

			req := httptest.NewRequest("GET", "/x", nil)
			req.Header.Set("X-Req", strconv.Itoa(id))
			resp, err := app.Test(req, 5000)
			if err != nil || resp.StatusCode != fh.StatusOK {
				t.Errorf("status=%v err=%v", resp, err)
			}
		}()
	}
	wg.Wait()

	events := sink.all()
	if len(events) != n {
		t.Fatalf("expected %d audit events, got %d", n, len(events))
	}
	seen := make(map[string]bool, n)
	for _, e := range events {
		if seen[e.ResourceID] {
			t.Fatalf("duplicate resource id recorded: %q", e.ResourceID)
		}
		seen[e.ResourceID] = true
	}
}
