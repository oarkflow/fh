package auditlog

import (
	"fmt"
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

func doRequest(t *testing.T, addr, method, path string, headers map[string]string) int {
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
	buf := make([]byte, 8192)
	n, _ := conn.Read(buf)
	var proto, status string
	fmt.Sscan(string(buf[:n]), &proto, &status)
	code := 0
	fmt.Sscan(status, &code)
	return code
}

// captureLogger records every log call so tests can assert on what (and
// what severity) reached the logger, without depending on stderr output.
type captureLogger struct {
	mu      sync.Mutex
	records []string
}

func (l *captureLogger) add(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, msg+" "+fmt.Sprint(args...))
}
func (l *captureLogger) Printf(format string, args ...any) { l.add(fmt.Sprintf(format, args...)) }
func (l *captureLogger) Info(msg string, args ...any)      { l.add(msg, args...) }
func (l *captureLogger) Warn(msg string, args ...any)      { l.add(msg, args...) }
func (l *captureLogger) Error(msg string, args ...any)     { l.add(msg, args...) }
func (l *captureLogger) Debug(msg string, args ...any)     { l.add(msg, args...) }
func (l *captureLogger) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.records...)
}

// ---- Pure function unit tests -------------------------------------------

func TestClassifyStatusBuckets(t *testing.T) {
	cases := map[int]Severity{
		200: SeverityInfo,
		301: SeverityInfo,
		399: SeverityInfo,
		400: SeverityWarning,
		401: SeverityWarning,
		499: SeverityWarning,
		500: SeverityCritical,
		503: SeverityCritical,
		0:   SeverityInfo,
	}
	for code, want := range cases {
		if got := classifyStatus(code); got != want {
			t.Errorf("classifyStatus(%d) = %q, want %q", code, got, want)
		}
	}
}

func TestClassifyEventMapping(t *testing.T) {
	cases := []struct {
		code int
		path string
		want Event
	}{
		{401, "/x", EventAuthFailure},
		{403, "/x", EventPrivilegeEsc},
		{429, "/x", EventRateLimit},
		{400, "/search?q=<script>", EventInjection},
		{400, "/search?q=' UNION SELECT * FROM users", EventInjection},
		{400, "/../etc/passwd", EventInjection},
		{400, "/plain-bad-request", EventSuspicious},
		{500, "/x", EventSuspicious},
		{503, "/x", EventSuspicious},
		{200, "/x", EventDataAccess},
		{304, "/x", EventDataAccess},
	}
	for _, tc := range cases {
		if got := classifyEvent(tc.code, tc.path); got != tc.want {
			t.Errorf("classifyEvent(%d, %q) = %q, want %q", tc.code, tc.path, got, tc.want)
		}
	}
}

func TestContainsInjectionDetectsKnownIndicatorsCaseInsensitively(t *testing.T) {
	positive := []string{
		"/a<script>alert(1)</script>",
		"/x?id=1 UNION SELECT password FROM users",
		"/x?id=1; DROP TABLE users",
		"/../../etc/passwd",
		"/a..%2f..%2fetc",
		"/a%00b",
		"/x?u=JAVASCRIPT:alert(1)",
		"/X?Q=<SCRIPT>",
	}
	for _, p := range positive {
		if !containsInjection(p) {
			t.Errorf("containsInjection(%q) = false, want true", p)
		}
	}
	negative := []string{
		"/users/42",
		"/search?q=hello+world",
		"",
	}
	for _, p := range negative {
		if containsInjection(p) {
			t.Errorf("containsInjection(%q) = true, want false", p)
		}
	}
}

func TestMaskSensitiveNeverReturnsTheRawSecretAndKeepsLengthPatterns(t *testing.T) {
	cases := []string{
		"",
		"a",
		"abcdefgh",                   // exactly 8
		"abcdefghi",                  // 9: short-form mask
		"abcdefghijklmnopqrstuvwxyz", // long: 4+mask+4
		"Bearer super-secret-token-value-1234567890",
	}
	for _, secret := range cases {
		masked := maskSensitive(secret)
		if secret != "" && masked == secret {
			t.Errorf("maskSensitive(%q) returned the raw value unmasked", secret)
		}
		if len(secret) > 8 && strings.Contains(masked, secret) {
			t.Errorf("maskSensitive(%q) = %q still contains the full raw secret", secret, masked)
		}
	}
	// Longer secrets keep a short recognizable prefix/suffix (useful for
	// correlating log entries) but must never expose the middle.
	got := maskSensitive("abcdefghijklmnopqrstuvwxyz")
	if !strings.HasPrefix(got, "abcd") || !strings.HasSuffix(got, "wxyz") {
		t.Errorf("maskSensitive long value = %q, want abcd***...***wxyz shape", got)
	}
	if strings.Contains(got, "ijklmnopqrstuv") {
		t.Errorf("maskSensitive long value = %q leaks its middle segment", got)
	}
}

func TestSeverityRankOrdering(t *testing.T) {
	if severityRank(SeverityCritical) <= severityRank(SeverityWarning) {
		t.Error("Critical must rank above Warning")
	}
	if severityRank(SeverityWarning) <= severityRank(SeverityInfo) {
		t.Error("Warning must rank above Info")
	}
	if severityRank(Severity("bogus")) != 0 {
		t.Error("an unknown severity must rank below Info (0), not panic or rank arbitrarily high")
	}
}

func TestNormalizeFillsDefaults(t *testing.T) {
	cfg := normalize(Config{})
	if cfg.MinSeverity != SeverityInfo {
		t.Errorf("MinSeverity = %q, want %q", cfg.MinSeverity, SeverityInfo)
	}
	if len(cfg.Sinks) != 1 {
		t.Fatalf("Sinks = %v, want exactly one default sink", cfg.Sinks)
	}
	if _, ok := cfg.Sinks[0].(*LogSink); !ok {
		t.Errorf("default sink = %T, want *LogSink", cfg.Sinks[0])
	}
}

func TestNormalizePreservesExplicitConfig(t *testing.T) {
	sink := NewBufferSink(5)
	cfg := normalize(Config{MinSeverity: SeverityCritical, Sinks: []Sink{sink}})
	if cfg.MinSeverity != SeverityCritical {
		t.Errorf("MinSeverity = %q, want %q (explicit value must survive normalize)", cfg.MinSeverity, SeverityCritical)
	}
	if len(cfg.Sinks) != 1 || cfg.Sinks[0] != sink {
		t.Error("explicit Sinks must not be replaced by normalize")
	}
}

// ---- Integration tests: what actually gets logged ------------------------

// TestAuditRecordsTrueStatusForTypedErrorReturns is the regression test for
// a real bug: the middleware used to read c.StatusCode() immediately after
// c.Next() returned, but a handler that returns a typed *fh.HTTPError (the
// idiomatic pattern everywhere in this codebase, e.g. fh.Unauthorized(...))
// never calls c.Status() itself — the framework only assigns the real
// status to the response *after* this middleware's c.Next() call already
// returned, via the default error handler. That meant every such error was
// audited under its stale pre-call status (200) and severity (info),
// silently hiding auth failures / rate limits / server errors from the
// audit trail regardless of MinSeverity filtering. New() now derives the
// status from c.ErrorReport(err) when an error is present.
func TestAuditRecordsTrueStatusForTypedErrorReturns(t *testing.T) {
	buf := NewBufferSink(10)
	app := fh.New()
	app.Use(New(Config{Sinks: []Sink{buf}}))
	app.Get("/", func(c fh.Ctx) error { return fh.Unauthorized("bad creds") })
	addr := testServer(t, app)

	code := doRequest(t, addr, "GET", "/", nil)
	if code != fh.StatusUnauthorized {
		t.Fatalf("actual response status = %d, want 401", code)
	}

	entries := buf.Entries()
	if len(entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(entries))
	}
	e := entries[0]
	if e.StatusCode != fh.StatusUnauthorized {
		t.Errorf("audited StatusCode = %d, want 401 (must match the real response)", e.StatusCode)
	}
	if e.Severity != SeverityWarning {
		t.Errorf("audited Severity = %q, want %q", e.Severity, SeverityWarning)
	}
	if e.Event != EventAuthFailure {
		t.Errorf("audited Event = %q, want %q", e.Event, EventAuthFailure)
	}
}

// TestAuditRecordsTrueStatusForServerErrorReturns proves the same fix
// covers 5xx: an unhandled server error must be audited as critical, not as
// a benign 200/info entry.
func TestAuditRecordsTrueStatusForServerErrorReturns(t *testing.T) {
	buf := NewBufferSink(10)
	app := fh.New()
	app.Use(New(Config{Sinks: []Sink{buf}}))
	app.Get("/", func(c fh.Ctx) error { return fh.Unavailable("dependency down") })
	addr := testServer(t, app)

	code := doRequest(t, addr, "GET", "/", nil)
	if code != fh.StatusServiceUnavailable {
		t.Fatalf("actual response status = %d, want 503", code)
	}
	entries := buf.Entries()
	if len(entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(entries))
	}
	if entries[0].StatusCode != fh.StatusServiceUnavailable {
		t.Errorf("audited StatusCode = %d, want 503", entries[0].StatusCode)
	}
	if entries[0].Severity != SeverityCritical {
		t.Errorf("audited Severity = %q, want %q", entries[0].Severity, SeverityCritical)
	}
}

// TestAuditRecordsSuccessAsInfoDataAccess proves the ordinary happy path is
// still recorded accurately at info/data_access.
func TestAuditRecordsSuccessAsInfoDataAccess(t *testing.T) {
	buf := NewBufferSink(10)
	app := fh.New()
	app.Use(New(Config{Sinks: []Sink{buf}}))
	app.Get("/ok", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	doRequest(t, addr, "GET", "/ok", nil)
	entries := buf.Entries()
	if len(entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(entries))
	}
	e := entries[0]
	if e.StatusCode != fh.StatusOK || e.Severity != SeverityInfo || e.Event != EventDataAccess {
		t.Errorf("entry = %+v, want status=200 severity=info event=data_access", e)
	}
	if e.Method != "GET" || e.Path != "/ok" {
		t.Errorf("entry method/path = %q/%q, want GET//ok", e.Method, e.Path)
	}
}

// TestMinSeverityFiltersOutLowerSeverityEntries proves configuring
// MinSeverity actually suppresses entries below that threshold rather than
// only affecting logging output.
func TestMinSeverityFiltersOutLowerSeverityEntries(t *testing.T) {
	buf := NewBufferSink(10)
	app := fh.New()
	app.Use(New(Config{Sinks: []Sink{buf}, MinSeverity: SeverityWarning}))
	app.Get("/ok", func(c fh.Ctx) error { return c.SendString("ok") })
	app.Get("/fail", func(c fh.Ctx) error { return fh.BadRequest("nope") })
	addr := testServer(t, app)

	doRequest(t, addr, "GET", "/ok", nil)
	doRequest(t, addr, "GET", "/fail", nil)

	entries := buf.Entries()
	if len(entries) != 1 {
		t.Fatalf("got %d audit entries, want 1 (the info-level /ok request must be filtered out)", len(entries))
	}
	if entries[0].Path != "/fail" {
		t.Errorf("surviving entry path = %q, want /fail", entries[0].Path)
	}
}

// TestSkipFunctionSuppressesAuditingButHandlerStillRuns proves Skip is a
// pure audit bypass: the request is still served normally, only the audit
// entry is omitted.
func TestSkipFunctionSuppressesAuditingButHandlerStillRuns(t *testing.T) {
	buf := NewBufferSink(10)
	var handlerCalls atomic.Int32
	app := fh.New()
	app.Use(New(Config{
		Sinks: []Sink{buf},
		Skip:  func(c fh.Ctx) bool { return c.Path() == "/health" },
	}))
	app.Get("/health", func(c fh.Ctx) error {
		handlerCalls.Add(1)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	code := doRequest(t, addr, "GET", "/health", nil)
	if code != fh.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if handlerCalls.Load() != 1 {
		t.Fatalf("handler called %d times, want 1 (Skip must not prevent normal handling)", handlerCalls.Load())
	}
	if len(buf.Entries()) != 0 {
		t.Fatalf("got %d audit entries, want 0 for a skipped path", len(buf.Entries()))
	}
}

// TestCaptureHeadersAreMaskedNeverRawInAuditEntry is the sensitive-field
// leak check: a header configured for capture (e.g. Authorization) must
// appear in Details only in masked form, never as the raw secret value.
func TestCaptureHeadersAreMaskedNeverRawInAuditEntry(t *testing.T) {
	buf := NewBufferSink(10)
	app := fh.New()
	app.Use(New(Config{Sinks: []Sink{buf}, CaptureHeaders: []string{"Authorization"}}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	const secret = "Bearer sk_live_super_secret_1234567890"
	doRequest(t, addr, "GET", "/", map[string]string{"Authorization": secret})

	entries := buf.Entries()
	if len(entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(entries))
	}
	captured, ok := entries[0].Details["Authorization"]
	if !ok {
		t.Fatal("expected Details[\"Authorization\"] to be present")
	}
	if captured == secret {
		t.Fatalf("Details[\"Authorization\"] = %q leaked the raw secret unmasked", captured)
	}
	if strings.Contains(captured, "sk_live_super_secret") {
		t.Fatalf("Details[\"Authorization\"] = %q leaked the secret's middle segment", captured)
	}
}

// TestUncapturedHeadersAreNeverIncludedInDetails proves the capture list is
// an allowlist: a header not named in CaptureHeaders never ends up in
// Details, even if present on the request.
func TestUncapturedHeadersAreNeverIncludedInDetails(t *testing.T) {
	buf := NewBufferSink(10)
	app := fh.New()
	app.Use(New(Config{Sinks: []Sink{buf}, CaptureHeaders: []string{"X-Wanted"}}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	doRequest(t, addr, "GET", "/", map[string]string{
		"X-Wanted": "keep-me",
		"X-Secret": "do-not-log-me",
		"Cookie":   "session=abc123",
	})

	entries := buf.Entries()
	if len(entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(entries))
	}
	d := entries[0].Details
	if _, ok := d["X-Secret"]; ok {
		t.Error("Details contains X-Secret, which was never configured for capture")
	}
	if _, ok := d["Cookie"]; ok {
		t.Error("Details contains Cookie, which was never configured for capture")
	}
	if _, ok := d["X-Wanted"]; !ok {
		t.Error("Details is missing X-Wanted, which was configured for capture")
	}
}

// TestNoCaptureHeadersConfiguredMeansNoDetails proves an empty
// CaptureHeaders produces a nil Details map rather than an empty one, per
// the omitempty contract on AuditEntry.Details.
func TestNoCaptureHeadersConfiguredMeansNoDetails(t *testing.T) {
	buf := NewBufferSink(10)
	app := fh.New()
	app.Use(New(Config{Sinks: []Sink{buf}}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	doRequest(t, addr, "GET", "/", map[string]string{"Authorization": "Bearer xyz"})
	entries := buf.Entries()
	if len(entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(entries))
	}
	if entries[0].Details != nil {
		t.Errorf("Details = %v, want nil when CaptureHeaders is empty", entries[0].Details)
	}
}

// TestPrincipalIDIsCapturedWhenAuthenticated proves an authenticated
// request's Principal ID ends up on the audit entry, so authenticated
// actions are attributable in the audit trail.
func TestPrincipalIDIsCapturedWhenAuthenticated(t *testing.T) {
	buf := NewBufferSink(10)
	app := fh.New()
	app.Use(func(c fh.Ctx) error {
		fh.SetPrincipal(c, fh.Principal{ID: "user-42"})
		return c.Next()
	})
	app.Use(New(Config{Sinks: []Sink{buf}}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	doRequest(t, addr, "GET", "/", nil)
	entries := buf.Entries()
	if len(entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(entries))
	}
	if entries[0].PrincipalID != "user-42" {
		t.Errorf("PrincipalID = %q, want %q", entries[0].PrincipalID, "user-42")
	}
}

// TestPrincipalIDIsEmptyWhenUnauthenticated proves the field is left blank
// (not some placeholder) for anonymous requests.
func TestPrincipalIDIsEmptyWhenUnauthenticated(t *testing.T) {
	buf := NewBufferSink(10)
	app := fh.New()
	app.Use(New(Config{Sinks: []Sink{buf}}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	doRequest(t, addr, "GET", "/", nil)
	entries := buf.Entries()
	if len(entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(entries))
	}
	if entries[0].PrincipalID != "" {
		t.Errorf("PrincipalID = %q, want empty for an anonymous request", entries[0].PrincipalID)
	}
}

// TestRequestIDIsCapturedFromHeader proves the correlating request id
// header is threaded onto the audit entry.
func TestRequestIDIsCapturedFromHeader(t *testing.T) {
	buf := NewBufferSink(10)
	app := fh.New()
	app.Use(New(Config{Sinks: []Sink{buf}}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	doRequest(t, addr, "GET", "/", map[string]string{fh.HeaderRequestID: "req-abc-123"})
	entries := buf.Entries()
	if len(entries) != 1 {
		t.Fatalf("got %d audit entries, want 1", len(entries))
	}
	if entries[0].RequestID != "req-abc-123" {
		t.Errorf("RequestID = %q, want %q", entries[0].RequestID, "req-abc-123")
	}
}

// TestLoggerOnlyInvokedAtOrAboveWarningSeverity proves the optional Logger
// hook is not spammed on every request: it fires for warning/critical
// entries but stays silent for ordinary info-level success traffic.
func TestLoggerOnlyInvokedAtOrAboveWarningSeverity(t *testing.T) {
	logger := &captureLogger{}
	buf := NewBufferSink(10)
	app := fh.New()
	app.Use(New(Config{Sinks: []Sink{buf}, Logger: logger}))
	app.Get("/ok", func(c fh.Ctx) error { return c.SendString("ok") })
	app.Get("/fail", func(c fh.Ctx) error { return fh.BadRequest("bad") })
	addr := testServer(t, app)

	doRequest(t, addr, "GET", "/ok", nil)
	if got := logger.all(); len(got) != 0 {
		t.Fatalf("logger received %d calls for an info-level request, want 0: %v", len(got), got)
	}

	doRequest(t, addr, "GET", "/fail", nil)
	if got := logger.all(); len(got) != 1 {
		t.Fatalf("logger received %d calls for a warning-level request, want 1: %v", len(got), got)
	}
}

// TestDefaultConfigUsesLogSinkWithoutPanicking proves omitting Sinks
// entirely (the normalize() fallback) still produces a working middleware
// rather than panicking on a nil sink slice.
func TestDefaultConfigUsesLogSinkWithoutPanicking(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	code := doRequest(t, addr, "GET", "/", nil)
	if code != fh.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
}

// ---- BufferSink concurrency and correctness -------------------------------

// TestBufferSinkConcurrentWritesAreSafeAndCountedExactly hammers a single
// BufferSink from many goroutines simultaneously and checks the mutex
// correctly serializes access: no entry is lost or duplicated, and no data
// race is triggered (run with -race).
func TestBufferSinkConcurrentWritesAreSafeAndCountedExactly(t *testing.T) {
	sink := NewBufferSink(100000)
	const goroutines = 50
	const perGoroutine = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				sink.Write(AuditEntry{Path: fmt.Sprintf("/g%d/%d", i, j)})
			}
		}(i)
	}
	wg.Wait()

	entries := sink.Entries()
	if len(entries) != goroutines*perGoroutine {
		t.Fatalf("got %d entries, want %d", len(entries), goroutines*perGoroutine)
	}
}

// TestBufferSinkTrimsToMaxSizeKeepingMostRecent proves the ring-buffer trim
// behavior: once maxSize is exceeded, the OLDEST entries are dropped and the
// most recent maxSize entries survive, in order.
func TestBufferSinkTrimsToMaxSizeKeepingMostRecent(t *testing.T) {
	sink := NewBufferSink(3)
	for i := 0; i < 5; i++ {
		sink.Write(AuditEntry{Path: fmt.Sprintf("/%d", i)})
	}
	entries := sink.Entries()
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	want := []string{"/2", "/3", "/4"}
	for i, e := range entries {
		if e.Path != want[i] {
			t.Errorf("entries[%d].Path = %q, want %q", i, e.Path, want[i])
		}
	}
}

// TestNewBufferSinkNonPositiveSizeFallsBackToDefault proves a caller
// mistake (zero or negative maxSize) does not produce a sink that discards
// everything or panics.
func TestNewBufferSinkNonPositiveSizeFallsBackToDefault(t *testing.T) {
	for _, size := range []int{0, -1, -100} {
		sink := NewBufferSink(size)
		sink.Write(AuditEntry{Path: "/x"})
		if len(sink.Entries()) != 1 {
			t.Errorf("NewBufferSink(%d): entry was dropped instead of falling back to a usable default size", size)
		}
	}
}

// TestBufferSinkClearEmptiesEntries proves Clear actually resets state
// rather than leaving stale entries visible via Entries().
func TestBufferSinkClearEmptiesEntries(t *testing.T) {
	sink := NewBufferSink(10)
	sink.Write(AuditEntry{Path: "/x"})
	sink.Clear()
	if got := sink.Entries(); len(got) != 0 {
		t.Fatalf("Entries() = %v after Clear, want empty", got)
	}
}

// TestBufferSinkEntriesReturnsACopyNotTheInternalSlice proves callers
// mutating the returned slice cannot corrupt the sink's internal state
// (defense against a caller aliasing the backing array).
func TestBufferSinkEntriesReturnsACopyNotTheInternalSlice(t *testing.T) {
	sink := NewBufferSink(10)
	sink.Write(AuditEntry{Path: "/original"})
	got := sink.Entries()
	got[0].Path = "/mutated"

	again := sink.Entries()
	if again[0].Path != "/original" {
		t.Fatalf("internal entry was mutated via the returned slice: got %q, want %q", again[0].Path, "/original")
	}
}

// TestConcurrentRequestsProduceExactlyOneEntryEach exercises the full
// middleware end-to-end under concurrent HTTP load and checks the sink
// received exactly one audit entry per request, with no cross-request data
// corruption in the recorded path/method.
func TestConcurrentRequestsProduceExactlyOneEntryEach(t *testing.T) {
	buf := NewBufferSink(1000)
	app := fh.New()
	app.Use(New(Config{Sinks: []Sink{buf}}))
	app.Get("/a", func(c fh.Ctx) error { return c.SendString("a") })
	app.Get("/b", func(c fh.Ctx) error { return c.SendString("b") })
	addr := testServer(t, app)

	const n = 40
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			path := "/a"
			if i%2 == 1 {
				path = "/b"
			}
			doRequest(t, addr, "GET", path, nil)
		}(i)
	}
	wg.Wait()

	entries := buf.Entries()
	if len(entries) != n {
		t.Fatalf("got %d audit entries, want %d", len(entries), n)
	}
	var countA, countB int
	for _, e := range entries {
		switch e.Path {
		case "/a":
			countA++
		case "/b":
			countB++
		default:
			t.Errorf("unexpected audited path %q", e.Path)
		}
	}
	if countA != n/2 || countB != n/2 {
		t.Fatalf("countA=%d countB=%d, want %d/%d", countA, countB, n/2, n/2)
	}
}
