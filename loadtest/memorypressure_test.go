package loadtest

import (
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// concurrentGets fires n concurrent GET requests against addr+path and
// returns how many got each status class, using a real net/http client per
// goroutine so this exercises real concurrent connections, not a simulated
// counter. Keep-alives are disabled so each connection (and any goroutine
// bookkeeping it) closes promptly once its request completes instead of
// lingering in the client's idle pool — that matters for the MaxGoroutines
// test below, which shares this process's goroutine count with its own test
// client.
func concurrentGets(t *testing.T, addr, path string, n int) (ok, shed, other int) {
	t.Helper()
	var okN, shedN, otherN atomic.Int64
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true, DialContext: fastCloseDialContext}}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get("http://" + addr + path)
			if err != nil {
				otherN.Add(1)
				return
			}
			defer resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusOK:
				okN.Add(1)
			case http.StatusServiceUnavailable:
				shedN.Add(1)
			default:
				otherN.Add(1)
			}
		}()
	}
	wg.Wait()
	return int(okN.Load()), int(shedN.Load()), int(otherN.Load())
}

// TestMaxInFlightRequestsShedsAndRecovers configures a very low
// MaxInFlightRequests ceiling (concern #4 / hardening.go
// defaultResourceGuardMiddleware) and sends a concurrency burst well above
// it. Asserts requests beyond the ceiling are shed with 503 rather than
// queued/hung or crashing the process, and that throughput returns to normal
// (the in-flight counter is not stuck) once the burst passes.
func TestMaxInFlightRequestsShedsAndRecovers(t *testing.T) {
	const ceiling = 3
	app := fh.New(fh.WithMaxInFlightRequests(ceiling))
	app.Get("/slow", func(c fh.Ctx) error {
		time.Sleep(200 * time.Millisecond)
		return c.SendString("ok")
	})
	addr := startApp(t, app)

	ok, shed, other := concurrentGets(t, addr, "/slow", 20)
	t.Logf("MaxInFlightRequests=%d burst of 20: ok=%d shed(503)=%d other=%d", ceiling, ok, shed, other)
	if other > 0 {
		t.Errorf("unexpected non-200/503 outcomes: %d", other)
	}
	if shed == 0 {
		t.Errorf("expected some requests to be shed with 503 once in-flight exceeded %d, got none", ceiling)
	}
	if ok == 0 {
		t.Errorf("expected some requests to succeed, got none")
	}

	// Recovery: after the burst drains, the semaphore must not be stuck —
	// a plain sequential request should succeed immediately.
	time.Sleep(100 * time.Millisecond)
	resp, err := http.Get("http://" + addr + "/slow")
	if err != nil {
		t.Fatalf("post-burst request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post-burst request status = %d, want 200 (in-flight counter appears stuck)", resp.StatusCode)
	}
}

// TestMaxGoroutinesShedsAndRecovers sets MaxGoroutines just above the
// process's own baseline and drives enough concurrent slow connections to
// push the real runtime.NumGoroutine() count over that ceiling. Asserts the
// server sheds load with 503 rather than letting goroutines grow past the
// configured bound, unbounded, and that it serves normally again once the
// burst's goroutines exit (concern #4).
func TestMaxGoroutinesShedsAndRecovers(t *testing.T) {
	baseline := numGoroutine()
	// Headroom needs to comfortably absorb this process's own steady-state
	// wobble (GC workers, the test's own client goroutines, etc.) while
	// still being low enough that a burst of concurrent slow requests pushes
	// past it — runtime.NumGoroutine() is process-wide, and this test's HTTP
	// client shares the process with the server it's driving.
	ceiling := baseline + 25
	app := fh.New(
		fh.WithMaxGoroutines(ceiling),
		fh.WithResourceCheckInterval(10*time.Millisecond),
	)
	app.Get("/slow", func(c fh.Ctx) error {
		time.Sleep(200 * time.Millisecond)
		return c.SendString("ok")
	})
	addr := startApp(t, app)

	ok, shed, other := concurrentGets(t, addr, "/slow", 40)
	t.Logf("MaxGoroutines=%d (baseline %d) burst of 40: ok=%d shed(503)=%d other=%d", ceiling, baseline, ok, shed, other)
	if other > 0 {
		t.Errorf("unexpected non-200/503 outcomes: %d", other)
	}
	if shed == 0 {
		t.Errorf("expected some requests to be shed with 503 once goroutines exceeded %d, got none", ceiling)
	}

	// Let the burst's connection goroutines actually exit before checking
	// recovery, and probe with a fresh, non-keep-alive client so the probe
	// itself doesn't leave a pooled connection/goroutine behind.
	runtime.GC()
	time.Sleep(150 * time.Millisecond)
	probe := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true, DialContext: fastCloseDialContext}}
	if !waitUntil(4*time.Second, 100*time.Millisecond, func() bool {
		resp, err := probe.Get("http://" + addr + "/slow")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}) {
		t.Fatalf("server did not resume serving 200s after the goroutine burst subsided")
	}
}

// TestMaxHeapBytesSheds configures an unreachably low MaxHeapBytes ceiling
// (concern #4) and asserts every request is shed with 503 rather than the
// process attempting to serve past its configured memory ceiling, crashing,
// or hanging.
//
// It intentionally does not assert recovery from heap pressure within this
// same process: Go's GC reclaiming enough heap to fall back under an
// artificially tiny ceiling is not something a short, deterministic test can
// force. Instead it demonstrates recovery structurally — a second app with
// no heap ceiling serves normally — which is what actually varies here (the
// configuration), rather than pretending to control the garbage collector.
func TestMaxHeapBytesSheds(t *testing.T) {
	app := fh.New(fh.WithMaxHeapBytes(1)) // 1 byte: always below current heap usage
	app.Get("/ping", func(c fh.Ctx) error { return c.SendString("pong") })
	addr := startApp(t, app)

	for i := 0; i < 5; i++ {
		resp, err := http.Get("http://" + addr + "/ping")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("request %d status = %d, want 503 (MaxHeapBytes=1 should shed every request)", i, resp.StatusCode)
		}
	}

	// "Recovery" demonstrated structurally: a normally-configured app on the
	// same process serves fine, proving the shedding above was the ceiling
	// doing its job, not the server being broken.
	normal := fh.New()
	normal.Get("/ping", func(c fh.Ctx) error { return c.SendString("pong") })
	normalAddr := startApp(t, normal)
	resp, err := http.Get("http://" + normalAddr + "/ping")
	if err != nil {
		t.Fatalf("control request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("control app status = %d, want 200", resp.StatusCode)
	}
}
