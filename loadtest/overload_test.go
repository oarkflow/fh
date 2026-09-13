package loadtest

import (
	"net/http"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/bulkhead"
	"github.com/oarkflow/fh/mw/loadshed"
)

// TestBulkheadRejectsBeyondCeilingAndRecovers mounts mw/bulkhead with a low
// MaxConcurrent ceiling and pushes a concurrency burst well past it. Asserts
// requests beyond the ceiling get a clean rejection rather than a hang, and
// that throughput returns to normal afterward — the bucket semaphore is not
// left stuck at capacity (concern #6).
func TestBulkheadRejectsBeyondCeilingAndRecovers(t *testing.T) {
	const ceiling = 4
	app := fh.New()
	app.Use(bulkhead.New(bulkhead.Config{MaxConcurrent: ceiling}))
	app.Get("/slow", func(c fh.Ctx) error {
		time.Sleep(200 * time.Millisecond)
		return c.SendString("ok")
	})
	addr := startApp(t, app)

	ok, shed, other := concurrentGets(t, addr, "/slow", 30)
	t.Logf("bulkhead MaxConcurrent=%d burst of 30: ok=%d shed(503)=%d other=%d", ceiling, ok, shed, other)
	if other > 0 {
		t.Errorf("unexpected non-200/503 outcomes: %d", other)
	}
	if shed == 0 {
		t.Errorf("expected bulkhead to reject some requests beyond MaxConcurrent=%d, got none", ceiling)
	}
	if ok == 0 {
		t.Errorf("expected some requests to be admitted, got none")
	}

	if !waitUntil(3*time.Second, 50*time.Millisecond, func() bool {
		resp, err := http.Get("http://" + addr + "/slow")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}) {
		t.Fatalf("bulkhead did not resume admitting requests after the burst passed (semaphore appears stuck)")
	}
}

// TestLoadShedRejectsBeyondCeilingAndRecovers mounts mw/loadshed with a low
// MaxInFlight ceiling and drives the same kind of burst, checking the
// alternative overload-control middleware behaves the same way: clean
// rejection past the ceiling, normal throughput once the burst passes
// (concern #6).
func TestLoadShedRejectsBeyondCeilingAndRecovers(t *testing.T) {
	const ceiling = 4
	app := fh.New()
	app.Use(loadshed.New(loadshed.Config{MaxInFlight: ceiling}))
	app.Get("/slow", func(c fh.Ctx) error {
		time.Sleep(200 * time.Millisecond)
		return c.SendString("ok")
	})
	addr := startApp(t, app)

	ok, shed, other := concurrentGets(t, addr, "/slow", 30)
	t.Logf("loadshed MaxInFlight=%d burst of 30: ok=%d shed(503)=%d other=%d", ceiling, ok, shed, other)
	if other > 0 {
		t.Errorf("unexpected non-200/503 outcomes: %d", other)
	}
	if shed == 0 {
		t.Errorf("expected loadshed to reject some requests beyond MaxInFlight=%d, got none", ceiling)
	}
	if ok == 0 {
		t.Errorf("expected some requests to be admitted, got none")
	}

	if !waitUntil(3*time.Second, 50*time.Millisecond, func() bool {
		resp, err := http.Get("http://" + addr + "/slow")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}) {
		t.Fatalf("loadshed did not resume admitting requests after the burst passed (counter appears stuck)")
	}
}
