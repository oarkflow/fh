package loadtest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// TestSoakKeepAliveAndConnectionChurn exercises concern #1 from
// docs/production-readiness.md: long-lived keep-alive connections plus a
// separate stream of connect/request/disconnect churn, run concurrently for a
// bounded duration (short for normal CI, much longer under
// FH_LOADTEST_SOAK=1). It is a real fh.App behind a real net.Listener, hit by
// real net/http clients over real TCP.
//
// It asserts: no unexpected request errors, goroutine count stays bounded
// after the run settles (no unbounded growth), and heap allocation shows no
// clear monotonic leak trend across the run.
func TestSoakKeepAliveAndConnectionChurn(t *testing.T) {
	app := fh.New(
		fh.WithReadTimeout(5*time.Second),
		fh.WithWriteTimeout(5*time.Second),
		fh.WithIdleTimeout(30*time.Second),
	)
	var served atomic.Int64
	app.Get("/ping", func(c fh.Ctx) error {
		served.Add(1)
		return c.SendString("pong")
	})
	addr := startApp(t, app)

	duration := soakDuration(3 * time.Second)
	t.Logf("running soak for %s (FH_LOADTEST_SOAK=%v)", duration, isSoak())

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Let the server settle and take a baseline goroutine reading before load
	// starts, so growth is measured relative to this app's own steady state
	// rather than an arbitrary constant.
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	baselineGoroutines := numGoroutine()

	const keepAliveWorkers = 8
	const churnWorkers = 3
	// Each churn iteration burns one client-side ephemeral port that then
	// sits in TIME_WAIT for tens of seconds. An unthrottled tight loop can
	// exhaust the local ephemeral port range within a few seconds (this is a
	// property of this test harness hammering 127.0.0.1 from one process,
	// not of the server), which then makes unrelated dials in this process
	// fail with "can't assign requested address" — including, confusingly,
	// in whichever test happens to run next, or in a same-suite rerun
	// started before earlier TIME_WAIT sockets drain (see README.md). A
	// small per-iteration pause keeps real churn (connect, request,
	// disconnect, repeat) without manufacturing that self-inflicted failure.
	const churnPause = 10 * time.Millisecond

	var reqErrors atomic.Int64
	var reqOK atomic.Int64
	var wg sync.WaitGroup

	// Keep-alive workers: one persistent client each, reused across many
	// requests over the same TCP connection.
	for i := 0; i < keepAliveWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{
				Timeout: 3 * time.Second,
				Transport: &http.Transport{
					MaxIdleConnsPerHost: 1,
					DisableKeepAlives:   false,
				},
			}
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				resp, err := client.Get("http://" + addr + "/ping")
				if err != nil {
					reqErrors.Add(1)
					continue
				}
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					reqOK.Add(1)
				} else {
					reqErrors.Add(1)
				}
			}
		}()
	}

	// Churn workers: fresh TCP connection, one request, close, repeat.
	for i := 0; i < churnWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				conn, err := net.DialTimeout("tcp", addr, time.Second)
				if err != nil {
					reqErrors.Add(1)
					time.Sleep(churnPause)
					continue
				}
				_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
				_, err = fmt.Fprintf(conn, "GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
				if err != nil {
					reqErrors.Add(1)
					closeFast(conn)
					time.Sleep(churnPause)
					continue
				}
				buf := make([]byte, 512)
				n, _ := conn.Read(buf)
				closeFast(conn)
				if n > 0 && string(buf[:12]) == "HTTP/1.1 200" {
					reqOK.Add(1)
				} else if n > 0 {
					reqErrors.Add(1)
				}
				time.Sleep(churnPause)
			}
		}()
	}

	// Memory/goroutine sampler: enough samples across the run to distinguish
	// a real trend from GC noise, not just a single before/after snapshot.
	const samples = 40
	interval := duration / samples
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	heapSamples := make([]uint64, 0, samples+1)
	goroutineSamples := make([]int, 0, samples+1)
	sampleTicker := time.NewTicker(interval)
	defer sampleTicker.Stop()
samplingLoop:
	for {
		select {
		case <-ctx.Done():
			break samplingLoop
		case <-sampleTicker.C:
			heapSamples = append(heapSamples, heapAlloc())
			goroutineSamples = append(goroutineSamples, numGoroutine())
		}
	}

	wg.Wait()

	t.Logf("requests ok=%d errors=%d", reqOK.Load(), reqErrors.Load())
	if reqOK.Load() == 0 {
		t.Fatalf("no successful requests were served during the soak")
	}
	// A handful of transient errors around client-side timeouts are expected
	// noise on a loaded CI box; a large error rate indicates a real problem.
	if total := reqOK.Load() + reqErrors.Load(); total > 0 {
		errRate := float64(reqErrors.Load()) / float64(total)
		if errRate > 0.02 {
			t.Errorf("request error rate too high: %d/%d (%.2f%%)", reqErrors.Load(), total, errRate*100)
		}
	}

	// Goroutine growth check: let things settle, then compare to baseline.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	finalGoroutines := numGoroutine()
	t.Logf("goroutines baseline=%d final=%d", baselineGoroutines, finalGoroutines)
	// Generous slack: worker goroutines exit asynchronously after ctx.Done,
	// and the runtime itself fluctuates by a handful of goroutines. A server
	// that leaks per-connection or per-request goroutines will overshoot this
	// by a wide margin, not a handful.
	if finalGoroutines > baselineGoroutines+40 {
		t.Errorf("goroutine count grew from %d to %d after the soak settled (possible leak)", baselineGoroutines, finalGoroutines)
	}

	if len(heapSamples) >= 10 {
		firstQuarter := heapSamples[:len(heapSamples)/4]
		lastQuarter := heapSamples[len(heapSamples)-len(heapSamples)/4:]
		avg := func(xs []uint64) float64 {
			var sum float64
			for _, x := range xs {
				sum += float64(x)
			}
			return sum / float64(len(xs))
		}
		firstAvg := avg(firstQuarter)
		lastAvg := avg(lastQuarter)
		t.Logf("heap alloc avg first-quarter=%.0f last-quarter=%.0f", firstAvg, lastAvg)
		// A real leak under continuous, bounded-payload traffic shows up as
		// heap that keeps climbing well beyond GC noise. Flag only a large,
		// sustained ratio rather than any growth at all (Go's heap is
		// naturally noisy run to run).
		if firstAvg > 0 && lastAvg > firstAvg*4 && lastAvg-firstAvg > 20<<20 {
			t.Errorf("heap allocation grew from ~%.0f to ~%.0f bytes across the soak (possible leak)", firstAvg, lastAvg)
		}
	} else {
		t.Logf("too few heap samples (%d) for a trend check in this short run", len(heapSamples))
	}
}
