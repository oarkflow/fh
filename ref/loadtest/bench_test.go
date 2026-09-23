package loadtest

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
)

// ============================================================
// fh app — production settings, same as benchmark server
// ============================================================

func newFhApp() *fh.App {
	app := fh.NewFast(
		fh.WithDisableHTTP2(true),
		fh.WithDisablePanicRecovery(true),
		fh.WithSendDateHeader(false),
	)

	app.Use(func(c fh.Ctx) error {
		if c.Get("Authorization") == "" {
			return c.Status(401).JSON(map[string]string{"error": "unauthorized"})
		}
		c.Locals("user_id", "usr-1")
		return c.Next()
	})

	app.Post("/bookmarks", func(c fh.Ctx) error {
		var input struct {
			URL   string `json:"url"`
			Title string `json:"title"`
		}
		if err := c.BodyParser(&input); err != nil {
			return c.Status(400).JSON(map[string]string{"error": "bad input"})
		}
		return c.Status(201).JSON(map[string]string{"id": "bm-1", "url": input.URL, "title": input.Title})
	})

	app.Get("/bookmarks", func(c fh.Ctx) error {
		return c.JSON([]map[string]string{{"id": "bm-1", "url": "https://go.dev"}})
	})

	app.Get("/health", func(c fh.Ctx) error {
		return c.JSON(map[string]string{"status": "ok"})
	})

	return app
}

// ============================================================
// REF engine — same business logic, compiled DAG
// ============================================================

var PrincipalKeyBench = fact.NewKey[capability.PrincipalFact]("bench.principal")

type CreateBookmarkIntent struct{}

func (CreateBookmarkIntent) Name() intent.Name { return "bookmark.create" }
func (CreateBookmarkIntent) Spec() intent.Spec {
	return intent.Spec{Requires: []fact.AnyKey{capability.PrincipalKey.Any()}}
}
func (CreateBookmarkIntent) Run(nc *ref.NodeContext, in struct {
	URL   string `json:"url"`
	Title string `json:"title"`
}) (ref.Outcome[map[string]string], error) {
	_, err := ref.Require(nc, capability.PrincipalKey)
	if err != nil {
		return ref.Outcome[map[string]string]{}, err
	}
	return ref.Outcome[map[string]string]{
		Value: map[string]string{"id": "bm-1", "url": in.URL, "title": in.Title},
		Effects: effect.EffectPlan{
			FireAndForget: []effect.Effect{&benchEffect{}},
		},
	}, nil
}

type benchEffect struct{}

func (e *benchEffect) Name() string            { return "analytics" }
func (e *benchEffect) Kind() effect.EffectKind { return effect.FireAndForget }
func (e *benchEffect) Commit(ctx context.Context) error { return nil }

// ============================================================
// Helpers
// ============================================================

func startApp(t *testing.T, app *fh.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	go app.Serve(ln)
	t.Cleanup(func() {
		app.ShutdownWithTimeout(5 * time.Second)
		ln.Close()
	})
	for i := 0; i < 100; i++ {
		conn, err := net.DialTimeout("tcp", addr, 10*time.Millisecond)
		if err == nil {
			conn.Close()
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("server not ready")
	return ""
}

func runWrk(t *testing.T, addr, method, path, body string, duration time.Duration, connections, threads int) wrkResult {
	t.Helper()

	wrkPath, err := exec.LookPath("wrk")
	if err != nil {
		t.Skip("wrk not installed, skipping HTTP benchmark")
	}

	args := []string{
		"-t" + fmt.Sprintf("%d", threads),
		"-c" + fmt.Sprintf("%d", connections),
		"-d" + duration.String(),
		"--latency",
	}

	if method == "POST" && body != "" {
		args = append(args, "-s", "-")
	}

	url := "http://" + addr + path
	args = append(args, url)

	cmd := exec.Command(wrkPath, args...)
	if method == "POST" && body != "" {
		cmd.Stdin = strings.NewReader(fmt.Sprintf("wrk.method = %q\nwrk.body = %q\nwrk.headers['Content-Type'] = 'application/json'\nwrk.headers['Authorization'] = 'Bearer test-token'\n", method, body))
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("wrk output: %s", string(out))
		return wrkResult{}
	}

	return parseWrkOutput(string(out))
}

type wrkResult struct {
	Requests    uint64
	AvgLatency  time.Duration
	P50Latency  time.Duration
	P95Latency  time.Duration
	P99Latency  time.Duration
	RequestsPerSec float64
	TransferPerSec string
}

func parseWrkOutput(output string) wrkResult {
	var r wrkResult
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "Requests/sec:"):
			fmt.Sscanf(strings.TrimPrefix(line, "Requests/sec:"), "%f", &r.RequestsPerSec)
		case strings.Contains(line, "requests in"):
			fmt.Sscanf(line, "%d requests in", &r.Requests)
		case strings.HasPrefix(line, "Latency"):
			// skip header
		case strings.Contains(line, "usec") || strings.Contains(line, "msec"):
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val := parts[0]
				var ns float64
				if strings.HasSuffix(val, "usec") {
					fmt.Sscanf(strings.TrimSuffix(val, "usec"), "%f", &ns)
					r.AvgLatency = time.Duration(ns * 1000)
				} else if strings.HasSuffix(val, "msec") {
					fmt.Sscanf(strings.TrimSuffix(val, "msec"), "%f", &ns)
					r.AvgLatency = time.Duration(ns * float64(time.Millisecond))
				}
			}
		}
	}
	return r
}

// ============================================================
// TEST: wrk load test — fh vs REF (HTTP level)
// ============================================================

func TestWrkLoadTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping wrk load test")
	}

	if _, err := exec.LookPath("wrk"); err != nil {
		t.Skip("wrk not installed")
	}

	const (
		duration    = 10 * time.Second
		connections = 100
		threads     = 8
	)

	body := `{"url":"https://go.dev","title":"Go Language","tags":["golang"]}`

	// ── fh server ──
	t.Run("fh_post", func(t *testing.T) {
		app := newFhApp()
		addr := startApp(t, app)
		time.Sleep(200 * time.Millisecond)
		r := runWrk(t, addr, "POST", "/bookmarks", body, duration, connections, threads)
		t.Logf("fh POST: %d requests, %.0f req/s, avg %v", r.Requests, r.RequestsPerSec, r.AvgLatency)
	})

	t.Run("fh_get", func(t *testing.T) {
		app := newFhApp()
		addr := startApp(t, app)
		time.Sleep(200 * time.Millisecond)
		r := runWrk(t, addr, "GET", "/bookmarks", "", duration, connections, threads)
		t.Logf("fh GET:  %d requests, %.0f req/s, avg %v", r.Requests, r.RequestsPerSec, r.AvgLatency)
	})

	t.Run("fh_health", func(t *testing.T) {
		app := newFhApp()
		addr := startApp(t, app)
		time.Sleep(200 * time.Millisecond)
		r := runWrk(t, addr, "GET", "/health", "", duration, connections, threads)
		t.Logf("fh /health: %d requests, %.0f req/s, avg %v", r.Requests, r.RequestsPerSec, r.AvgLatency)
	})

	// ── fh with REF engine mounted ──
	t.Run("ref_post", func(t *testing.T) {
		app := newFhApp()
		engine := ref.NewEngine(
			ref.WithCapability(capability.NewAuthCapability("auth.bench", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
				return capability.PrincipalFact{ID: "usr-1"}, nil
			})),
		)
		if err := ref.Register(engine, CreateBookmarkIntent{}); err != nil {
			t.Fatal(err)
		}
		if err := engine.Compile(); err != nil {
			t.Fatal(err)
		}

		app.Post("/ref/bookmarks", func(c fh.Ctx) error {
			inv := &invocation.Invocation{
				ID:     invocation.ID(fmt.Sprintf("req-%d", time.Now().UnixNano())),
				Intent: "bookmark.create",
				Input:  ref.NewInput(c.Body(), "application/json"),
				Principal: invocation.PrincipalHint{
					BearerToken: c.Get("Authorization"),
				},
			}
			result, err := engine.Dispatch(c.Context(), inv)
			if err != nil {
				return c.Status(500).JSON(map[string]string{"error": err.Error()})
			}
			return c.Status(201).JSON(result.Value)
		})

		addr := startApp(t, app)
		time.Sleep(200 * time.Millisecond)
		r := runWrk(t, addr, "POST", "/ref/bookmarks", body, duration, connections, threads)
		t.Logf("REF POST: %d requests, %.0f req/s, avg %v", r.Requests, r.RequestsPerSec, r.AvgLatency)
	})
}

// ============================================================
// TEST: In-process comparison (no HTTP overhead)
// ============================================================

func TestInProcessComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in-process comparison")
	}

	const (
		totalRequests = 100000
		concurrency   = 100
	)

	var fhMetrics, refMetrics *ObservableMetrics

	// ── fh handler directly ──
	t.Run("fh_handler", func(t *testing.T) {
		app := newFhApp()
		addr := startApp(t, app)
		time.Sleep(200 * time.Millisecond)

		client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
			MaxIdleConns:        concurrency,
			MaxIdleConnsPerHost: concurrency,
			IdleConnTimeout:     90 * time.Second,
		}}

		runtime.GC()
		metrics := NewObservableMetrics()
		sampler := NewSystemSampler(metrics, 100*time.Millisecond)
		throughput := NewThroughputSampler(metrics, 1*time.Second)
		metrics.StartTime = time.Now()
		sampler.Start()
		throughput.Start()

		var wg sync.WaitGroup
		sem := make(chan struct{}, concurrency)
		body := `{"url":"https://go.dev","title":"Go","tags":["golang"]}`

		for i := 0; i < totalRequests; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				req, _ := http.NewRequest("POST", "http://"+addr+"/bookmarks", bytes.NewBufferString(body))
				req.Header.Set("Authorization", "Bearer test-token")
				req.Header.Set("Content-Type", "application/json")
				start := time.Now()
				resp, err := client.Do(req)
				metrics.RecordLatency(time.Since(start).Nanoseconds())
				if err != nil || resp.StatusCode != 201 {
					metrics.RecordError()
				} else {
					metrics.RecordSuccess()
				}
				if resp != nil {
					resp.Body.Close()
				}
			}()
		}
		wg.Wait()
		metrics.EndTime = time.Now()
		sampler.Stop()
		throughput.Stop()
		fhMetrics = metrics
		PrintFullReport("fh HTTP Handler", metrics)
	})

	// ── REF engine dispatch ──
	t.Run("ref_dispatch", func(t *testing.T) {
		engine := ref.NewEngine(
			ref.WithCapability(capability.NewAuthCapability("auth.bench", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
				return capability.PrincipalFact{ID: "usr-1"}, nil
			})),
		)
		if err := ref.Register(engine, CreateBookmarkIntent{}); err != nil {
			t.Fatal(err)
		}
		if err := engine.Compile(); err != nil {
			t.Fatal(err)
		}

		inv := &invocation.Invocation{
			ID:     "bench",
			Intent: "bookmark.create",
			Input:  ref.NewInput([]byte(`{"url":"https://go.dev","title":"Go","tags":["golang"]}`), "application/json"),
			Principal: invocation.PrincipalHint{BearerToken: "token"},
		}
		ctx := context.Background()

		runtime.GC()
		metrics := NewObservableMetrics()
		sampler := NewSystemSampler(metrics, 100*time.Millisecond)
		throughput := NewThroughputSampler(metrics, 1*time.Second)
		metrics.StartTime = time.Now()
		sampler.Start()
		throughput.Start()

		var wg sync.WaitGroup
		sem := make(chan struct{}, concurrency)

		for i := 0; i < totalRequests; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				start := time.Now()
				_, err := engine.Dispatch(ctx, inv)
				metrics.RecordLatency(time.Since(start).Nanoseconds())
				if err != nil {
					metrics.RecordError()
				} else {
					metrics.RecordSuccess()
				}
			}()
		}
		wg.Wait()
		metrics.EndTime = time.Now()
		sampler.Stop()
		throughput.Stop()
		refMetrics = metrics
		PrintFullReport("REF Engine Dispatch", metrics)
	})

	if fhMetrics != nil && refMetrics != nil {
		PrintComparisonReport(fhMetrics, refMetrics)
	}
}

// ============================================================
// TEST: Stress test
// ============================================================

func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test")
	}

	app := newFhApp()
	addr := startApp(t, app)
	time.Sleep(200 * time.Millisecond)

	// ── fh stress ──
	t.Run("fh_stress", func(t *testing.T) {
		client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
		}}

		body := `{"url":"https://go.dev","title":"Go","tags":["golang"]}`
		runtime.GC()
		metrics := NewObservableMetrics()
		sampler := NewSystemSampler(metrics, 100*time.Millisecond)
		throughput := NewThroughputSampler(metrics, 1*time.Second)
		metrics.StartTime = time.Now()
		sampler.Start()
		throughput.Start()

		var wg sync.WaitGroup
		for i := 0; i < 500; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 200; j++ {
					req, _ := http.NewRequest("POST", "http://"+addr+"/bookmarks", bytes.NewBufferString(body))
					req.Header.Set("Authorization", "Bearer test-token")
					req.Header.Set("Content-Type", "application/json")
					start := time.Now()
					resp, err := client.Do(req)
					metrics.RecordLatency(time.Since(start).Nanoseconds())
					if err != nil || resp.StatusCode != 201 {
						metrics.RecordError()
					} else {
						metrics.RecordSuccess()
					}
					if resp != nil {
						resp.Body.Close()
					}
				}
			}()
		}
		wg.Wait()
		metrics.EndTime = time.Now()
		sampler.Stop()
		throughput.Stop()
		PrintFullReport("fh Stress (500 goroutines × 200)", metrics)
		PrintTimelineChart("fh Stress", metrics)
	})

	// ── REF stress ──
	t.Run("ref_stress", func(t *testing.T) {
		engine := ref.NewEngine(
			ref.WithCapability(capability.NewAuthCapability("auth.stress", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
				return capability.PrincipalFact{ID: "usr-1"}, nil
			})),
		)
		if err := ref.Register(engine, CreateBookmarkIntent{}); err != nil {
			t.Fatal(err)
		}
		if err := engine.Compile(); err != nil {
			t.Fatal(err)
		}

		inv := &invocation.Invocation{
			ID:     "stress",
			Intent: "bookmark.create",
			Input:  ref.NewInput([]byte(`{"url":"https://go.dev","title":"Go","tags":["golang"]}`), "application/json"),
			Principal: invocation.PrincipalHint{BearerToken: "token"},
		}
		ctx := context.Background()

		runtime.GC()
		metrics := NewObservableMetrics()
		sampler := NewSystemSampler(metrics, 100*time.Millisecond)
		throughput := NewThroughputSampler(metrics, 1*time.Second)
		metrics.StartTime = time.Now()
		sampler.Start()
		throughput.Start()

		var wg sync.WaitGroup
		for i := 0; i < 500; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 200; j++ {
					start := time.Now()
					_, err := engine.Dispatch(ctx, inv)
					metrics.RecordLatency(time.Since(start).Nanoseconds())
					if err != nil {
						metrics.RecordError()
					} else {
						metrics.RecordSuccess()
					}
				}
			}()
		}
		wg.Wait()
		metrics.EndTime = time.Now()
		sampler.Stop()
		throughput.Stop()
		PrintFullReport("REF Stress (500 goroutines × 200)", metrics)
		PrintTimelineChart("REF Stress", metrics)
	})
}
