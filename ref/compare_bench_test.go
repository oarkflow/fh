package ref_test

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	refhttp "github.com/oarkflow/fh/ref/transport/http"
)

type BenchInput struct {
	ID string `json:"id"`
}
type BenchOutput struct {
	Result string `json:"result"`
}

const benchToken = "bench-token"
const benchUser = "usr-bench"
const benchPath = "/bench"
const benchPayload = "{\"id\":\"bench-123\"}"

func authenticateBenchToken(token string) (string, bool) {
	if token != benchToken {
		return "", false
	}
	return benchUser, true
}
func runBenchApplication(input BenchInput, principal string) BenchOutput {
	return BenchOutput{Result: principal + ":" + input.ID}
}
func extractBenchBearer(header string) string {
	if len(header) > 7 && strings.EqualFold(header[:7], "Bearer ") {
		return strings.TrimSpace(header[7:])
	}
	return ""
}

func fhBenchmarkHandler(c fh.Ctx) error {
	principal, ok := authenticateBenchToken(extractBenchBearer(c.Get("Authorization")))
	if !ok {
		return c.Status(stdhttp.StatusUnauthorized).JSON(map[string]string{"error": "unauthorized"})
	}
	var input BenchInput
	if err := c.BodyParser(&input); err != nil {
		return c.Status(stdhttp.StatusBadRequest).JSON(map[string]string{"error": "bad input"})
	}
	return c.JSON(runBenchApplication(input, principal))
}

type BenchIntent struct{}

func (BenchIntent) Name() intent.Name { return "bench.intent" }
func (BenchIntent) Spec() intent.Spec {
	return intent.Spec{Requires: []fact.AnyKey{capability.PrincipalKey.Any()}}
}
func (BenchIntent) Run(nc *execution.NodeContext, input BenchInput) (intent.Outcome[BenchOutput], error) {
	principal, err := execution.Require(nc, capability.PrincipalKey)
	if err != nil {
		return intent.Outcome[BenchOutput]{}, err
	}
	return intent.Outcome[BenchOutput]{Value: runBenchApplication(input, principal.ID), Effects: effect.EffectPlan{}}, nil
}

type benchmarkListener struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func newBenchmarkListener() *benchmarkListener {
	return &benchmarkListener{conns: make(chan net.Conn, 1024), done: make(chan struct{})}
}
func (l *benchmarkListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, io.EOF
	}
}
func (l *benchmarkListener) Close() error { l.once.Do(func() { close(l.done) }); return nil }
func (*benchmarkListener) Addr() net.Addr { return &net.IPAddr{IP: net.IPv4(127, 0, 0, 1)} }

type benchmarkServer struct {
	app      *fh.App
	listener *benchmarkListener
}

func startBenchmarkServer(tb testing.TB, app *fh.App) *benchmarkServer {
	tb.Helper()
	listener := newBenchmarkListener()
	errCh := make(chan error, 1)
	go func() { errCh <- app.Serve(listener) }()
	server := &benchmarkServer{app: app, listener: listener}
	tb.Cleanup(func() {
		_ = listener.Close()
		_ = app.ShutdownWithTimeout(time.Second)
		select {
		case <-errCh:
		case <-time.After(time.Second):
			tb.Errorf("fh server did not stop")
		}
	})
	return server
}
func (s *benchmarkServer) request(tb testing.TB) (int, []byte, error) {
	client, server := net.Pipe()
	select {
	case s.listener.conns <- server:
	case <-s.listener.done:
		_ = client.Close()
		_ = server.Close()
		return 0, nil, io.ErrClosedPipe
	}
	req, err := stdhttp.NewRequest(stdhttp.MethodPost, "http://benchmark.local"+benchPath, strings.NewReader(benchPayload))
	if err != nil {
		_ = client.Close()
		_ = server.Close()
		return 0, nil, err
	}
	req.Close = true
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+benchToken)
	req.Header.Set("X-Request-ID", "bench-request-0001")
	if err = req.Write(client); err != nil {
		_ = client.Close()
		return 0, nil, err
	}
	resp, err := stdhttp.ReadResponse(bufio.NewReader(client), req)
	if err != nil {
		_ = client.Close()
		return 0, nil, err
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	_ = client.Close()
	return resp.StatusCode, body, err
}
func buildBenchmarkApps(tb testing.TB) (*fh.App, *fh.App) {
	tb.Helper()
	engine := ref.NewEngine(ref.WithCapability(capability.NewAuthCapability("auth.bench", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
		principal, ok := authenticateBenchToken(hint.BearerToken)
		if !ok {
			return capability.PrincipalFact{}, intent.Failure{Code: "INVALID_TOKEN", Category: intent.CategoryAuth, Message: "unauthorized"}
		}
		return capability.PrincipalFact{ID: principal}, nil
	})))
	if err := ref.Register(engine, BenchIntent{}); err != nil {
		tb.Fatalf("register intent: %v", err)
	}
	if err := engine.Compile(); err != nil {
		tb.Fatalf("compile intent: %v", err)
	}
	traditional := fh.NewFast()
	traditional.Post(benchPath, fhBenchmarkHandler)
	withREF := fh.NewFast()
	withREF.Post(benchPath, refhttp.Adapter(engine, "bench.intent"))
	return traditional, withREF
}

func benchmarkServers(tb testing.TB) (*benchmarkServer, *benchmarkServer) {
	traditional, withREF := buildBenchmarkApps(tb)
	return startBenchmarkServer(tb, traditional), startBenchmarkServer(tb, withREF)
}

func TestHTTPParity(t *testing.T) {
	traditional, withREF := benchmarkServers(t)
	statusA, bodyA, err := traditional.request(t)
	if err != nil {
		t.Fatal(err)
	}
	statusB, bodyB, err := withREF.request(t)
	if err != nil {
		t.Fatal(err)
	}
	if statusA != stdhttp.StatusOK || statusB != stdhttp.StatusOK {
		t.Fatalf("status mismatch: fh=%d REF=%d", statusA, statusB)
	}
	var gotA, gotB BenchOutput
	if err := json.Unmarshal(bodyA, &gotA); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(bodyB, &gotB); err != nil {
		t.Fatal(err)
	}
	if gotA != gotB || gotA.Result != benchUser+":bench-123" {
		t.Fatalf("response mismatch: fh=%+v REF=%+v", gotA, gotB)
	}
}

func BenchmarkHTTPParity(b *testing.B) {
	traditional, withREF := benchmarkServers(b)
	for name, server := range map[string]*benchmarkServer{"fh/traditional": traditional, "fh/ref": withREF} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(benchPayload)))
			b.ResetTimer()
			for b.Loop() {
				status, _, err := server.request(b)
				if err != nil {
					b.Fatal(err)
				}
				if status != stdhttp.StatusOK {
					b.Fatalf("status %d", status)
				}
			}
		})
	}
}
func BenchmarkHTTPParityParallel(b *testing.B) {
	traditional, withREF := benchmarkServers(b)
	for name, server := range map[string]*benchmarkServer{"fh/traditional": traditional, "fh/ref": withREF} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(benchPayload)))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					status, _, err := server.request(b)
					if err != nil {
						b.Errorf("request: %v", err)
						return
					}
					if status != stdhttp.StatusOK {
						b.Errorf("status %d", status)
						return
					}
				}
			})
		})
	}
}

// Opt-in in-process load test: persistent fh server, HTTP/1 request and
// response serialization over net.Pipe, same endpoints and payload. Excludes
// TCP kernel, TLS, external client, and NIC costs. Set FH_LOAD=1 to run.
func TestHTTPParityLoad(t *testing.T) {
	if os.Getenv("FH_LOAD") != "1" {
		t.Skip("set FH_LOAD=1 to run")
	}
	traditional, withREF := benchmarkServers(t)
	for tierIndex, tier := range []struct {
		name    string
		workers int
	}{{"single", 1}, {"low", 4}, {"medium", 16}, {"high", 64}} {
		variants := []struct {
			name   string
			server *benchmarkServer
		}{{"fh/traditional", traditional}, {"fh/ref", withREF}}
		if tierIndex%2 == 1 {
			variants[0], variants[1] = variants[1], variants[0]
		}
		for _, variant := range variants {
			name, server := variant.name, variant.server
			t.Run(tier.name+"/"+name, func(t *testing.T) {
				duration := 1200 * time.Millisecond
				if raw := os.Getenv("FH_LOAD_DURATION"); raw != "" {
					d, err := time.ParseDuration(raw)
					if err != nil {
						t.Fatal(err)
					}
					duration = d
				}
				var wg sync.WaitGroup
				var mu sync.Mutex
				latencies := make([]time.Duration, 0, tier.workers*1000)
				var requests, failures int64
				start := time.Now()
				deadline := start.Add(duration)
				for i := 0; i < tier.workers; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						local := make([]time.Duration, 0, 1000)
						var n, bad int64
						for time.Now().Before(deadline) {
							began := time.Now()
							status, _, err := server.request(t)
							local = append(local, time.Since(began))
							n++
							if err != nil || status != stdhttp.StatusOK {
								bad++
							}
						}
						mu.Lock()
						latencies = append(latencies, local...)
						requests += n
						failures += bad
						mu.Unlock()
					}()
				}
				wg.Wait()
				elapsed := time.Since(start)
				if len(latencies) == 0 {
					t.Fatal("no requests")
				}
				sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
				pct := func(p float64) time.Duration { return latencies[int(float64(len(latencies)-1)*p)] }
				t.Logf("workers=%d requests=%d failures=%d elapsed=%s throughput=%.0f req/s p50=%s p95=%s p99=%s", tier.workers, requests, failures, elapsed, float64(requests)/elapsed.Seconds(), pct(.50), pct(.95), pct(.99))
				if failures != 0 {
					t.Fatalf("%d failed requests", failures)
				}
			})
		}
	}
}

// Three independent read-like operations with fixed 2ms service time model
// latency-bound I/O. The FH pipeline performs these sequentially; REF declares
// them as independent pre-auth-safe graph nodes so the scheduler can overlap
// the same three waits. This is a synthetic latency experiment, not a claim
// about a particular database or network service.
var tenantFact = fact.NewKey[string]("bench.tenant")
var quotaFact = fact.NewKey[string]("bench.quota")
var profileFact = fact.NewKey[string]("bench.profile")

const simulatedLookupDelay = 2 * time.Millisecond

func (BenchOverlapIntent) Name() intent.Name { return "bench.overlap" }

type BenchOverlapIntent struct{}

func (BenchOverlapIntent) Spec() intent.Spec {
	return intent.Spec{Requires: []fact.AnyKey{capability.PrincipalKey.Any(), tenantFact.Any(), quotaFact.Any(), profileFact.Any()}}
}
func (BenchOverlapIntent) Run(nc *execution.NodeContext, input BenchInput) (intent.Outcome[BenchOutput], error) {
	p, e := execution.Require(nc, capability.PrincipalKey)
	if e != nil {
		return intent.Outcome[BenchOutput]{}, e
	}
	tenant, e := execution.Require(nc, tenantFact)
	if e != nil {
		return intent.Outcome[BenchOutput]{}, e
	}
	quota, e := execution.Require(nc, quotaFact)
	if e != nil {
		return intent.Outcome[BenchOutput]{}, e
	}
	profile, e := execution.Require(nc, profileFact)
	if e != nil {
		return intent.Outcome[BenchOutput]{}, e
	}
	return intent.Outcome[BenchOutput]{Value: BenchOutput{Result: p.ID + ":" + input.ID + ":" + tenant + ":" + quota + ":" + profile}}, nil
}
func publishLookup[T any](key fact.Key[T], value T) func(*execution.NodeContext) error {
	return func(nc *execution.NodeContext) error {
		time.Sleep(simulatedLookupDelay)
		execution.Publish(nc, key, value)
		return nil
	}
}
func benchmarkOverlapServers(tb testing.TB) (*benchmarkServer, *benchmarkServer) {
	tb.Helper()
	auth := capability.NewAuthCapability("auth.bench", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
		if hint.BearerToken != benchToken {
			return capability.PrincipalFact{}, intent.Failure{Code: "INVALID_TOKEN", Category: intent.CategoryAuth, Message: "unauthorized"}
		}
		return capability.PrincipalFact{ID: benchUser}, nil
	})
	pre := capability.WithSpeculation(graph.PreAuthSafe)
	engine := ref.NewEngine(ref.WithCapability(auth), ref.WithCapability(capability.Read("read.tenant", pre).WithProvides(tenantFact.Any()).WithRun(publishLookup(tenantFact, "acme"))), ref.WithCapability(capability.Read("read.quota", pre).WithProvides(quotaFact.Any()).WithRun(publishLookup(quotaFact, "allowed"))), ref.WithCapability(capability.Read("read.profile", pre).WithProvides(profileFact.Any()).WithRun(publishLookup(profileFact, "standard"))))
	if err := ref.Register(engine, BenchOverlapIntent{}); err != nil {
		tb.Fatalf("register overlap intent: %v", err)
	}
	if err := engine.Compile(); err != nil {
		tb.Fatalf("compile overlap intent: %v", err)
	}
	traditional := fh.NewFast()
	traditional.Post(benchPath, func(c fh.Ctx) error {
		if c.Get("Authorization") != "Bearer "+benchToken {
			return c.Status(stdhttp.StatusUnauthorized).JSON(map[string]string{"error": "unauthorized"})
		}
		var in BenchInput
		if err := c.BodyParser(&in); err != nil {
			return c.Status(stdhttp.StatusBadRequest).JSON(map[string]string{"error": "bad input"})
		}
		time.Sleep(simulatedLookupDelay)
		tenant := "acme"
		time.Sleep(simulatedLookupDelay)
		quota := "allowed"
		time.Sleep(simulatedLookupDelay)
		profile := "standard"
		return c.JSON(BenchOutput{Result: benchUser + ":" + in.ID + ":" + tenant + ":" + quota + ":" + profile})
	})
	withREF := fh.NewFast()
	withREF.Post(benchPath, refhttp.Adapter(engine, "bench.overlap"))
	return startBenchmarkServer(tb, traditional), startBenchmarkServer(tb, withREF)
}
func TestHTTPDependencyOverlapParity(t *testing.T) {
	traditional, withREF := benchmarkOverlapServers(t)
	a, ab, err := traditional.request(t)
	if err != nil {
		t.Fatal(err)
	}
	b, bb, err := withREF.request(t)
	if err != nil {
		t.Fatal(err)
	}
	var x, y BenchOutput
	if err = json.Unmarshal(ab, &x); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(bb, &y); err != nil {
		t.Fatal(err)
	}
	if a != b || x != y {
		t.Fatalf("dependency workload differs: fh=(%d,%+v) REF=(%d,%+v)", a, x, b, y)
	}
}
func BenchmarkHTTPDependencyOverlap(b *testing.B) {
	traditional, withREF := benchmarkOverlapServers(b)
	for n, server := range map[string]*benchmarkServer{"fh/sequential": traditional, "ref/overlap": withREF} {
		b.Run(n, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				status, _, err := server.request(b)
				if err != nil {
					b.Fatal(err)
				}
				if status != stdhttp.StatusOK {
					b.Fatalf("status %d", status)
				}
			}
		})
	}
}
func TestHTTPDependencyOverlapLoad(t *testing.T) {
	if os.Getenv("FH_LOAD") != "1" {
		t.Skip("set FH_LOAD=1 to run")
	}
	traditional, withREF := benchmarkOverlapServers(t)
	duration := time.Second
	if raw := os.Getenv("FH_LOAD_DURATION"); raw != "" {
		d, e := time.ParseDuration(raw)
		if e != nil {
			t.Fatal(e)
		}
		duration = d
	}
	for tierIndex, tier := range []struct {
		name    string
		workers int
	}{{"single", 1}, {"concurrent", 16}} {
		variants := []struct {
			name   string
			server *benchmarkServer
		}{{"fh/sequential", traditional}, {"ref/overlap", withREF}}
		if tierIndex%2 == 1 {
			variants[0], variants[1] = variants[1], variants[0]
		}
		for _, variant := range variants {
			name, server := variant.name, variant.server
			t.Run(tier.name+"/"+name, func(t *testing.T) {
				var wg sync.WaitGroup
				var mu sync.Mutex
				lat := make([]time.Duration, 0, 1000)
				var count, bad int64
				begin := time.Now()
				deadline := begin.Add(duration)
				for i := 0; i < tier.workers; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						local := make([]time.Duration, 0, 100)
						var n, f int64
						for time.Now().Before(deadline) {
							at := time.Now()
							status, _, err := server.request(t)
							local = append(local, time.Since(at))
							n++
							if err != nil || status != stdhttp.StatusOK {
								f++
							}
						}
						mu.Lock()
						lat = append(lat, local...)
						count += n
						bad += f
						mu.Unlock()
					}()
				}
				wg.Wait()
				elapsed := time.Since(begin)
				sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
				pct := func(p float64) time.Duration { return lat[int(float64(len(lat)-1)*p)] }
				t.Logf("workers=%d requests=%d failures=%d throughput=%.0f req/s p50=%s p95=%s p99=%s", tier.workers, count, bad, float64(count)/elapsed.Seconds(), pct(.5), pct(.95), pct(.99))
				if bad > 0 {
					t.Fatalf("%d requests failed", bad)
				}
			})
		}
	}
}
