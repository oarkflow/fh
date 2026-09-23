package ref_test

import (
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

type tcpBenchmarkServer struct {
	app    *fh.App
	url    string
	client *http.Client
}

func startTCPBenchmarkServer(tb testing.TB, app *fh.App) *tcpBenchmarkServer {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen loopback: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- app.Serve(listener) }()
	client := &http.Client{Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 256, MaxConnsPerHost: 256, IdleConnTimeout: 30 * time.Second}}
	server := &tcpBenchmarkServer{app: app, url: "http://" + listener.Addr().String() + benchPath, client: client}
	tb.Cleanup(func() {
		client.CloseIdleConnections()
		_ = listener.Close()
		_ = app.ShutdownWithTimeout(time.Second)
		select {
		case <-errCh:
		case <-time.After(time.Second):
			tb.Errorf("TCP server did not stop")
		}
	})
	return server
}
func (s *tcpBenchmarkServer) request() (int, error) {
	req, err := http.NewRequest(http.MethodPost, s.url, strings.NewReader(benchPayload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+benchToken)
	req.Header.Set("X-Request-ID", "bench-request-0001")
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	_, readErr := io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, readErr
}

// Opt-in real loopback TCP load test. Unlike the net.Pipe benchmark, this
// includes TCP sockets and the standard Go client connection pool. Run only
// when loopback binding is permitted: FH_TCP_LOAD=1 go test -run TestHTTPParityTCPLoad.
func TestHTTPParityTCPLoad(t *testing.T) {
	if os.Getenv("FH_TCP_LOAD") != "1" {
		t.Skip("set FH_TCP_LOAD=1 to open local TCP listeners")
	}
	fhApp, refApp := buildBenchmarkApps(t)
	baseline := startTCPBenchmarkServer(t, fhApp)
	newREF := startTCPBenchmarkServer(t, refApp)
	duration := 3 * time.Second
	if raw := os.Getenv("FH_LOAD_DURATION"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatal(err)
		}
		duration = d
	}
	variants := []struct {
		name   string
		server *tcpBenchmarkServer
	}{{"fh/traditional", baseline}, {"fh/ref", newREF}}
	for tierIndex, tier := range []struct {
		name    string
		workers int
	}{{"single", 1}, {"stress", 32}} {
		ordered := append([]struct {
			name   string
			server *tcpBenchmarkServer
		}(nil), variants...)
		if tierIndex%2 == 1 {
			ordered[0], ordered[1] = ordered[1], ordered[0]
		}
		for _, variant := range ordered {
			t.Run(tier.name+"/"+variant.name, func(t *testing.T) {
				for i := 0; i < 100; i++ {
					status, err := variant.server.request()
					if err != nil || status != http.StatusOK {
						t.Fatalf("warmup status=%d err=%v", status, err)
					}
				}
				deadline := time.Now().Add(duration)
				begin := time.Now()
				latencies := make([]time.Duration, 0, 10000)
				var wg sync.WaitGroup
				var mu sync.Mutex
				var requests, failures int64
				for i := 0; i < tier.workers; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						local := make([]time.Duration, 0, 1000)
						var n, bad int64
						for time.Now().Before(deadline) {
							at := time.Now()
							status, err := variant.server.request()
							local = append(local, time.Since(at))
							n++
							if err != nil || status != http.StatusOK {
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
				elapsed := time.Since(begin)
				if len(latencies) == 0 {
					t.Fatal("no requests completed")
				}
				sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
				pct := func(p float64) time.Duration { return latencies[int(float64(len(latencies)-1)*p)] }
				t.Logf("workers=%d requests=%d failures=%d throughput=%.0f req/s p50=%s p95=%s p99=%s", tier.workers, requests, failures, float64(requests)/elapsed.Seconds(), pct(.5), pct(.95), pct(.99))
				if failures != 0 {
					t.Fatalf("%d requests failed", failures)
				}
			})
		}
	}
}
