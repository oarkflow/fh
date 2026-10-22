package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type scenario struct {
	name        string
	method      string
	path        string
	body        string
	contentType string
}

type server struct {
	name    string
	port    string
	dir     string
	process *exec.Cmd
}

type result struct {
	Server            string  `json:"server"`
	Scenario          string  `json:"scenario"`
	Round             int     `json:"round,omitempty"`
	Requests          uint64  `json:"requests"`
	Errors            uint64  `json:"errors"`
	RequestsPerSecond float64 `json:"requests_per_second"`
	AverageLatencyMS  float64 `json:"average_latency_ms"`
	P50LatencyMS      float64 `json:"p50_latency_ms"`
	P95LatencyMS      float64 `json:"p95_latency_ms"`
	P99LatencyMS      float64 `json:"p99_latency_ms"`
}

var scenarios = []scenario{
	{name: "plaintext", method: http.MethodGet, path: "/plaintext"},
	{name: "json", method: http.MethodGet, path: "/json"},
	{name: "params", method: http.MethodGet, path: "/users/42"},
	{name: "query", method: http.MethodGet, path: "/search?q=benchmark"},
	{name: "echo", method: http.MethodPost, path: "/echo", body: `{"message":"Hello, World!"}`, contentType: "application/json"},
	{name: "users", method: http.MethodGet, path: "/users"},
}

func main() {
	duration := flag.Duration("duration", 5*time.Second, "measurement duration per scenario")
	warmup := flag.Duration("warmup", time.Second, "warmup duration per server")
	concurrency := flag.Int("concurrency", 32, "parallel keep-alive clients per server")
	output := flag.String("output", "results/results.json", "JSON result file")
	only := flag.String("only", "", "run only one server: fh, fiber, or fasthttp")
	seed := flag.Int64("seed", 0, "server order seed; zero selects a random seed")
	scenarioNames := flag.String("scenarios", "", "comma-separated scenario names; empty runs all")
	scenarioSeed := flag.Int64("scenario-seed", 0, "shuffle scenario order with this seed; zero keeps the default order")
	rounds := flag.Int("rounds", 1, "repeat rounds > 1 interleaves servers per scenario and reports medians")
	noStart := flag.Bool("nostart", false, "assume servers are already running (profiling/debug runs)")
	flag.Parse()
	if *duration <= 0 || *warmup < 0 || *concurrency <= 0 {
		fatalf("duration and concurrency must be positive; warmup cannot be negative")
	}

	root, err := filepath.Abs(".")
	if err != nil {
		fatalf("benchmark root: %v", err)
	}
	servers := []server{
		{name: "fh", port: "3001", dir: filepath.Join(root, "servers/go/fh")},
		{name: "fiber", port: "3003", dir: filepath.Join(root, "servers/go/fiber")},
		{name: "fasthttp", port: "3004", dir: filepath.Join(root, "servers/go/fasthttp")},
	}
	if *only != "" {
		filtered := servers[:0]
		for _, candidate := range servers {
			if candidate.name == *only {
				filtered = append(filtered, candidate)
			}
		}
		if len(filtered) == 0 {
			fatalf("unknown server %q", *only)
		}
		servers = filtered
	}
	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	rand.New(rand.NewSource(*seed)).Shuffle(len(servers), func(i, j int) {
		servers[i], servers[j] = servers[j], servers[i]
	})
	runScenarios, err := selectScenarios(*scenarioNames)
	if err != nil {
		fatalf("%v", err)
	}
	if *scenarioSeed != 0 {
		rand.New(rand.NewSource(*scenarioSeed)).Shuffle(len(runScenarios), func(i, j int) {
			runScenarios[i], runScenarios[j] = runScenarios[j], runScenarios[i]
		})
	}
	fmt.Printf("Server order (seed %d):", *seed)
	for _, s := range servers {
		fmt.Printf(" %s", s.name)
	}
	fmt.Println()
	fmt.Printf("Scenario order:")
	for _, scenario := range runScenarios {
		fmt.Printf(" %s", scenario.name)
	}
	fmt.Println()
	cleanup := func() {}
	if !*noStart {
		var err error
		cleanup, err = startServers(servers)
		if err != nil {
			fatalf("start servers: %v", err)
		}
		defer cleanup()
	}

	for i := range servers {
		if !waitReady(servers[i].port, 15*time.Second) {
			fatalf("%s did not become ready on 127.0.0.1:%s", servers[i].name, servers[i].port)
		}
	}

	warmCPU(2 * time.Second)

	results := make([]result, 0, len(servers)*len(runScenarios))
	if *rounds > 1 {
		runInterleavedRounds(*rounds, *seed, *scenarioSeed, servers, runScenarios, *warmup, *duration, *concurrency, &results)
	} else {
		for i := range servers {
			baseURL := "http://127.0.0.1:" + servers[i].port
			fmt.Printf("%s: warmup %s\n", servers[i].name, warmup)
			for _, s := range runScenarios {
				if err := validate(baseURL, s); err != nil {
					fatalf("%s/%s validation: %v", servers[i].name, s.name, err)
				}
			}
			for _, s := range runScenarios {
				r := runLoad(baseURL, s, *warmup, *duration, *concurrency)
				r.Server, r.Scenario = servers[i].name, s.name
				results = append(results, r)
				fmt.Printf("%-9s %-10s %9.0f req/s  p50 %7.3f ms  p95 %7.3f ms  errors %d\n", r.Server, r.Scenario, r.RequestsPerSecond, r.P50LatencyMS, r.P95LatencyMS, r.Errors)
			}
		}
	}

	if err := os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		fatalf("create result directory: %v", err)
	}
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fatalf("encode results: %v", err)
	}
	if err := os.WriteFile(*output, append(b, '\n'), 0644); err != nil {
		fatalf("write results: %v", err)
	}
	fmt.Printf("\nWrote %d results to %s\n", len(results), *output)
	if *rounds > 1 {
		printMedianSummary(results, servers, runScenarios)
	}
}

// runInterleavedRounds measures each scenario once per server per round, with
// both scenario and server order reshuffled every round. Desktop machines run
// background tasks in multi-second bursts; sequential per-server sections can
// be unlucky for a whole section, while interleaving samples every server in
// the same time band and medians absorb isolated environmental bursts.
func runInterleavedRounds(rounds int, seed, scenarioSeed int64, servers []server, runScenarios []scenario, warmup, duration time.Duration, concurrency int, results *[]result) {
	// Validate every route once up front so mid-round validation failures do
	// not waste measurement time.
	for i := range servers {
		baseURL := "http://127.0.0.1:" + servers[i].port
		for _, s := range runScenarios {
			if err := validate(baseURL, s); err != nil {
				fatalf("%s/%s validation: %v", servers[i].name, s.name, err)
			}
		}
	}
	fmt.Printf("%d rounds x %d scenarios x %d servers; warmup %s, duration %s each\n", rounds, len(runScenarios), len(servers), warmup, duration)
	for r := 0; r < rounds; r++ {
		roundScenarios := append([]scenario(nil), runScenarios...)
		rand.New(rand.NewSource(scenarioSeed)).Shuffle(len(roundScenarios), func(i, j int) {
			roundScenarios[i], roundScenarios[j] = roundScenarios[j], roundScenarios[i]
		})
		for k, s := range roundScenarios {
			scenarioServers := append([]server(nil), servers...)
			rand.New(rand.NewSource(seed+int64(r)*1000+int64(k))).Shuffle(len(scenarioServers), func(i, j int) {
				scenarioServers[i], scenarioServers[j] = scenarioServers[j], scenarioServers[i]
			})
			for i := range scenarioServers {
				baseURL := "http://127.0.0.1:" + scenarioServers[i].port
				out := runLoad(baseURL, s, warmup, duration, concurrency)
				out.Server, out.Scenario, out.Round = scenarioServers[i].name, s.name, r
				start := len(*results)
				*results = append(*results, out)
				out2 := (*results)[start]
				fmt.Printf("[[%s]] round %d %-9s %-10s %9.0f req/s  p50 %7.3f ms  p95 %7.3f ms  errors %d\n", time.Now().Format("15:04:05.000"), out2.Round, out2.Server, out2.Scenario, out2.RequestsPerSecond, out2.P50LatencyMS, out2.P95LatencyMS, out2.Errors)
			}
		}
	}
}

func printMedianSummary(results []result, servers []server, runScenarios []scenario) {
	type key struct{ server, scenario string }
	byKey := make(map[key][]result)
	for _, r := range results {
		k := key{r.Server, r.Scenario}
		byKey[k] = append(byKey[k], r)
	}
	median := func(vals []float64) float64 {
		sort.Float64s(vals)
		return vals[len(vals)/2]
	}
	fmt.Println("\nMedian across rounds:")
	fmt.Printf("%-9s %-10s %11s %11s %11s %11s %11s\n", "server", "scenario", "req/s", "avg ms", "p50 ms", "p95 ms", "p99 ms")
	for i := range servers {
		for _, s := range runScenarios {
			rs := byKey[key{servers[i].name, s.name}]
			if len(rs) == 0 {
				continue
			}
			collect := func(f func(result) float64) []float64 {
				vals := make([]float64, len(rs))
				for i := range rs {
					vals[i] = f(rs[i])
				}
				return vals
			}
			fmt.Printf("%-9s %-10s %11.0f %11.3f %11.3f %11.3f %11.3f\n", servers[i].name, s.name,
				median(collect(func(r result) float64 { return r.RequestsPerSecond })),
				median(collect(func(r result) float64 { return r.AverageLatencyMS })),
				median(collect(func(r result) float64 { return r.P50LatencyMS })),
				median(collect(func(r result) float64 { return r.P95LatencyMS })),
				median(collect(func(r result) float64 { return r.P99LatencyMS })))
		}
	}
}

func selectScenarios(names string) ([]scenario, error) {
	if strings.TrimSpace(names) == "" {
		return append([]scenario(nil), scenarios...), nil
	}
	selected := make([]scenario, 0)
	seen := make(map[string]bool)
	for _, raw := range strings.Split(names, ",") {
		name := strings.TrimSpace(raw)
		if name == "" || seen[name] {
			return nil, fmt.Errorf("invalid or repeated scenario %q", name)
		}
		found := false
		for _, candidate := range scenarios {
			if candidate.name == name {
				selected = append(selected, candidate)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown scenario %q", name)
		}
		seen[name] = true
	}
	return selected, nil
}

func startServers(servers []server) (func(), error) {
	started := make([]server, 0, len(servers))
	cleanup := func() {
		for i := range started {
			if started[i].process != nil && started[i].process.Process != nil {
				_ = started[i].process.Process.Signal(os.Interrupt)
				_, _ = started[i].process.Process.Wait()
			}
		}
	}
	for i := range servers {
		probe, err := net.Listen("tcp", "127.0.0.1:"+servers[i].port)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("check listen address 127.0.0.1:%s: %w", servers[i].port, err)
		}
		_ = probe.Close()
		cmd := exec.Command(filepath.Join(filepath.Dir(servers[i].dir), servers[i].name+"-server"))
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		if err := cmd.Start(); err != nil {
			cleanup()
			return nil, fmt.Errorf("start %s: %w", servers[i].name, err)
		}
		servers[i].process = cmd
		started = append(started, servers[i])
		if !waitReady(servers[i].port, 5*time.Second) {
			cleanup()
			return nil, fmt.Errorf("%s did not become ready on 127.0.0.1:%s", servers[i].name, servers[i].port)
		}
	}
	return cleanup, nil
}

// warmCPU busy-spins every GOMAXPROCS core for d before any server is
// measured. On CPU-governor policies like "powersave"/"schedutil", clock
// speed ramps from idle up to max over a few seconds of sustained load; the
// server measured first would otherwise absorb that ramp-up and appear far
// slower than servers measured later against an already-boosted CPU. This
// primes the CPU once, up front, so scenario order does not bias results.
func warmCPU(d time.Duration) {
	n := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup
	deadline := time.Now().Add(d)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x := 1.0001
			for time.Now().Before(deadline) {
				for i := 0; i < 1<<16; i++ {
					x = x*1.0000001 + 1
				}
			}
			runtime.KeepAlive(x)
		}()
	}
	wg.Wait()
}

func waitReady(port string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 250*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func newClient(concurrency int) *http.Client {
	transport := &http.Transport{
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
		MaxIdleConns:        concurrency,
		MaxIdleConnsPerHost: concurrency,
		IdleConnTimeout:     30 * time.Second,
	}
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}
}

func validate(baseURL string, s scenario) error {
	var body io.Reader
	if s.body != "" {
		body = strings.NewReader(s.body)
	}
	req, err := http.NewRequest(s.method, baseURL+s.path, body)
	if err != nil {
		return err
	}
	if s.contentType != "" {
		req.Header.Set("Content-Type", s.contentType)
	}
	resp, err := (&http.Client{Transport: &http.Transport{DisableCompression: true, ForceAttemptHTTP2: false}}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d, body %q", resp.StatusCode, data)
	}
	return validatePayload(s, data)
}

func validatePayload(s scenario, data []byte) error {
	if s.name == "plaintext" {
		if string(data) != "Hello, World!" {
			return fmt.Errorf("unexpected plaintext body %q", data)
		}
		return nil
	}
	var got any
	if err := json.Unmarshal(data, &got); err != nil {
		return fmt.Errorf("invalid JSON body %q: %w", data, err)
	}
	var want any
	switch s.name {
	case "json":
		want = map[string]any{"message": "Hello, World!"}
	case "params":
		want = map[string]any{"id": float64(0), "name": "User 42"}
	case "query":
		want = map[string]any{"query": "benchmark"}
	case "echo":
		if err := json.Unmarshal([]byte(s.body), &want); err != nil {
			return fmt.Errorf("invalid benchmark echo body: %w", err)
		}
	case "users":
		users := make([]any, 100)
		for i := range users {
			users[i] = map[string]any{"id": float64(i + 1), "name": fmt.Sprintf("User %d", i+1)}
		}
		want = users
	default:
		return fmt.Errorf("unknown scenario %q", s.name)
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("unexpected %s response %q", s.name, data)
	}
	return nil
}

func runLoad(baseURL string, s scenario, warmup, duration time.Duration, concurrency int) result {
	var requests, errors atomic.Uint64
	latencies := make(chan float64, concurrency*128)
	values := make([]float64, 0, concurrency*128)
	collectorDone := make(chan struct{})
	go func() {
		for latency := range latencies {
			values = append(values, latency)
		}
		close(collectorDone)
	}()
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	ready.Add(concurrency)
	start := make(chan struct{})
	var measureStart time.Time
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := newClient(1)
			warmupDeadline := time.Now().Add(warmup)
			for time.Now().Before(warmupDeadline) {
				var body io.Reader
				if s.body != "" {
					body = strings.NewReader(s.body)
				}
				req, err := http.NewRequest(s.method, baseURL+s.path, body)
				if err != nil {
					errors.Add(1)
					continue
				}
				if s.contentType != "" {
					req.Header.Set("Content-Type", s.contentType)
				}
				resp, err := client.Do(req)
				if err != nil {
					continue
				}
				_, readErr := io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				_ = readErr
			}
			ready.Done()
			<-start
			deadline := measureStart.Add(duration)
			for time.Now().Before(deadline) {
				var body io.Reader
				if s.body != "" {
					body = strings.NewReader(s.body)
				}
				req, err := http.NewRequest(s.method, baseURL+s.path, body)
				if err != nil {
					errors.Add(1)
					continue
				}
				if s.contentType != "" {
					req.Header.Set("Content-Type", s.contentType)
				}
				started := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					errors.Add(1)
					continue
				}
				_, readErr := io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if readErr != nil || resp.StatusCode != http.StatusOK {
					errors.Add(1)
				}
				requests.Add(1)
				latencies <- float64(time.Since(started).Microseconds()) / 1000
			}
		}()
	}
	ready.Wait()
	measureStart = time.Now()
	close(start)
	wg.Wait()
	close(latencies)
	<-collectorDone
	sort.Float64s(values)
	if len(values) == 0 {
		return result{Requests: requests.Load(), Errors: errors.Load()}
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	percentile := func(p float64) float64 { index := int(p * float64(len(values)-1)); return values[index] }
	return result{Requests: requests.Load(), Errors: errors.Load(), RequestsPerSecond: float64(requests.Load()) / duration.Seconds(), AverageLatencyMS: sum / float64(len(values)), P50LatencyMS: percentile(.50), P95LatencyMS: percentile(.95), P99LatencyMS: percentile(.99)}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "benchmark: "+format+"\n", args...)
	os.Exit(1)
}
