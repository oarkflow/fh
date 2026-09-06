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
	fmt.Printf("Server order (seed %d):", *seed)
	for _, s := range servers {
		fmt.Printf(" %s", s.name)
	}
	fmt.Println()
	cleanup, err := startServers(servers)
	if err != nil {
		fatalf("start servers: %v", err)
	}
	defer cleanup()

	for i := range servers {
		if !waitReady(servers[i].port, 15*time.Second) {
			fatalf("%s did not become ready on 127.0.0.1:%s", servers[i].name, servers[i].port)
		}
	}

	results := make([]result, 0, len(servers)*len(scenarios))
	for i := range servers {
		baseURL := "http://127.0.0.1:" + servers[i].port
		fmt.Printf("%s: warmup %s\n", servers[i].name, warmup)
		for _, s := range scenarios {
			if err := validate(baseURL, s); err != nil {
				fatalf("%s/%s validation: %v", servers[i].name, s.name, err)
			}
		}
		for _, s := range scenarios {
			r := runLoad(baseURL, s, *warmup, *duration, *concurrency)
			r.Server, r.Scenario = servers[i].name, s.name
			results = append(results, r)
			fmt.Printf("%-9s %-10s %9.0f req/s  p50 %7.3f ms  p95 %7.3f ms  errors %d\n", r.Server, r.Scenario, r.RequestsPerSecond, r.P50LatencyMS, r.P95LatencyMS, r.Errors)
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
			return nil, fmt.Errorf("port %s is already in use", servers[i].port)
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
	switch s.name {
	case "plaintext":
		if string(data) != "Hello, World!" {
			return fmt.Errorf("unexpected body %q", data)
		}
	case "json", "params", "query", "echo", "users":
		if !json.Valid(data) {
			return fmt.Errorf("invalid JSON body %q", data)
		}
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
