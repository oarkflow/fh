package main

import (
	"context"
	"debug/buildinfo"
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
	"strconv"
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
	name       string
	port       string
	dir        string
	binaryPath string
	process    *exec.Cmd
}

type result struct {
	Run                runMetadata `json:"run"`
	ServerBuild        serverBuild `json:"server_build"`
	Server             string      `json:"server"`
	Scenario           string      `json:"scenario"`
	Round              int         `json:"round,omitempty"`
	Requests           uint64      `json:"requests"`
	Errors             uint64      `json:"errors"`
	RequestsPerSecond  float64     `json:"requests_per_second"`
	AverageLatencyMS   float64     `json:"average_latency_ms"`
	P50LatencyMS       float64     `json:"p50_latency_ms"`
	P95LatencyMS       float64     `json:"p95_latency_ms"`
	P99LatencyMS       float64     `json:"p99_latency_ms"`
	AllocatedBytes     uint64      `json:"allocated_bytes"`
	AllocBytesPerReq   float64     `json:"alloc_bytes_per_request"`
	HeapAllocBefore    uint64      `json:"heap_alloc_before_bytes"`
	HeapAllocAfter     uint64      `json:"heap_alloc_after_bytes"`
	HeapAllocDelta     int64       `json:"heap_alloc_delta_bytes"`
	GCDelta            uint32      `json:"gc_count_delta"`
	GoCPUSeconds       float64     `json:"go_cpu_seconds"`
	GoCPUMSPerReq      float64     `json:"go_cpu_ms_per_request"`
	GoCPUAvailable     bool        `json:"go_cpu_available"`
	RSSBeforeBytes     uint64      `json:"rss_before_bytes"`
	RSSAfterBytes      uint64      `json:"rss_after_bytes"`
	RSSDeltaBytes      int64       `json:"rss_delta_bytes"`
	RSSAvailable       bool        `json:"rss_available"`
	RSSStatsError      string      `json:"rss_stats_error,omitempty"`
	ResourceStatsError string      `json:"resource_stats_error,omitempty"`
}

type serverBuild struct {
	GoVersion      string `json:"go_version"`
	GOOS           string `json:"goos"`
	GOARCH         string `json:"goarch"`
	LogicalCPUs    int    `json:"logical_cpus"`
	GOMAXPROCS     int    `json:"gomaxprocs"`
	MainModule     string `json:"main_module,omitempty"`
	MainVersion    string `json:"main_version,omitempty"`
	SourceRevision string `json:"source_revision,omitempty"`
	SourceModified string `json:"source_modified,omitempty"`
}

// runMetadata makes each result row independently traceable without changing
// the historical top-level JSON array consumed by existing analysis scripts.
type runMetadata struct {
	StartedAtUTC      string   `json:"started_at_utc"`
	GoVersion         string   `json:"go_version"`
	GOOS              string   `json:"goos"`
	GOARCH            string   `json:"goarch"`
	LogicalCPUs       int      `json:"logical_cpus"`
	GOMAXPROCS        int      `json:"gomaxprocs"`
	Duration          string   `json:"duration"`
	Warmup            string   `json:"warmup"`
	Concurrency       int      `json:"concurrency"`
	TargetHost        string   `json:"target_host"`
	Rounds            int      `json:"rounds"`
	ServerOrderSeed   int64    `json:"server_order_seed"`
	ScenarioOrderSeed int64    `json:"scenario_order_seed"`
	ServerOrder       []string `json:"server_order"`
	Scenarios         []string `json:"scenarios"`
	OnlyServer        string   `json:"only_server,omitempty"`
	NoStart           bool     `json:"no_start"`
	FHWriteBufferSize string   `json:"fh_write_buffer_size,omitempty"`
	SourceRevision    string   `json:"source_revision,omitempty"`
	SourceModified    string   `json:"source_modified,omitempty"`
}

type runtimeSnapshot struct {
	TotalAlloc     uint64      `json:"total_alloc_bytes"`
	HeapAlloc      uint64      `json:"heap_alloc_bytes"`
	NumGC          uint32      `json:"num_gc"`
	GoCPU          float64     `json:"go_cpu_seconds"`
	GoCPUAvailable bool        `json:"go_cpu_available"`
	Build          serverBuild `json:"build"`
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
	binDir := flag.String("bin-dir", "", "directory containing prebuilt server binaries; defaults to benchmarks/servers/go")
	fhPort := flag.Int("fh-port", 3001, "port for fh")
	fiberPort := flag.Int("fiber-port", 3003, "port for Fiber")
	fasthttpPort := flag.Int("fasthttp-port", 3004, "port for fasthttp")
	targetHost := flag.String("host", "127.0.0.1", "server host for benchmark clients; use -nostart for remote servers")
	flag.Parse()
	if *duration <= 0 || *warmup < 0 || *concurrency <= 0 {
		fatalf("duration and concurrency must be positive; warmup cannot be negative")
	}
	if err := validatePorts(*fhPort, *fiberPort, *fasthttpPort); err != nil {
		fatalf("invalid benchmark ports: %v", err)
	}
	*targetHost = strings.TrimSpace(*targetHost)
	if *targetHost == "" || strings.ContainsAny(*targetHost, "/\\?#@ \t\r\n") {
		fatalf("invalid target host %q", *targetHost)
	}
	if strings.HasPrefix(*targetHost, "[") != strings.HasSuffix(*targetHost, "]") {
		fatalf("invalid target host %q", *targetHost)
	}
	if strings.HasPrefix(*targetHost, "[") {
		*targetHost = (*targetHost)[1 : len(*targetHost)-1]
	}
	if *targetHost == "" || strings.ContainsAny(*targetHost, "[]") {
		fatalf("invalid target host %q", *targetHost)
	}

	root, err := filepath.Abs(".")
	if err != nil {
		fatalf("benchmark root: %v", err)
	}
	servers := []server{
		{name: "fh", port: strconv.Itoa(*fhPort), dir: filepath.Join(root, "servers/go/fh")},
		{name: "fiber", port: strconv.Itoa(*fiberPort), dir: filepath.Join(root, "servers/go/fiber")},
		{name: "fasthttp", port: strconv.Itoa(*fasthttpPort), dir: filepath.Join(root, "servers/go/fasthttp")},
	}
	for i := range servers {
		binaryDir := *binDir
		if binaryDir == "" {
			binaryDir = filepath.Dir(servers[i].dir)
		}
		servers[i].binaryPath = filepath.Join(binaryDir, servers[i].name+"-server")
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
	metadata := runMetadata{
		StartedAtUTC:      time.Now().UTC().Format(time.RFC3339Nano),
		GoVersion:         runtime.Version(),
		GOOS:              runtime.GOOS,
		GOARCH:            runtime.GOARCH,
		LogicalCPUs:       runtime.NumCPU(),
		GOMAXPROCS:        runtime.GOMAXPROCS(0),
		Duration:          duration.String(),
		Warmup:            warmup.String(),
		Concurrency:       *concurrency,
		TargetHost:        *targetHost,
		Rounds:            *rounds,
		ServerOrderSeed:   *seed,
		ScenarioOrderSeed: *scenarioSeed,
		ServerOrder:       make([]string, len(servers)),
		Scenarios:         make([]string, len(runScenarios)),
		OnlyServer:        *only,
		NoStart:           *noStart,
		FHWriteBufferSize: os.Getenv("FH_BENCH_WRITE_BUFFER_SIZE"),
	}
	for i, server := range servers {
		metadata.ServerOrder[i] = server.name
	}
	for i, scenario := range runScenarios {
		metadata.Scenarios[i] = scenario.name
	}
	if info, ok := buildinfo.ReadFile(os.Args[0]); ok == nil {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				metadata.SourceRevision = setting.Value
			case "vcs.modified":
				metadata.SourceModified = setting.Value
			}
		}
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

	serverBuilds := make(map[string]serverBuild, len(servers))
	for i := range servers {
		if !waitReady(*targetHost, servers[i].port, 15*time.Second) {
			fatalf("%s did not become ready on %s", servers[i].name, net.JoinHostPort(*targetHost, servers[i].port))
		}
		baseURL := benchmarkBaseURL(*targetHost, servers[i].port)
		stats, err := fetchRuntimeStats(baseURL)
		if err != nil {
			fatalf("%s runtime stats endpoint: %v", servers[i].name, err)
		}
		serverBuilds[servers[i].name] = stats.Build
	}

	warmCPU(2 * time.Second)

	results := make([]result, 0, len(servers)*len(runScenarios))
	if *rounds > 1 {
		runInterleavedRounds(*rounds, *seed, *scenarioSeed, *targetHost, servers, runScenarios, *warmup, *duration, *concurrency, &results)
	} else {
		for i := range servers {
			baseURL := benchmarkBaseURL(*targetHost, servers[i].port)
			fmt.Printf("%s: warmup %s\n", servers[i].name, warmup)
			for _, s := range runScenarios {
				if err := validate(baseURL, s); err != nil {
					fatalf("%s/%s validation: %v", servers[i].name, s.name, err)
				}
			}
			for _, s := range runScenarios {
				r := runLoad(baseURL, s, *warmup, *duration, *concurrency, serverPID(servers[i]))
				r.Server, r.Scenario = servers[i].name, s.name
				results = append(results, r)
				printResult("", r)
			}
		}
	}
	for i := range results {
		results[i].Run = metadata
		results[i].ServerBuild = serverBuilds[results[i].Server]
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
func runInterleavedRounds(rounds int, seed, scenarioSeed int64, targetHost string, servers []server, runScenarios []scenario, warmup, duration time.Duration, concurrency int, results *[]result) {
	// Validate every route once up front so mid-round validation failures do
	// not waste measurement time.
	for i := range servers {
		baseURL := benchmarkBaseURL(targetHost, servers[i].port)
		for _, s := range runScenarios {
			if err := validate(baseURL, s); err != nil {
				fatalf("%s/%s validation: %v", servers[i].name, s.name, err)
			}
		}
	}
	fmt.Printf("%d rounds x %d scenarios x %d servers; warmup %s, duration %s each\n", rounds, len(runScenarios), len(servers), warmup, duration)
	for r := 0; r < rounds; r++ {
		roundScenarios := append([]scenario(nil), runScenarios...)
		roundScenarioSeed := scenarioSeed + int64(r)*0x9e3779b9
		rand.New(rand.NewSource(roundScenarioSeed)).Shuffle(len(roundScenarios), func(i, j int) {
			roundScenarios[i], roundScenarios[j] = roundScenarios[j], roundScenarios[i]
		})
		for k, s := range roundScenarios {
			scenarioServers := append([]server(nil), servers...)
			rand.New(rand.NewSource(seed+int64(r)*1000+int64(k))).Shuffle(len(scenarioServers), func(i, j int) {
				scenarioServers[i], scenarioServers[j] = scenarioServers[j], scenarioServers[i]
			})
			for i := range scenarioServers {
				baseURL := benchmarkBaseURL(targetHost, scenarioServers[i].port)
				out := runLoad(baseURL, s, warmup, duration, concurrency, serverPID(scenarioServers[i]))
				out.Server, out.Scenario, out.Round = scenarioServers[i].name, s.name, r
				start := len(*results)
				*results = append(*results, out)
				out2 := (*results)[start]
				prefix := fmt.Sprintf("[[%s]] round %d", time.Now().Format("15:04:05.000"), out2.Round)
				printResult(prefix, out2)
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
		mid := len(vals) / 2
		if len(vals)%2 == 1 {
			return vals[mid]
		}
		return (vals[mid-1] + vals[mid]) / 2
	}
	fmt.Println("\nMedian across rounds:")
	fmt.Printf("%-9s %-10s %11s %11s %11s %11s %11s %11s %11s %11s %11s\n", "server", "scenario", "req/s", "avg ms", "p50 ms", "p95 ms", "p99 ms", "alloc B/req", "Go CPU ms/req", "RSS MiB", "heap Δ KiB")
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
			goCPUSummary := "n/a"
			allGoCPUAvailable := true
			for _, sample := range rs {
				if !sample.GoCPUAvailable {
					allGoCPUAvailable = false
					break
				}
			}
			if allGoCPUAvailable {
				goCPUSummary = fmt.Sprintf("%.4f", median(collect(func(r result) float64 { return r.GoCPUMSPerReq })))
			}
			fmt.Printf("%-9s %-10s %11.0f %11.3f %11.3f %11.3f %11.3f %11.1f %11s %11.1f %+11.1f\n", servers[i].name, s.name,
				median(collect(func(r result) float64 { return r.RequestsPerSecond })),
				median(collect(func(r result) float64 { return r.AverageLatencyMS })),
				median(collect(func(r result) float64 { return r.P50LatencyMS })),
				median(collect(func(r result) float64 { return r.P95LatencyMS })),
				median(collect(func(r result) float64 { return r.P99LatencyMS })),
				median(collect(func(r result) float64 { return r.AllocBytesPerReq })),
				goCPUSummary,
				median(collect(func(r result) float64 { return float64(r.RSSAfterBytes) / (1024 * 1024) })),
				median(collect(func(r result) float64 { return float64(r.HeapAllocDelta) / 1024 })))
		}
	}
}

func printResult(prefix string, r result) {
	goCPU := "unavailable"
	if r.GoCPUAvailable {
		goCPU = fmt.Sprintf("%7.4f ms/req", r.GoCPUMSPerReq)
	}
	fmt.Printf("%s %-9s %-10s %9.0f req/s  p50 %7.3f ms  p95 %7.3f ms  alloc %6.1f B/req  Go CPU %s  RSS %7.1f MiB  heap %+7.1f KiB  GC +%d  errors %d", prefix, r.Server, r.Scenario, r.RequestsPerSecond, r.P50LatencyMS, r.P95LatencyMS, r.AllocBytesPerReq, goCPU, float64(r.RSSAfterBytes)/(1024*1024), float64(r.HeapAllocDelta)/1024, r.GCDelta, r.Errors)
	if r.ResourceStatsError != "" {
		fmt.Printf("  stats error: %s", r.ResourceStatsError)
	}
	if r.RSSStatsError != "" {
		fmt.Printf("  RSS stats error: %s", r.RSSStatsError)
	}
	fmt.Println()
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
		cmd := exec.Command(servers[i].binaryPath)
		cmd.Env = setEnvironmentValue(os.Environ(), "FH_BENCH_ADDR", "127.0.0.1:"+servers[i].port)
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		if err := cmd.Start(); err != nil {
			cleanup()
			return nil, fmt.Errorf("start %s: %w", servers[i].name, err)
		}
		servers[i].process = cmd
		started = append(started, servers[i])
		if !waitReady("127.0.0.1", servers[i].port, 5*time.Second) {
			cleanup()
			return nil, fmt.Errorf("%s did not become ready on 127.0.0.1:%s", servers[i].name, servers[i].port)
		}
	}
	return cleanup, nil
}

func validatePorts(ports ...int) error {
	seen := make(map[int]struct{}, len(ports))
	for _, port := range ports {
		if port < 1 || port > 65535 {
			return fmt.Errorf("port %d is outside 1-65535", port)
		}
		if _, exists := seen[port]; exists {
			return fmt.Errorf("port %d is specified more than once", port)
		}
		seen[port] = struct{}{}
	}
	return nil
}

func setEnvironmentValue(env []string, key, value string) []string {
	entry := key + "=" + value
	for i, current := range env {
		if strings.HasPrefix(current, key+"=") {
			env[i] = entry
			return env
		}
	}
	return append(env, entry)
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

func benchmarkBaseURL(host, port string) string {
	return "http://" + net.JoinHostPort(host, port)
}

func waitReady(host, port string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 250*time.Millisecond)
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

func fetchRuntimeStats(baseURL string) (runtimeSnapshot, error) {
	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			DisableCompression: true,
			ForceAttemptHTTP2:  false,
		},
	}
	defer client.CloseIdleConnections()
	resp, err := client.Get(baseURL + "/__bench/stats")
	if err != nil {
		return runtimeSnapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return runtimeSnapshot{}, fmt.Errorf("status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return runtimeSnapshot{}, err
	}
	var stats runtimeSnapshot
	if err := json.Unmarshal(data, &stats); err != nil {
		return runtimeSnapshot{}, err
	}
	return stats, nil
}

func serverPID(s server) int {
	if s.process == nil || s.process.Process == nil {
		return 0
	}
	return s.process.Process.Pid
}

func readProcessRSSBytes(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid pid %d", pid)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	kilobytes, err := parseRSSKilobytes(string(out))
	if err != nil {
		return 0, err
	}
	return kilobytes * 1024, nil
}

func parseRSSKilobytes(raw string) (uint64, error) {
	value := strings.TrimSpace(raw)
	kilobytes, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse RSS %q: %w", value, err)
	}
	if kilobytes > ^uint64(0)/1024 {
		return 0, fmt.Errorf("RSS value overflows bytes")
	}
	return kilobytes, nil
}

func applyRuntimeStats(out *result, before, after runtimeSnapshot) {
	if after.TotalAlloc < before.TotalAlloc {
		out.ResourceStatsError = "TotalAlloc counter decreased"
		return
	}
	out.AllocatedBytes = after.TotalAlloc - before.TotalAlloc
	if out.Requests > 0 {
		out.AllocBytesPerReq = float64(out.AllocatedBytes) / float64(out.Requests)
	}
	beforeCPUAvailable := before.GoCPUAvailable || before.GoCPU > 0
	afterCPUAvailable := after.GoCPUAvailable || after.GoCPU > 0
	if !beforeCPUAvailable || !afterCPUAvailable {
		out.GoCPUAvailable = false
	} else if after.GoCPU < before.GoCPU {
		out.ResourceStatsError = "Go CPU counter decreased"
		return
	} else {
		out.GoCPUAvailable = true
		out.GoCPUSeconds = after.GoCPU - before.GoCPU
		if out.Requests > 0 {
			out.GoCPUMSPerReq = out.GoCPUSeconds * 1000 / float64(out.Requests)
		}
	}
	out.HeapAllocBefore = before.HeapAlloc
	out.HeapAllocAfter = after.HeapAlloc
	if after.HeapAlloc >= before.HeapAlloc {
		out.HeapAllocDelta = int64(after.HeapAlloc - before.HeapAlloc)
	} else {
		out.HeapAllocDelta = -int64(before.HeapAlloc - after.HeapAlloc)
	}
	if after.NumGC >= before.NumGC {
		out.GCDelta = after.NumGC - before.NumGC
	}
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

func runLoad(baseURL string, s scenario, warmup, duration time.Duration, concurrency int, pid int) result {
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
			req, err := newLoadRequest(baseURL, s)
			if err != nil {
				errors.Add(1)
				ready.Done()
				return
			}
			warmupDeadline := time.Now().Add(warmup)
			for time.Now().Before(warmupDeadline) {
				resetLoadRequestBody(req, s)
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
				resetLoadRequestBody(req, s)
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
	statsBefore, statsBeforeErr := fetchRuntimeStats(baseURL)
	rssBefore, rssBeforeErr := readProcessRSSBytes(pid)
	measureStart = time.Now()
	close(start)
	wg.Wait()
	statsAfter, statsAfterErr := fetchRuntimeStats(baseURL)
	rssAfter, rssAfterErr := readProcessRSSBytes(pid)
	close(latencies)
	<-collectorDone
	sort.Float64s(values)
	out := result{Requests: requests.Load(), Errors: errors.Load()}
	switch {
	case statsBeforeErr != nil && statsAfterErr != nil:
		out.ResourceStatsError = fmt.Sprintf("before: %v; after: %v", statsBeforeErr, statsAfterErr)
	case statsBeforeErr != nil:
		out.ResourceStatsError = "before: " + statsBeforeErr.Error()
	case statsAfterErr != nil:
		out.ResourceStatsError = "after: " + statsAfterErr.Error()
	default:
		applyRuntimeStats(&out, statsBefore, statsAfter)
	}
	if pid <= 0 {
		out.RSSStatsError = "unavailable (no child PID)"
	} else if rssBeforeErr != nil || rssAfterErr != nil {
		out.RSSStatsError = fmt.Sprintf("before: %v; after: %v", rssBeforeErr, rssAfterErr)
	} else {
		out.RSSBeforeBytes, out.RSSAfterBytes, out.RSSAvailable = rssBefore, rssAfter, true
		if rssAfter >= rssBefore {
			out.RSSDeltaBytes = int64(rssAfter - rssBefore)
		} else {
			out.RSSDeltaBytes = -int64(rssBefore - rssAfter)
		}
	}
	if len(values) == 0 {
		return out
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	percentile := func(p float64) float64 { index := int(p * float64(len(values)-1)); return values[index] }
	out.RequestsPerSecond = float64(out.Requests) / duration.Seconds()
	out.AverageLatencyMS = sum / float64(len(values))
	out.P50LatencyMS = percentile(.50)
	out.P95LatencyMS = percentile(.95)
	out.P99LatencyMS = percentile(.99)
	return out
}

func newLoadRequest(baseURL string, s scenario) (*http.Request, error) {
	var body io.Reader
	if s.body != "" {
		body = strings.NewReader(s.body)
	}
	req, err := http.NewRequest(s.method, baseURL+s.path, body)
	if err != nil {
		return nil, err
	}
	if s.contentType != "" {
		req.Header.Set("Content-Type", s.contentType)
	}
	return req, nil
}

func resetLoadRequestBody(req *http.Request, s scenario) {
	if s.body != "" {
		req.Body = io.NopCloser(strings.NewReader(s.body))
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "benchmark: "+format+"\n", args...)
	os.Exit(1)
}
