package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server struct {
	Name   string
	Lang   string
	Port   int
	Start  func() *exec.Cmd
	Setup  func() error
	Ready  func() bool
	RunDir string
}

type BenchmarkResult struct {
	Name   string  `json:"name"`
	RPS    float64 `json:"rps"`
	AvgLat float64 `json:"avg_latency_ms"`
	P50    float64 `json:"p50_ms"`
	P95    float64 `json:"p95_ms"`
	P99    float64 `json:"p99_ms"`
	MaxLat float64 `json:"max_latency_ms"`
	Errors int     `json:"errors"`
}

var bombImgRegex = regexp.MustCompile(`Reqs/sec\s+([\d.]+)`)
var latAvgRegex = regexp.MustCompile(`Latency\s+(\d+\.?\d*[µms]*)`)
var latPctRegex = regexp.MustCompile(`(50%|95%|99%)\s+(\d+\.?\d*[µms]*)`)
var errorsRegex = regexp.MustCompile(`Errors\s+(\d+)`)

func parseDuration(d string) float64 {
	d = strings.TrimSpace(d)
	switch {
	case strings.HasSuffix(d, "ms"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(d, "ms"), 64)
		return v
	case strings.HasSuffix(d, "µs") || strings.HasSuffix(d, "us"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSuffix(d, "µs"), "us"), 64)
		return v / 1000
	case strings.HasSuffix(d, "s"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(d, "s"), 64)
		return v * 1000
	case strings.HasSuffix(d, "m"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(d, "m"), 64)
		return v * 60000
	default:
		v, _ := strconv.ParseFloat(d, 64)
		return v
	}
}

func (s Server) String() string {
	return fmt.Sprintf("%-10s [%s] :%d", s.Name, s.Lang, s.Port)
}

var servers = []Server{
	{Name: "fh", Lang: "Go", Port: 3001, RunDir: "servers/go/fh"},
	{Name: "fiber", Lang: "Go", Port: 3003, RunDir: "servers/go/fiber"},
	{Name: "fasthttp", Lang: "Go", Port: 3004, RunDir: "servers/go/fasthttp"},
}

type Scenario struct {
	Name   string
	Path   string
	Method string
	Body   string
	Header string
}

var scenarios = []Scenario{
	{Name: "Plaintext", Path: "/plaintext", Method: "GET"},
	{Name: "JSON", Path: "/json", Method: "GET"},
	{Name: "Params", Path: "/users/42", Method: "GET"},
	{Name: "Query", Path: "/search?q=benchmark", Method: "GET"},
	{Name: "Echo", Path: "/echo", Method: "POST", Body: `{"message":"Hello, World!"}`, Header: "Content-Type: application/json"},
	{Name: "Users", Path: "/users", Method: "GET"},
	{Name: "Method GET", Path: "/methods/get", Method: "GET"},
	{Name: "Method HEAD", Path: "/methods/head", Method: "HEAD"},
	{Name: "Method POST", Path: "/methods/post", Method: "POST"},
	{Name: "Method PUT", Path: "/methods/put", Method: "PUT"},
	{Name: "Method PATCH", Path: "/methods/patch", Method: "PATCH"},
	{Name: "Method DELETE", Path: "/methods/delete", Method: "DELETE"},
	{Name: "Method OPTIONS", Path: "/methods/options", Method: "OPTIONS"},
	{Name: "Method CONNECT", Path: "/methods/connect", Method: "CONNECT"},
	{Name: "Method TRACE", Path: "/methods/trace", Method: "TRACE"},
	{Name: "Method QUERY", Path: "/methods/query", Method: "QUERY"},
}

func findBombardier() string {
	paths := []string{
		"/home/sujit/go/bin/bombardier",
		filepath.Join(os.Getenv("HOME"), "go", "bin", "bombardier"),
		"bombardier",
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
		if pth, err := exec.LookPath("bombardier"); err == nil {
			return pth
		}
	}
	return "bombardier"
}

func isServerReady(port int) bool {
	cmd := exec.Command("sh", "-c", fmt.Sprintf("lsof -i :%d 2>/dev/null | grep -q LISTEN", port))
	return cmd.Run() == nil
}

func waitForServer(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isServerReady(port) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// serverCores / clientCores split the machine so the servers under test and
// the load generator do not fight for the same cores. Unpinned, both sides
// thrash the Go scheduler (25-30% of CPU in runtime lock contention at 1M+
// req/s) and run-to-run noise exceeds the real differences between frameworks.
func coreSplit() (serverCores, clientCores string, ok bool) {
	n := runtime.NumCPU()
	if n < 4 {
		return "", "", false
	}
	if _, err := exec.LookPath("taskset"); err != nil {
		return "", "", false
	}
	half := n / 2
	return fmt.Sprintf("0-%d", half-1), fmt.Sprintf("%d-%d", half, n-1), true
}

func startServer(s Server) (*exec.Cmd, error) {
	var cmd *exec.Cmd

	switch s.Lang {
	case "Go":
		absDir, _ := filepath.Abs(s.RunDir)
		if serverCores, _, ok := coreSplit(); ok {
			cmd = exec.Command("taskset", "-c", serverCores, "go", "run", ".")
		} else {
			cmd = exec.Command("go", "run", ".")
		}
		cmd.Dir = absDir
	}

	if cmd != nil {
		cmd.Stdout = nil
		cmd.Stderr = nil
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("starting %s: %w", s.Name, err)
		}
	}
	return cmd, nil
}

func runBombardier(url, method, body, header string, duration int, connections int) (BenchmarkResult, error) {
	// Use one persistent TCP driver for the entire method matrix. Bombardier has
	// method-specific client paths and rejects CONNECT, TRACE, and QUERY, which
	// would otherwise make identical dispatch workloads incomparable.
	if strings.Contains(url, "/methods/") {
		return runRawHTTP(url, method, body, header, time.Duration(duration)*time.Second, connections)
	}
	bombPath := findBombardier()
	args := []string{
		"-m", method,
		"-d", fmt.Sprintf("%ds", duration),
		"-c", fmt.Sprintf("%d", connections),
		"--print", "r",
		"--format", "json",
		"-l",
	}
	if body != "" {
		args = append(args, "-b", body)
	}
	if header != "" {
		args = append(args, "-H", header)
	}
	args = append(args, url)

	var cmd *exec.Cmd
	if _, clientCores, ok := coreSplit(); ok {
		cmd = exec.Command("taskset", append([]string{"-c", clientCores, bombPath}, args...)...)
	} else {
		cmd = exec.Command(bombPath, args...)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("bombardier %s: %w\n%s", url, err, string(output))
	}

	return parseBombardierJSON(output)
}

type rawWorkerResult struct {
	latencies []float64
	errors    int
}

type responseSnapshot struct {
	status      int
	contentType string
	body        []byte
}

func rawRequest(u *url.URL, method, body, header string) []byte {
	request := []byte(method + " " + u.RequestURI() + " HTTP/1.1\r\nHost: " + u.Host + "\r\nConnection: keep-alive\r\n")
	if header != "" {
		request = append(request, header...)
		request = append(request, '\r', '\n')
	}
	if body != "" {
		request = append(request, "Content-Length: "...)
		request = strconv.AppendInt(request, int64(len(body)), 10)
		request = append(request, '\r', '\n')
	}
	request = append(request, '\r', '\n')
	return append(request, body...)
}

func fetchRawResponse(rawURL, method, body, header string) (responseSnapshot, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return responseSnapshot{}, err
	}
	address := u.Host
	if !strings.Contains(address, ":") {
		address += ":80"
	}
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		return responseSnapshot{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err = conn.Write(rawRequest(u, method, body, header)); err != nil {
		return responseSnapshot{}, err
	}
	reader := bufio.NewReaderSize(conn, 4096)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		return responseSnapshot{}, err
	}
	parts := strings.Fields(statusLine)
	if len(parts) < 2 {
		return responseSnapshot{}, fmt.Errorf("malformed status line %q", statusLine)
	}
	status, err := strconv.Atoi(parts[1])
	if err != nil {
		return responseSnapshot{}, err
	}
	contentLength := 0
	contentType := ""
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			return responseSnapshot{}, readErr
		}
		if line == "\r\n" {
			break
		}
		name, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		switch strings.ToLower(name) {
		case "content-length":
			contentLength, _ = strconv.Atoi(strings.TrimSpace(value))
		case "content-type":
			contentType = strings.ToLower(strings.TrimSpace(value))
			if semi := strings.IndexByte(contentType, ';'); semi >= 0 {
				contentType = strings.TrimSpace(contentType[:semi])
			}
		}
	}
	if method == "HEAD" {
		contentLength = 0
	}
	responseBody := make([]byte, contentLength)
	if _, err = io.ReadFull(reader, responseBody); err != nil {
		return responseSnapshot{}, err
	}
	return responseSnapshot{status: status, contentType: contentType, body: responseBody}, nil
}

func runRawHTTP(rawURL, method, body, header string, duration time.Duration, connections int) (BenchmarkResult, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return BenchmarkResult{}, err
	}
	address := u.Host
	if !strings.Contains(address, ":") {
		address += ":80"
	}
	request := rawRequest(u, method, body, header)

	results := make(chan rawWorkerResult, connections)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(connections)
	for range connections {
		go func() {
			local := rawWorkerResult{latencies: make([]float64, 0, 4096)}
			conn, dialErr := net.DialTimeout("tcp", address, 3*time.Second)
			ready.Done()
			<-start
			if dialErr != nil {
				local.errors++
				results <- local
				return
			}
			defer conn.Close()
			reader := bufio.NewReaderSize(conn, 4096)
			deadline := time.Now().Add(duration)
			for time.Now().Before(deadline) {
				started := time.Now()
				if _, writeErr := conn.Write(request); writeErr != nil {
					local.errors++
					break
				}
				status, readErr := reader.ReadSlice('\n')
				if readErr != nil || !bytes.Contains(status, []byte(" 200 ")) {
					local.errors++
					break
				}
				contentLength := 0
				for {
					line, lineErr := reader.ReadSlice('\n')
					if lineErr != nil {
						readErr = lineErr
						break
					}
					if len(line) == 2 && line[0] == '\r' {
						break
					}
					if len(line) >= 15 && bytes.EqualFold(line[:15], []byte("Content-Length:")) {
						contentLength, _ = strconv.Atoi(strings.TrimSpace(string(line[15:])))
					}
				}
				// HEAD carries the GET Content-Length but never a response body.
				if method == "HEAD" {
					contentLength = 0
				}
				if readErr == nil && contentLength > 0 {
					_, readErr = io.CopyN(io.Discard, reader, int64(contentLength))
				}
				if readErr != nil {
					local.errors++
					break
				}
				local.latencies = append(local.latencies, float64(time.Since(started))/float64(time.Millisecond))
			}
			results <- local
		}()
	}
	ready.Wait()
	started := time.Now()
	close(start)

	var all []float64
	totalErrors := 0
	for range connections {
		result := <-results
		all = append(all, result.latencies...)
		totalErrors += result.errors
	}
	elapsed := time.Since(started).Seconds()
	if len(all) == 0 {
		return BenchmarkResult{Errors: totalErrors}, nil
	}
	sort.Float64s(all)
	var sum float64
	for _, latency := range all {
		sum += latency
	}
	percentile := func(p float64) float64 {
		idx := int(float64(len(all)-1) * p)
		return all[idx]
	}
	return BenchmarkResult{
		RPS:    float64(len(all)) / elapsed,
		AvgLat: sum / float64(len(all)),
		P50:    percentile(0.50),
		P95:    percentile(0.95),
		P99:    percentile(0.99),
		MaxLat: all[len(all)-1],
		Errors: totalErrors,
	}, nil
}

func parseBombardierJSON(data []byte) (BenchmarkResult, error) {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return parseBombardierText(data)
	}

	r := BenchmarkResult{}

	result, ok := raw["result"].(map[string]any)
	if !ok {
		return r, fmt.Errorf("no 'result' key in bombardier JSON")
	}

	// RPS
	if rr, ok := result["rps"].(map[string]any); ok {
		if mean, ok := rr["mean"].(float64); ok {
			r.RPS = mean
		}
	}

	// Latency (values are in microseconds)
	if lat, ok := result["latency"].(map[string]any); ok {
		if mean, ok := lat["mean"].(float64); ok {
			r.AvgLat = mean / 1e3 // µs to ms
		}
		if max, ok := lat["max"].(float64); ok {
			r.MaxLat = max / 1e3
		}
		if pcts, ok := lat["percentiles"].(map[string]any); ok {
			if p50, ok := pcts["50"].(float64); ok {
				r.P50 = p50 / 1e3
			}
			if p95, ok := pcts["95"].(float64); ok {
				r.P95 = p95 / 1e3
			}
			if p99, ok := pcts["99"].(float64); ok {
				r.P99 = p99 / 1e3
			}
		}
	}

	// Errors
	if rawErrs, ok := result["errors"].(map[string]any); ok {
		if n, ok := rawErrs["count"].(float64); ok {
			r.Errors = int(n)
		}
	}

	return r, nil
}

func parseBombardierText(data []byte) (BenchmarkResult, error) {
	r := BenchmarkResult{}
	text := string(data)

	if m := bombImgRegex.FindStringSubmatch(text); len(m) > 1 {
		r.RPS, _ = strconv.ParseFloat(m[1], 64)
	}

	if m := errorsRegex.FindStringSubmatch(text); len(m) > 1 {
		r.Errors, _ = strconv.Atoi(m[1])
	}

	lines := strings.Split(text, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Latency") {
			parts := strings.Fields(line)
			if len(parts) >= 4 {
				r.AvgLat = parseDuration(parts[2])
			}
		}
	}

	return r, nil
}

func killPort(port int) {
	exec.Command("sh", "-c", fmt.Sprintf("lsof -ti :%d | xargs -r kill -9 2>/dev/null", port)).Run()
}

func cleanup() {
	for _, s := range servers {
		killPort(s.Port)
	}
}

func main() {
	bombPath := findBombardier()
	fmt.Printf("Using bombardier: %s\n\n", bombPath)

	// Parse flags
	duration := 10
	connections := 100
	rounds := 3
	filter := ""
	scenarioFilter := ""

	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-d":
			if i+1 < len(args) {
				duration, _ = strconv.Atoi(args[i+1])
				i++
			}
		case "-c":
			if i+1 < len(args) {
				connections, _ = strconv.Atoi(args[i+1])
				i++
			}
		case "-n", "--rounds":
			if i+1 < len(args) {
				rounds, _ = strconv.Atoi(args[i+1])
				i++
			}
		case "--server":
			if i+1 < len(args) {
				if filter != "" {
					filter += ","
				}
				filter += strings.ToLower(args[i+1])
				i++
			}
		case "--scenario":
			if i+1 < len(args) {
				scenarioFilter = strings.ToLower(args[i+1])
				i++
			}
		case "-h", "--help":
			fmt.Println("Usage: go run main.go [flags]")
			fmt.Println("  -d SEC     Benchmark duration in seconds (default: 10)")
			fmt.Println("  -c N       Concurrent connections (default: 100)")
			fmt.Println("  -n N       Measurement rounds; median is reported (default: 3)")
			fmt.Println("  --server S Filter by server name (e.g., fh, gin)")
			fmt.Println("  --scenario S Filter by scenario name (e.g., query, echo)")
			fmt.Println("\nAvailable servers:")
			for _, s := range servers {
				fmt.Printf("  %s\n", s.String())
			}
			fmt.Println("\nScenarios:")
			for _, sc := range scenarios {
				fmt.Printf("  %s %s %s\n", sc.Name, sc.Method, sc.Path)
			}
			return
		}
	}
	if rounds < 1 {
		rounds = 1
	}
	if scenarioFilter != "" {
		filtered := make([]Scenario, 0, len(scenarios))
		for _, sc := range scenarios {
			if strings.Contains(strings.ToLower(sc.Name), scenarioFilter) {
				filtered = append(filtered, sc)
			}
		}
		scenarios = filtered
		if len(scenarios) == 0 {
			fmt.Printf("No scenarios matched %q.\n", scenarioFilter)
			return
		}
	}

	// Cleanup on exit
	defer cleanup()

	// Results collection
	type resultKey struct {
		Server   string
		Scenario string
	}
	results := make(map[resultKey]BenchmarkResult)
	samples := make(map[resultKey][]BenchmarkResult)
	type runInfo struct {
		Server string
		Port   int
		Cmd    *exec.Cmd
	}

	var toRun []Server
	filters := strings.Split(filter, ",")
	for _, s := range servers {
		if filter == "" {
			toRun = append(toRun, s)
			continue
		}
		name := strings.NewReplacer("/", "", "-", "", " ", "").Replace(strings.ToLower(s.Name))
		for _, f := range filters {
			if f != "" && (strings.Contains(strings.ToLower(s.Name), f) || strings.Contains(name, f)) {
				toRun = append(toRun, s)
				break
			}
		}
	}

	if len(toRun) == 0 {
		fmt.Println("No servers matched filter.")
		return
	}

	fmt.Printf("Benchmarking %d server(s) with %d connections for %d seconds, %d round(s) each\n\n", len(toRun), connections, duration, rounds)
	for _, s := range toRun {
		fmt.Printf("  %s\n", s.String())
	}
	fmt.Println()

	// Kill any processes on our ports first
	cleanup()
	time.Sleep(500 * time.Millisecond)

	// Start servers
	var running []runInfo
	for _, s := range toRun {
		fmt.Printf("Starting %s... ", s.Name)
		cmd, err := startServer(s)
		if err != nil {
			fmt.Printf("FAILED: %v\n", err)
			continue
		}
		if waitForServer(s.Port, 30*time.Second) {
			fmt.Printf("ready on :%d\n", s.Port)
			running = append(running, runInfo{Server: s.Name, Port: s.Port, Cmd: cmd})
		} else {
			fmt.Printf("TIMEOUT\n")
			if cmd != nil {
				cmd.Process.Kill()
			}
		}
	}

	if len(running) == 0 {
		fmt.Println("No servers could be started.")
		return
	}
	fmt.Println()

	// Correctness gate: throughput is meaningless unless successful servers
	// return byte-identical bodies and equivalent media types for each workload.
	fmt.Println("Validating apples-to-apples responses...")
	for _, sc := range scenarios {
		var reference *responseSnapshot
		for _, r := range running {
			rawURL := fmt.Sprintf("http://127.0.0.1:%d%s", r.Port, sc.Path)
			snapshot, err := fetchRawResponse(rawURL, sc.Method, sc.Body, sc.Header)
			if err != nil {
				fmt.Printf("  FAILED %s/%s: %v\n", r.Server, sc.Name, err)
				return
			}
			if snapshot.status != 200 {
				// QUERY is an extension method. Unsupported frameworks remain in
				// the result table as errors instead of invalidating other servers.
				if sc.Method == "QUERY" {
					continue
				}
				fmt.Printf("  FAILED %s/%s: status %d, want 200\n", r.Server, sc.Name, snapshot.status)
				return
			}
			if reference == nil {
				copySnapshot := snapshot
				reference = &copySnapshot
				continue
			}
			if snapshot.contentType != reference.contentType || !bytes.Equal(snapshot.body, reference.body) {
				fmt.Printf("  FAILED %s/%s: response differs (content-type %q, body %q)\n", r.Server, sc.Name, snapshot.contentType, snapshot.body)
				return
			}
		}
	}
	fmt.Println("  all successful responses match")
	fmt.Println()

	// Run scenario-first and rotate the first server across both scenarios and
	// rounds. Report the median round so transient scheduler/thermal noise cannot
	// decide a ranking from one favorable sample.
	for round := range rounds {
		fmt.Printf("######## ROUND %d/%d ########\n\n", round+1, rounds)
		for scenarioIndex, sc := range scenarios {
			fmt.Printf("=== %s (%s) ===\n", sc.Name, sc.Method)
			for offset := range len(running) {
				r := running[(scenarioIndex+round+offset)%len(running)]
				url := fmt.Sprintf("http://127.0.0.1:%d%s", r.Port, sc.Path)
				fmt.Printf("  %s... ", r.Server)
				result, err := runBombardier(url, sc.Method, sc.Body, sc.Header, duration, connections)
				if err != nil {
					fmt.Printf("ERROR: %v\n", err)
					continue
				}
				key := resultKey{r.Server, sc.Name}
				samples[key] = append(samples[key], result)
				fmt.Printf("%.0f req/s, avg %.2fms\n", result.RPS, result.AvgLat)
			}
			fmt.Println()
		}
	}
	for key, values := range samples {
		sort.Slice(values, func(i, j int) bool { return values[i].RPS < values[j].RPS })
		results[key] = values[len(values)/2]
	}

	// Print comparison table
	fmt.Println("=== COMPARISON TABLE ===")
	fmt.Println()

	for _, sc := range scenarios {
		fmt.Printf("--- %s (%s) ---\n", sc.Name, sc.Method)
		fmt.Printf("%-12s %12s %14s %10s %10s %10s %8s\n",
			"Server", "RPS", "Avg Lat (ms)", "P50 (ms)", "P95 (ms)", "P99 (ms)", "Errors")
		fmt.Println(strings.Repeat("-", 82))

		type entry struct {
			Name string
			BenchmarkResult
		}
		var entries []entry
		for _, r := range running {
			if res, ok := results[resultKey{r.Server, sc.Name}]; ok {
				entries = append(entries, entry{r.Server, res})
			}
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].RPS > entries[j].RPS
		})

		for _, e := range entries {
			fmt.Printf("%-12s %12.0f %14.3f %10.3f %10.3f %10.3f %8d\n",
				e.Name, e.RPS, e.AvgLat, e.P50, e.P95, e.P99, e.Errors)
		}
		fmt.Println()
	}

	// Save results to JSON
	outPath := filepath.Join("results", fmt.Sprintf("bench_%s.json", time.Now().Format("20060102_150405")))
	outData, _ := json.MarshalIndent(results, "", "  ")
	os.WriteFile(outPath, outData, 0644)
	fmt.Printf("Results saved to %s\n", outPath)
}
