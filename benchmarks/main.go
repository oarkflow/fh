package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	Name    string
	Lang    string
	Port    int
	Start   func() *exec.Cmd
	Setup   func() error
	Ready   func() bool
	RunDir  string
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
	{Name: "gin", Lang: "Go", Port: 3002, RunDir: "servers/go/gin"},
	{Name: "fiber", Lang: "Go", Port: 3003, RunDir: "servers/go/fiber"},
	{Name: "fasthttp", Lang: "Go", Port: 3004, RunDir: "servers/go/fasthttp"},
	{Name: "net/http", Lang: "Go", Port: 3005, RunDir: "servers/go/nethttp"},
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

func startServer(s Server) (*exec.Cmd, error) {
	var cmd *exec.Cmd

	switch s.Lang {
	case "Go":
		absDir, _ := filepath.Abs(s.RunDir)
		cmd = exec.Command("go", "run", ".")
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

	cmd := exec.Command(bombPath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("bombardier %s: %w\n%s", url, err, string(output))
	}

	return parseBombardierJSON(output)
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
	filter := ""

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
		case "--server":
			if i+1 < len(args) {
				if filter != "" {
					filter += ","
				}
				filter += strings.ToLower(args[i+1])
				i++
			}
		case "-h", "--help":
			fmt.Println("Usage: go run main.go [flags]")
			fmt.Println("  -d SEC     Benchmark duration in seconds (default: 10)")
			fmt.Println("  -c N       Concurrent connections (default: 100)")
			fmt.Println("  --server S Filter by server name (e.g., fh, gin)")
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

	// Cleanup on exit
	defer cleanup()

	// Results collection
	type resultKey struct {
		Server   string
		Scenario string
	}
	results := make(map[resultKey]BenchmarkResult)
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

	fmt.Printf("Benchmarking %d server(s) with %d connections for %d seconds each\n\n", len(toRun), connections, duration)
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

	// Run benchmarks
	for _, r := range running {
		fmt.Printf("=== %s ===\n", r.Server)
		for _, sc := range scenarios {
			url := fmt.Sprintf("http://127.0.0.1:%d%s", r.Port, sc.Path)
			fmt.Printf("  %s %s... ", sc.Name, sc.Method)
			result, err := runBombardier(url, sc.Method, sc.Body, sc.Header, duration, connections)
			if err != nil {
				fmt.Printf("ERROR: %v\n", err)
				continue
			}
			results[resultKey{r.Server, sc.Name}] = result
			fmt.Printf("%.0f req/s, avg %.2fms\n", result.RPS, result.AvgLat)
		}
		fmt.Println()
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
