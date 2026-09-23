package loadtest

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================
// ObservableMetrics captures the full system state during a test.
// ============================================================

type ObservableMetrics struct {
	// Request metrics
	TotalRequests   atomic.Int64
	SuccessCount    atomic.Int64
	ErrorCount      atomic.Int64
	TimeoutCount    atomic.Int64
	RateLimited     atomic.Int64
	TotalLatencyNs  atomic.Int64
	MinLatencyNs    atomic.Int64
	MaxLatencyNs    atomic.Int64

	// Throughput tracking (per-second snapshots)
	SecondlyRPS     []float64
	SecondlyLatency []time.Duration

	// System metrics (sampled at intervals)
	GoroutineSamples   []int
	HeapAllocSamples   []uint64
	HeapObjectsSamples []uint64
	GCCycles           []uint32
	GCPauseNs          []uint64
	ThreadSamples      []int
	MallocsSamples     []uint64
	FreesSamples       []uint64
	StackSamples       []uint64
	SysAllocSamples    []uint64

	// Timing
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration

	// CPU profiling
	CPUProfileData []byte

	// Latency histogram (microsecond buckets) - protected by mutex
	LatencyBuckets map[string]int64
	bucketMu       sync.Mutex
}

// NewObservableMetrics creates a fresh metrics collector.
func NewObservableMetrics() *ObservableMetrics {
	m := &ObservableMetrics{
		LatencyBuckets: make(map[string]int64),
	}
	m.MinLatencyNs.Store(math.MaxInt64) // start with max int64
	return m
}

// RecordLatency records a single request latency in nanoseconds.
func (m *ObservableMetrics) RecordLatency(ns int64) {
	m.TotalLatencyNs.Add(ns)
	m.TotalRequests.Add(1)

	// Update min/max using atomic CAS
	for {
		old := m.MinLatencyNs.Load()
		if ns >= old || m.MinLatencyNs.CompareAndSwap(old, ns) {
			break
		}
	}
	for {
		old := m.MaxLatencyNs.Load()
		if ns <= old || m.MaxLatencyNs.CompareAndSwap(old, ns) {
			break
		}
	}

	// Bucket for histogram (in microseconds)
	us := ns / 1000
	var bucket string
	switch {
	case us < 1:
		bucket = "<1us"
	case us < 5:
		bucket = "1-5us"
	case us < 10:
		bucket = "5-10us"
	case us < 50:
		bucket = "10-50us"
	case us < 100:
		bucket = "50-100us"
	case us < 500:
		bucket = "100-500us"
	case us < 1000:
		bucket = "500us-1ms"
	case us < 5000:
		bucket = "1-5ms"
	case us < 10000:
		bucket = "5-10ms"
	case us < 50000:
		bucket = "10-50ms"
	case us < 100000:
		bucket = "50-100ms"
	default:
		bucket = ">100ms"
	}
	m.bucketMu.Lock()
	m.LatencyBuckets[bucket]++
	m.bucketMu.Unlock()
}

// RecordSuccess increments the success counter.
func (m *ObservableMetrics) RecordSuccess() { m.SuccessCount.Add(1) }

// RecordError increments the error counter.
func (m *ObservableMetrics) RecordError() { m.ErrorCount.Add(1) }

// ============================================================
// SystemSampler periodically captures Go runtime metrics.
// ============================================================

type SystemSampler struct {
	metrics  *ObservableMetrics
	interval time.Duration
	stop     chan struct{}
	wg       sync.WaitGroup
}

// NewSystemSampler creates a sampler that captures runtime stats periodically.
func NewSystemSampler(metrics *ObservableMetrics, interval time.Duration) *SystemSampler {
	return &SystemSampler{
		metrics:  metrics,
		interval: interval,
		stop:     make(chan struct{}),
	}
}

// Start begins background sampling.
func (s *SystemSampler) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.sample()
			case <-s.stop:
				return
			}
		}
	}()
}

// Stop halts sampling and waits for the goroutine to finish.
func (s *SystemSampler) Stop() {
	close(s.stop)
	s.wg.Wait()
}

func (s *SystemSampler) sample() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	s.metrics.GoroutineSamples = append(s.metrics.GoroutineSamples, runtime.NumGoroutine())
	s.metrics.HeapAllocSamples = append(s.metrics.HeapAllocSamples, m.HeapAlloc)
	s.metrics.HeapObjectsSamples = append(s.metrics.HeapObjectsSamples, m.HeapObjects)
	s.metrics.GCCycles = append(s.metrics.GCCycles, m.NumGC)
	s.metrics.GCPauseNs = append(s.metrics.GCPauseNs, m.PauseNs[(m.NumGC+255)%256])
	s.metrics.ThreadSamples = append(s.metrics.ThreadSamples, threadCount())
	s.metrics.MallocsSamples = append(s.metrics.MallocsSamples, m.Mallocs)
	s.metrics.FreesSamples = append(s.metrics.FreesSamples, m.Frees)
	s.metrics.StackSamples = append(s.metrics.StackSamples, m.StackSys)
	s.metrics.SysAllocSamples = append(s.metrics.SysAllocSamples, m.Sys)
}

// ============================================================
// ThroughputSampler records requests-per-second snapshots.
// ============================================================

type ThroughputSampler struct {
	metrics    *ObservableMetrics
	interval   time.Duration
	lastReqs   int64
	stop       chan struct{}
	wg         sync.WaitGroup
}

// NewThroughputSampler creates a sampler that tracks RPS over time.
func NewThroughputSampler(metrics *ObservableMetrics, interval time.Duration) *ThroughputSampler {
	return &ThroughputSampler{
		metrics:  metrics,
		interval: interval,
		stop:     make(chan struct{}),
	}
}

// Start begins background sampling.
func (t *ThroughputSampler) Start() {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				current := t.metrics.TotalRequests.Load()
				delta := current - t.lastReqs
				t.lastReqs = current
				rps := float64(delta) / t.interval.Seconds()
				t.metrics.SecondlyRPS = append(t.metrics.SecondlyRPS, rps)

				if t.metrics.TotalRequests.Load() > 0 {
					avg := time.Duration(t.metrics.TotalLatencyNs.Load() / t.metrics.TotalRequests.Load())
					t.metrics.SecondlyLatency = append(t.metrics.SecondlyLatency, avg)
				}
			case <-t.stop:
				return
			}
		}
	}()
}

// Stop halts sampling.
func (t *ThroughputSampler) Stop() {
	close(t.stop)
	t.wg.Wait()
}

// ============================================================
// CPUProfiler captures a CPU profile during the test window.
// ============================================================

type CPUProfiler struct {
	metrics *ObservableMetrics
	file    *os.File
	stop    chan struct{}
	wg      sync.WaitGroup
}

// NewCPUProfiler creates a profiler that captures CPU usage.
func NewCPUProfiler(metrics *ObservableMetrics, filename string) *CPUProfiler {
	f, _ := os.Create(filename)
	return &CPUProfiler{
		metrics: metrics,
		file:    f,
		stop:    make(chan struct{}),
	}
}

// Start begins CPU profiling.
func (p *CPUProfiler) Start() {
	if p.file == nil {
		return
	}
	pprof.StartCPUProfile(p.file)
}

// Stop halts CPU profiling and reads the profile data.
func (p *CPUProfiler) Stop() {
	pprof.StopCPUProfile()
	if p.file != nil {
		p.file.Close()
	}
}

// ============================================================
// MemoryProfiler captures heap profile at the end.
// ============================================================

type MemoryProfiler struct {
	filename string
}

// NewMemoryProfiler creates a profiler that captures heap state.
func NewMemoryProfiler(filename string) *MemoryProfiler {
	return &MemoryProfiler{filename: filename}
}

// Dump writes the current heap profile.
func (mp *MemoryProfiler) Dump() {
	f, err := os.Create(mp.filename)
	if err != nil {
		return
	}
	defer f.Close()
	pprof.WriteHeapProfile(f)
}

// ============================================================
// Report generation
// ============================================================

// PrintFullReport outputs a comprehensive observability report.
func PrintFullReport(name string, m *ObservableMetrics) {
	m.Duration = m.EndTime.Sub(m.StartTime)
	total := m.TotalRequests.Load()
	success := m.SuccessCount.Load()
	errors := m.ErrorCount.Load()

	var avgLatency time.Duration
	if total > 0 {
		avgLatency = time.Duration(m.TotalLatencyNs.Load() / total)
	}
	rps := float64(total) / m.Duration.Seconds()
	errRate := float64(errors) / float64(total) * 100

	minLat := time.Duration(m.MinLatencyNs.Load())
	maxLat := time.Duration(m.MaxLatencyNs.Load())
	if m.MinLatencyNs.Load() == math.MaxInt64 {
		minLat = 0
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════╗")
	fmt.Printf("║  %-*s  LOAD TEST OBSERVABILITY REPORT\n", 60, name)
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════╣")

	// ── Request Metrics ──
	fmt.Println("║                                                                        ║")
	fmt.Println("║  ── REQUEST METRICS ────────────────────────────────────────────────── ║")
	fmt.Printf("║  Total Requests:      %-50d ║\n", total)
	fmt.Printf("║  Successful:          %-50d ║\n", success)
	fmt.Printf("║  Errors:              %-50d ║\n", errors)
	fmt.Printf("║  Error Rate:          %-49.2f%% ║\n", errRate)
	fmt.Printf("║  Duration:            %-50v ║\n", m.Duration.Round(time.Millisecond))

	// ── Throughput ──
	fmt.Println("║                                                                        ║")
	fmt.Println("║  ── THROUGHPUT ─────────────────────────────────────────────────────── ║")
	fmt.Printf("║  Requests/sec:        %-50.0f ║\n", rps)

	if len(m.SecondlyRPS) > 0 {
		var sum float64
		for _, v := range m.SecondlyRPS {
			sum += v
		}
		avgRPS := sum / float64(len(m.SecondlyRPS))
		minRPS, maxRPS := m.SecondlyRPS[0], m.SecondlyRPS[0]
		for _, v := range m.SecondlyRPS {
			if v < minRPS {
				minRPS = v
			}
			if v > maxRPS {
				maxRPS = v
			}
		}
		fmt.Printf("║  Avg RPS (1s window): %-50.0f ║\n", avgRPS)
		fmt.Printf("║  Min RPS (1s window): %-50.0f ║\n", minRPS)
		fmt.Printf("║  Max RPS (1s window): %-50.0f ║\n", maxRPS)
	}

	// ── Latency ──
	fmt.Println("║                                                                        ║")
	fmt.Println("║  ── LATENCY ───────────────────────────────────────────────────────── ║")
	fmt.Printf("║  Min:                 %-50v ║\n", minLat.Round(time.Microsecond))
	fmt.Printf("║  Avg:                 %-50v ║\n", avgLatency.Round(time.Microsecond))
	fmt.Printf("║  Max:                 %-50v ║\n", maxLat.Round(time.Microsecond))

	if len(m.SecondlyLatency) > 0 {
		var sum time.Duration
		for _, v := range m.SecondlyLatency {
			sum += v
		}
		avgWindow := sum / time.Duration(len(m.SecondlyLatency))
		fmt.Printf("║  Avg (1s window):     %-50v ║\n", avgWindow.Round(time.Microsecond))
	}

	// ── Latency Histogram ──
	fmt.Println("║                                                                        ║")
	fmt.Println("║  ── LATENCY DISTRIBUTION ──────────────────────────────────────────── ║")
	buckets := []string{"<1us", "1-5us", "5-10us", "10-50us", "50-100us", "100-500us",
		"500us-1ms", "1-5ms", "5-10ms", "10-50ms", "50-100ms", ">100ms"}
	m.bucketMu.Lock()
	for _, b := range buckets {
		count := m.LatencyBuckets[b]
		pct := float64(count) / float64(total) * 100
		barLen := int(pct / 2) // scale: 2% per char
		if barLen > 40 {
			barLen = 40
		}
		if count > 0 || barLen > 0 {
			bar := ""
			for i := 0; i < barLen; i++ {
				bar += "█"
			}
			fmt.Printf("║  %-12s %6d (%5.1f%%) %-20s ║\n", b, count, pct, bar)
		}
	}
	m.bucketMu.Unlock()

	// ── Go Runtime ──
	fmt.Println("║                                                                        ║")
	fmt.Println("║  ── GO RUNTIME ────────────────────────────────────────────────────── ║")

	if len(m.GoroutineSamples) > 0 {
		avgGoroutines := averageInt(m.GoroutineSamples)
		minG, maxG := minMaxInt(m.GoroutineSamples)
		fmt.Printf("║  Goroutines (avg):    %-50d ║\n", avgGoroutines)
		fmt.Printf("║  Goroutines (min):    %-50d ║\n", minG)
		fmt.Printf("║  Goroutines (max):    %-50d ║\n", maxG)
	}

	if len(m.ThreadSamples) > 0 {
		avgThreads := averageInt(m.ThreadSamples)
		fmt.Printf("║  OS Threads (avg):    %-50d ║\n", avgThreads)
	}

	fmt.Printf("║  GOMAXPROCS:          %-50d ║\n", runtime.GOMAXPROCS(0))

	// ── Memory ──
	fmt.Println("║                                                                        ║")
	fmt.Println("║  ── MEMORY ───────────────────────────────────────────────────────── ║")

	if len(m.HeapAllocSamples) > 0 {
		avgHeap := averageUint64(m.HeapAllocSamples)
		minH, maxH := minMaxUint64(m.HeapAllocSamples)
		fmt.Printf("║  Heap Alloc (avg):    %-46s ║\n", formatBytes(avgHeap))
		fmt.Printf("║  Heap Alloc (min):    %-46s ║\n", formatBytes(minH))
		fmt.Printf("║  Heap Alloc (max):    %-46s ║\n", formatBytes(maxH))
	}

	if len(m.HeapObjectsSamples) > 0 {
		avgObjects := averageUint64(m.HeapObjectsSamples)
		fmt.Printf("║  Heap Objects (avg):  %-50d ║\n", avgObjects)
	}

	if len(m.StackSamples) > 0 {
		avgStack := averageUint64(m.StackSamples)
		fmt.Printf("║  Stack Sys (avg):     %-46s ║\n", formatBytes(avgStack))
	}

	if len(m.SysAllocSamples) > 0 {
		avgSys := averageUint64(m.SysAllocSamples)
		fmt.Printf("║  Total Sys (avg):     %-46s ║\n", formatBytes(avgSys))
	}

	// ── Garbage Collection ──
	fmt.Println("║                                                                        ║")
	fmt.Println("║  ── GARBAGE COLLECTION ───────────────────────────────────────────── ║")

	if len(m.GCCycles) > 0 && len(m.GCCycles) >= 2 {
		gcCycles := m.GCCycles[len(m.GCCycles)-1] - m.GCCycles[0]
		fmt.Printf("║  GC Cycles:           %-50d ║\n", gcCycles)
		gcRate := float64(gcCycles) / m.Duration.Seconds()
		fmt.Printf("║  GC Rate:             %-49.1f/s ║\n", gcRate)
	}

	if len(m.GCPauseNs) > 0 {
		// Get the actual pause times (circular buffer)
		var totalPause time.Duration
		var maxPause time.Duration
		count := 0
		for _, p := range m.GCPauseNs {
			if p > 0 {
				pause := time.Duration(p)
				totalPause += pause
				if pause > maxPause {
					maxPause = pause
				}
				count++
			}
		}
		if count > 0 {
			avgPause := totalPause / time.Duration(count)
			fmt.Printf("║  Avg GC Pause:        %-50v ║\n", avgPause.Round(time.Nanosecond))
			fmt.Printf("║  Max GC Pause:        %-50v ║\n", maxPause.Round(time.Nanosecond))
		}
	}

	if len(m.MallocsSamples) > 0 && len(m.FreesSamples) > 0 {
		mallocs := m.MallocsSamples[len(m.MallocsSamples)-1] - m.MallocsSamples[0]
		frees := m.FreesSamples[len(m.FreesSamples)-1] - m.FreesSamples[0]
		fmt.Printf("║  Total Mallocs:       %-50d ║\n", mallocs)
		fmt.Printf("║  Total Frees:         %-50d ║\n", frees)
		fmt.Printf("║  Net Live Objects:    %-50d ║\n", int64(mallocs)-int64(frees))
	}

	fmt.Println("║                                                                        ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════╣")

	// ── Efficiency Score ──
	// Compute a composite score based on throughput, latency, and memory
	rpsScore := math.Min(rps/10000*100, 100)                          // 10k rps = 100
	latencyScore := math.Max(0, 100-float64(avgLatency.Microseconds())/100) // <100us = 100
	memScore := 100.0
	if len(m.HeapAllocSamples) > 0 {
		peakHeap := m.HeapAllocSamples[len(m.HeapAllocSamples)-1]
		memScore = math.Max(0, 100-float64(peakHeap)/1024/1024) // 1MB = 0
	}
	overall := (rpsScore + latencyScore + memScore) / 3

	fmt.Printf("║  EFFICIENCY SCORE:    %-50.1f ║\n", overall)
	fmt.Println("║    (composite of throughput, latency, memory)                          ║")
	fmt.Println("║                                                                        ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════╝")
}

// PrintComparisonReport outputs a side-by-side comparison.
func PrintComparisonReport(traditional, ref *ObservableMetrics) {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║            APPLE-TO-APPLE COMPARISON: Traditional vs REF               ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════╣")

	tTotal := traditional.TotalRequests.Load()
	rTotal := ref.TotalRequests.Load()
	tDur := traditional.EndTime.Sub(traditional.StartTime)
	rDur := ref.EndTime.Sub(ref.StartTime)
	tRPS := float64(tTotal) / tDur.Seconds()
	rRPS := float64(rTotal) / rDur.Seconds()
	tAvg := time.Duration(traditional.TotalLatencyNs.Load() / tTotal)
	rAvg := time.Duration(ref.TotalLatencyNs.Load() / rTotal)
	tMin := time.Duration(traditional.MinLatencyNs.Load())
	rMin := time.Duration(ref.MinLatencyNs.Load())
	tMax := time.Duration(traditional.MaxLatencyNs.Load())
	rMax := time.Duration(ref.MaxLatencyNs.Load())

	rpsDelta := ((rRPS - tRPS) / tRPS) * 100
	latDelta := ((float64(tAvg) - float64(rAvg)) / float64(tAvg)) * 100

	fmt.Println("║                                                                        ║")
	fmt.Printf("║  %-30s │ %-18s │ %-18s ║\n", "METRIC", "Traditional", "REF")
	fmt.Println("║  ──────────────────────────┼────────────────────┼──────────────────── ║")
	fmt.Printf("║  %-30s │ %-18d │ %-18d ║\n", "Total Requests", tTotal, rTotal)
	fmt.Printf("║  %-30s │ %-18.0f │ %-18.0f ║\n", "Throughput (req/s)", tRPS, rRPS)
	fmt.Printf("║  %-30s │ %-18v │ %-18v ║\n", "Avg Latency", tAvg.Round(time.Microsecond), rAvg.Round(time.Microsecond))
	fmt.Printf("║  %-30s │ %-18v │ %-18v ║\n", "Min Latency", tMin.Round(time.Microsecond), rMin.Round(time.Microsecond))
	fmt.Printf("║  %-30s │ %-18v │ %-18v ║\n", "Max Latency", tMax.Round(time.Microsecond), rMax.Round(time.Microsecond))
	fmt.Printf("║  %-30s │ %-18d │ %-18d ║\n", "Errors", traditional.ErrorCount.Load(), ref.ErrorCount.Load())

	// Memory comparison
	if len(traditional.HeapAllocSamples) > 0 && len(ref.HeapAllocSamples) > 0 {
		tHeap := traditional.HeapAllocSamples[len(traditional.HeapAllocSamples)-1]
		rHeap := ref.HeapAllocSamples[len(ref.HeapAllocSamples)-1]
		fmt.Printf("║  %-30s │ %-18s │ %-18s ║\n", "Peak Heap", formatBytes(tHeap), formatBytes(rHeap))
	}
	if len(traditional.GoroutineSamples) > 0 && len(ref.GoroutineSamples) > 0 {
		tGoroutines := traditional.GoroutineSamples[len(traditional.GoroutineSamples)-1]
		rGoroutines := ref.GoroutineSamples[len(ref.GoroutineSamples)-1]
		fmt.Printf("║  %-30s │ %-18d │ %-18d ║\n", "Peak Goroutines", tGoroutines, rGoroutines)
	}
	if len(traditional.GCCycles) > 1 && len(ref.GCCycles) > 1 {
		tGC := traditional.GCCycles[len(traditional.GCCycles)-1] - traditional.GCCycles[0]
		rGC := ref.GCCycles[len(ref.GCCycles)-1] - ref.GCCycles[0]
		fmt.Printf("║  %-30s │ %-18d │ %-18d ║\n", "GC Cycles", tGC, rGC)
	}

	fmt.Println("║                                                                        ║")
	fmt.Println("║  ── DELTA ─────────────────────────────────────────────────────────── ║")
	fmt.Printf("║  Throughput:          %+50.1f%% ║\n", rpsDelta)
	fmt.Printf("║  Latency:             %+50.1f%% ║\n", latDelta)

	if rpsDelta > 0 {
		fmt.Printf("║  ════════════════════════════════════════════════════════════════════  ║\n")
		fmt.Printf("║  REF is %.1fx FASTER and %.1f%% LOWER LATENCY                       ║\n",
			rRPS/tRPS, latDelta)
	}

	fmt.Println("║                                                                        ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════╝")
}

// PrintTimelineChart outputs an ASCII chart of RPS over time.
func PrintTimelineChart(name string, m *ObservableMetrics) {
	if len(m.SecondlyRPS) < 2 {
		return
	}

	fmt.Println()
	fmt.Printf("  %s — Throughput Over Time (req/s)\n", name)
	fmt.Println("  ─────────────────────────────────────────────────")

	maxRPS := 0.0
	for _, v := range m.SecondlyRPS {
		if v > maxRPS {
			maxRPS = v
		}
	}
	if maxRPS == 0 {
		return
	}

	const chartWidth = 60
	for i, rps := range m.SecondlyRPS {
		barLen := int((rps / maxRPS) * chartWidth)
		bar := ""
		for j := 0; j < barLen; j++ {
			bar += "█"
		}
		sec := i + 1
		fmt.Printf("  %3ds │%-*s %8.0f\n", sec, chartWidth, bar, rps)
	}
	fmt.Println()
}

// PrintLatencyTimelineChart outputs an ASCII chart of avg latency over time.
func PrintLatencyTimelineChart(name string, m *ObservableMetrics) {
	if len(m.SecondlyLatency) < 2 {
		return
	}

	fmt.Printf("  %s — Avg Latency Over Time\n", name)
	fmt.Println("  ─────────────────────────────────────────────────")

	maxLat := time.Duration(0)
	for _, v := range m.SecondlyLatency {
		if v > maxLat {
			maxLat = v
		}
	}
	if maxLat == 0 {
		return
	}

	const chartWidth = 60
	for i, lat := range m.SecondlyLatency {
		barLen := int((float64(lat) / float64(maxLat)) * chartWidth)
		bar := ""
		for j := 0; j < barLen; j++ {
			bar += "░"
		}
		sec := i + 1
		fmt.Printf("  %3ds │%-*s %8v\n", sec, chartWidth, bar, lat.Round(time.Microsecond))
	}
	fmt.Println()
}

// ============================================================
// Helpers
// ============================================================

func formatBytes(b uint64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func averageInt(vals []int) int {
	if len(vals) == 0 {
		return 0
	}
	sum := 0
	for _, v := range vals {
		sum += v
	}
	return sum / len(vals)
}

func averageUint64(vals []uint64) uint64 {
	if len(vals) == 0 {
		return 0
	}
	var sum uint64
	for _, v := range vals {
		sum += v
	}
	return sum / uint64(len(vals))
}

func minMaxInt(vals []int) (int, int) {
	if len(vals) == 0 {
		return 0, 0
	}
	min, max := vals[0], vals[0]
	for _, v := range vals {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func minMaxUint64(vals []uint64) (uint64, uint64) {
	if len(vals) == 0 {
		return 0, 0
	}
	min, max := vals[0], vals[0]
	for _, v := range vals {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func threadCount() int {
	// Attempt to read /proc/self/status for thread count
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	var threads int
	fmt.Sscanf(string(data), "Threads:\t%d", &threads)
	return threads
}
