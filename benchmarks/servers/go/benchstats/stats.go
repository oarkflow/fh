package benchstats

import (
	"debug/buildinfo"
	"os"
	"runtime"
	"runtime/metrics"
)

// Snapshot exposes the Go runtime counters needed for allocation and live-heap
// comparisons. It is fetched only outside the measured request window.
type Snapshot struct {
	TotalAlloc     uint64  `json:"total_alloc_bytes"`
	HeapAlloc      uint64  `json:"heap_alloc_bytes"`
	NumGC          uint32  `json:"num_gc"`
	GoCPU          float64 `json:"go_cpu_seconds"`
	GoCPUAvailable bool    `json:"go_cpu_available"`
	Build          Build   `json:"build"`
}

type Build struct {
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

func Read() Snapshot {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	build := Build{
		GoVersion:   runtime.Version(),
		GOOS:        runtime.GOOS,
		GOARCH:      runtime.GOARCH,
		LogicalCPUs: runtime.NumCPU(),
		GOMAXPROCS:  runtime.GOMAXPROCS(0),
	}
	if info, err := buildinfo.ReadFile(os.Args[0]); err == nil {
		build.MainModule = info.Main.Path
		build.MainVersion = info.Main.Version
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				build.SourceRevision = setting.Value
			case "vcs.modified":
				build.SourceModified = setting.Value
			}
		}
	}
	cpuSamples := []metrics.Sample{
		{Name: "/cpu/classes/user:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/scavenge/total:cpu-seconds"},
	}
	metrics.Read(cpuSamples)
	var goCPU float64
	for _, sample := range cpuSamples {
		if sample.Value.Kind() != metrics.KindFloat64 {
			return Snapshot{TotalAlloc: stats.TotalAlloc, HeapAlloc: stats.HeapAlloc, NumGC: stats.NumGC, Build: build}
		}
		goCPU += sample.Value.Float64()
	}
	// Some Go/OS combinations expose CPU-class metrics but leave them at zero.
	// A measured request workload should advance the user counter; without that
	// evidence, report unavailable instead of a plausible but false zero.
	available := cpuSamples[0].Value.Float64() > 0
	return Snapshot{TotalAlloc: stats.TotalAlloc, HeapAlloc: stats.HeapAlloc, NumGC: stats.NumGC, GoCPU: goCPU, GoCPUAvailable: available, Build: build}
}
