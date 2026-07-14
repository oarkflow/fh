package loadshed

import (
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type RejectHandler func(fh.Ctx, Snapshot) error

type Snapshot struct {
	InFlight      int64
	Goroutines    int
	HeapAlloc     uint64
	MaxInFlight   int64
	MaxGoroutines int
	MaxHeapAlloc  uint64
}

type Config struct {
	MaxInFlight   int64
	MaxGoroutines int
	MaxHeapAlloc  uint64
	RetryAfter    time.Duration
	Reject        RejectHandler
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.RetryAfter <= 0 {
		cfg.RetryAfter = time.Second
	}
	if cfg.Reject == nil {
		cfg.Reject = func(c fh.Ctx, s Snapshot) error {
			c.Set("Retry-After", strconv.Itoa(int(cfg.RetryAfter.Seconds())))
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{"error": "load_shed", "retry_after": int(cfg.RetryAfter.Seconds())})
		}
	}
	var in atomic.Int64
	return func(c fh.Ctx) error {
		cur := in.Add(1)
		defer in.Add(-1)
		var ms runtime.MemStats
		if cfg.MaxHeapAlloc > 0 {
			runtime.ReadMemStats(&ms)
		}
		snap := Snapshot{InFlight: cur, Goroutines: runtime.NumGoroutine(), HeapAlloc: ms.HeapAlloc, MaxInFlight: cfg.MaxInFlight, MaxGoroutines: cfg.MaxGoroutines, MaxHeapAlloc: cfg.MaxHeapAlloc}
		if (cfg.MaxInFlight > 0 && cur > cfg.MaxInFlight) || (cfg.MaxGoroutines > 0 && snap.Goroutines > cfg.MaxGoroutines) || (cfg.MaxHeapAlloc > 0 && ms.HeapAlloc > cfg.MaxHeapAlloc) {
			return cfg.Reject(c, snap)
		}
		return c.Next()
	}
}
