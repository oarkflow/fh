package slowloris

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type Config struct {
	MaxGoroutines  int
	MaxHeapBytes   uint64
	SampleInterval time.Duration
	Reject         func(fh.Ctx) error
	Skip           func(fh.Ctx) bool
}

func New(cfg Config) fh.HandlerFunc {
	cfg = normalize(cfg)
	var sampleMu sync.Mutex
	var sampledAt time.Time
	var heapAlloc uint64
	var lastPressureGC atomic.Int64
	return func(c fh.Ctx) error {
		if cfg.Skip != nil && cfg.Skip(c) {
			return c.Next()
		}

		if cfg.MaxGoroutines > 0 && runtime.NumGoroutine() > cfg.MaxGoroutines {
			return cfg.Reject(c)
		}

		if cfg.MaxHeapBytes > 0 {
			sampleMu.Lock()
			if time.Since(sampledAt) >= cfg.SampleInterval {
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				heapAlloc = ms.HeapAlloc
				sampledAt = time.Now()
			}
			currentHeap := heapAlloc
			sampleMu.Unlock()
			if currentHeap > cfg.MaxHeapBytes {
				// HeapAlloc may include garbage that the runtime has not
				// reclaimed yet. Give one sampling interval a collection attempt
				// before shedding this request.
				now := time.Now().UnixNano()
				last := lastPressureGC.Load()
				if (last == 0 || now-last >= int64(cfg.SampleInterval)) && lastPressureGC.CompareAndSwap(last, now) {
					var ms runtime.MemStats
					runtime.GC()
					runtime.ReadMemStats(&ms)
					if ms.HeapAlloc <= cfg.MaxHeapBytes {
						return c.Next()
					}
				}
				return cfg.Reject(c)
			}
		}

		return c.Next()
	}
}

func normalize(cfg Config) Config {
	if cfg.MaxGoroutines <= 0 {
		cfg.MaxGoroutines = 10000
	}
	if cfg.MaxHeapBytes <= 0 {
		cfg.MaxHeapBytes = 512 << 20
	}
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = 500 * time.Millisecond
	}
	if cfg.Reject == nil {
		cfg.Reject = func(c fh.Ctx) error {
			return fh.NewHTTPError(fh.StatusServiceUnavailable, "RESOURCE_LIMIT", "Server capacity is temporarily exhausted").Temporary()
		}
	}
	return cfg
}

var activeConns atomic.Int64

func ActiveConnections() int64 {
	return activeConns.Load()
}

func TrackConn() func() {
	activeConns.Add(1)
	var once sync.Once
	return func() { once.Do(func() { activeConns.Add(-1) }) }
}
