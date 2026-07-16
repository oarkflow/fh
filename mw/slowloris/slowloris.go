package slowloris

import (
	"runtime"
	"sync/atomic"

	"github.com/oarkflow/fh"
)

type Config struct {
	MaxGoroutines  int
	MaxHeapBytes   uint64
	Reject         func(fh.Ctx) error
	Skip           func(fh.Ctx) bool
}

func New(cfg Config) fh.HandlerFunc {
	cfg = normalize(cfg)
	return func(c fh.Ctx) error {
		if cfg.Skip != nil && cfg.Skip(c) {
			return c.Next()
		}

		if cfg.MaxGoroutines > 0 && runtime.NumGoroutine() > cfg.MaxGoroutines {
			return cfg.Reject(c)
		}

		if cfg.MaxHeapBytes > 0 {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > cfg.MaxHeapBytes {
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
	if cfg.Reject == nil {
		cfg.Reject = func(c fh.Ctx) error {
			return c.Status(fh.StatusServiceUnavailable).SendString("Service Unavailable")
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
	return func() { activeConns.Add(-1) }
}
