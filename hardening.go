package fh

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// defaultResourceGuardMiddleware returns a middleware that sheds requests once
// in-flight concurrency, goroutine count, or heap allocation crosses a
// configured ceiling. It is defense-in-depth against slowloris-style resource
// exhaustion and sudden request bursts that connection counts and I/O
// timeouts alone do not catch: a client can hold connections open within
// every configured timeout while still driving goroutines or memory up
// through many such connections or expensive handlers. Returns nil when no
// ceiling is configured, so a server that opts out pays no request-path cost.
func defaultResourceGuardMiddleware(cfg Config) HandlerFunc {
	maxInFlight := cfg.MaxInFlightRequests
	maxGoroutines := cfg.MaxGoroutines
	maxHeap := cfg.MaxHeapBytes
	if maxInFlight <= 0 && maxGoroutines <= 0 && maxHeap <= 0 {
		return nil
	}
	interval := cfg.ResourceCheckInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}

	var inFlight atomic.Int64
	var sampleMu sync.Mutex
	var sampledAt time.Time
	var goroutines int
	var heapAlloc uint64

	sample := func() (int, uint64) {
		sampleMu.Lock()
		defer sampleMu.Unlock()
		if time.Since(sampledAt) >= interval {
			if maxGoroutines > 0 {
				goroutines = runtime.NumGoroutine()
			}
			if maxHeap > 0 {
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				heapAlloc = ms.HeapAlloc
			}
			sampledAt = time.Now()
		}
		return goroutines, heapAlloc
	}

	return func(c Ctx) error {
		if maxInFlight > 0 {
			cur := inFlight.Add(1)
			defer inFlight.Add(-1)
			if cur > maxInFlight {
				return c.Status(StatusServiceUnavailable).SendString("Service Unavailable")
			}
		}
		if maxGoroutines > 0 || maxHeap > 0 {
			g, h := sample()
			if maxGoroutines > 0 && g > maxGoroutines {
				return c.Status(StatusServiceUnavailable).SendString("Service Unavailable")
			}
			if maxHeap > 0 && h > maxHeap {
				return c.Status(StatusServiceUnavailable).SendString("Service Unavailable")
			}
		}
		return c.Next()
	}
}

// defaultHardeningMiddleware returns a middleware that applies production
// security defaults. Static policy is resolved once here; the request path is
// a small fixed loop with no mode branches or allocations.
func defaultHardeningMiddleware(cfg Config) HandlerFunc {
	mode := cfg.Mode
	isProd := mode == ModeProduction || mode == ModeStrict || mode == ModeEnterprise || cfg.Compliance.Enabled

	if !isProd {
		return nil
	}

	strict := mode == ModeStrict || mode == ModeEnterprise || cfg.Compliance.Strict || cfg.SecureByDefault
	referrer := "strict-origin-when-cross-origin"
	if strict {
		referrer = "no-referrer"
	}
	headers := [][2]string{
		{"X-Content-Type-Options", "nosniff"},
		{"X-Frame-Options", "DENY"},
		{"Referrer-Policy", referrer},
		{"X-XSS-Protection", "0"},
		{"Permissions-Policy", "geolocation=(), microphone=(), camera=(), payment=(), usb=()"},
	}
	if cfg.SecureByDefault {
		hsts := "max-age=31536000; includeSubDomains"
		if cfg.HSTSPreload {
			hsts += "; preload"
		}
		headers = append(headers,
			[2]string{"Strict-Transport-Security", hsts},
			[2]string{"Cross-Origin-Opener-Policy", "same-origin"},
			[2]string{"Cross-Origin-Resource-Policy", "same-origin"},
		)
	}

	return func(c Ctx) error {
		if dc, ok := c.(*DefaultCtx); ok {
			for _, header := range headers {
				dc.Set(header[0], header[1])
			}
		} else {
			for _, header := range headers {
				c.Set(header[0], header[1])
			}
		}
		return c.Next()
	}
}
