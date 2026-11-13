package fh

import (
	"net"
	"runtime"
	"strings"
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
	return defaultResourceGuardMiddlewareWithCounter(cfg, &atomic.Int64{})
}

func defaultResourceGuardMiddlewareWithCounter(cfg Config, inFlight *atomic.Int64) HandlerFunc {
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

	var sampleMu sync.Mutex
	type resourceSample struct {
		at         time.Time
		goroutines int
		heapAlloc  uint64
	}
	var latestSample atomic.Pointer[resourceSample]

	sample := func() (int, uint64) {
		now := time.Now()
		cached := latestSample.Load()
		if cached != nil {
			if now.Sub(cached.at) < interval {
				return cached.goroutines, cached.heapAlloc
			}
			// A single request refreshes an expired snapshot. Other requests use
			// the last sample instead of queueing behind runtime.ReadMemStats at
			// the refresh boundary; the exact in-flight limit remains independent.
			if !sampleMu.TryLock() {
				return cached.goroutines, cached.heapAlloc
			}
		} else {
			sampleMu.Lock()
		}

		// Requests between samples read the immutable snapshot without locking.
		defer sampleMu.Unlock()
		now = time.Now()
		if cached := latestSample.Load(); cached != nil && now.Sub(cached.at) < interval {
			return cached.goroutines, cached.heapAlloc
		}

		current := &resourceSample{}
		if maxGoroutines > 0 {
			current.goroutines = runtime.NumGoroutine()
		}
		if maxHeap > 0 {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			current.heapAlloc = ms.HeapAlloc
		}
		current.at = time.Now()
		latestSample.Store(current)
		return current.goroutines, current.heapAlloc
	}

	return func(c Ctx) error {
		reserved := false
		if dc, ok := c.(*DefaultCtx); ok {
			reserved = dc.inFlightReserved
		}
		if maxInFlight > 0 && !reserved {
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
	// CSP is caller-configured, unlike the fixed policy values above. It is
	// stored through setStaticHeader below, which skips per-request validation,
	// so validate it once while building the middleware.
	if csp := strings.TrimSpace(cfg.ContentSecurityPolicy); csp != "" && len(csp) <= 16<<10 && validHeaderValue(csp) {
		headers = append(headers, [2]string{"Content-Security-Policy", csp})
	}

	// Pre-encode the fixed header set once at build time. These names/values
	// are hardcoded constants above, never caller input, so it is safe to skip
	// Set's per-call []byte conversions, blocklist scan, and token validation
	// on every request and reuse the same immutable byte slices instead.
	staticHeaders := make([]Header, len(headers))
	for i, header := range headers {
		staticHeaders[i] = Header{Key: []byte(header[0]), Value: []byte(header[1])}
	}

	return func(c Ctx) error {
		if dc, ok := c.(*DefaultCtx); ok {
			for _, h := range staticHeaders {
				dc.setStaticHeader(h.Key, h.Value)
			}
		} else {
			for _, header := range headers {
				c.Set(header[0], header[1])
			}
		}
		return c.Next()
	}
}

func normalizeAllowedHosts(hosts []string) []string {
	if len(hosts) == 0 {
		return hosts
	}
	normalized := make([]string, len(hosts))
	for i, host := range hosts {
		normalized[i] = strings.ToLower(strings.TrimSpace(host))
	}
	return normalized
}

// allowedHost expects the allow-list to have been normalized once at app
// construction. Keeping policy normalization out of this request-time helper
// avoids repeating work for every HTTP/1.1 request and HTTP/2 stream.
func allowedHost(host string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	hostWithoutPort := ""
	if strings.Contains(host, ":") {
		if name, _, err := net.SplitHostPort(host); err == nil {
			hostWithoutPort = name
		}
	}
	for _, candidate := range allowed {
		if candidate == "" {
			continue
		}
		if host == candidate || strings.EqualFold(host, candidate) {
			return true
		}
		// Allow a host-only policy entry to match the same host with a port.
		if hostWithoutPort != "" && (hostWithoutPort == candidate || strings.EqualFold(hostWithoutPort, candidate)) {
			return true
		}
	}
	return false
}
