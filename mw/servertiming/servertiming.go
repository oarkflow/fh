// Package servertiming provides Server-Timing header support per RFC 8638.
// It measures request processing duration and allows handlers to record custom
// timing metrics visible in browser DevTools.
//
// Usage:
//
//	app.Use(servertiming.New())
//
//	app.Get("/slow", func(c *fh.Ctx) error {
//	    t := servertiming.Get(c)
//	    t.Start("db")
//	    // ... database query ...
//	    t.Stop("db")
//	    t.AddMetric("rows", "1000", "")
//	    return c.JSON(data)
//	})
//
// Response header example:
//
//	Server-Timing: total;dur=145.23, db;dur=89.12;desc="Database Query", rows;desc="1000"
package servertiming

import (
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

// Metric represents a single Server-Timing metric.
type Metric struct {
	Name   string
	Dur    time.Duration
	Desc   string
	Extra  string // extra params like "rows=1000"
	ErrDur bool   // if true, dur is treated as error timing (opaque to client)
}

// Timings holds timing metrics for a single request.
type Timings struct {
	started time.Time
	metrics []Metric
	_spans  map[string]time.Time
	mu      sync.Mutex
}

// Start begins a named timing span.
func (t *Timings) Start(name string) {
	t.mu.Lock()
	if t._spans == nil {
		t._spans = make(map[string]time.Time, 4)
	}
	t._spans[name] = time.Now()
	t.mu.Unlock()
}

// Stop ends a named timing span and records the duration.
func (t *Timings) Stop(name string) {
	t.mu.Lock()
	start, ok := t._spans[name]
	if ok {
		delete(t._spans, name)
		t.metrics = append(t.metrics, Metric{
			Name: name,
			Dur:  time.Since(start),
		})
	}
	t.mu.Unlock()
}

// AddMetric adds a custom metric with name, optional duration, and description.
func (t *Timings) AddMetric(name string, desc string, dur ...time.Duration) {
	m := Metric{Name: name, Desc: desc}
	if len(dur) > 0 {
		m.Dur = dur[0]
	}
	t.mu.Lock()
	t.metrics = append(t.metrics, m)
	t.mu.Unlock()
}

// SetName sets the name of the total timing metric (default: "total").
func (t *Timings) SetName(name string) {
	t.mu.Lock()
	if len(t.metrics) > 0 {
		t.metrics[0].Name = name
	}
	t.mu.Unlock()
}

// AddBytes adds a custom metric with a byte value.
func (t *Timings) AddBytes(name string, bytes int64) {
	t.mu.Lock()
	t.metrics = append(t.metrics, Metric{
		Name:  name,
		Extra: "bytes=" + strconv.FormatInt(bytes, 10),
	})
	t.mu.Unlock()
}

// Config holds configuration for the Server-Timing middleware.
type Config struct {
	// Server is the server identifier included in the header.
	Server string

	// MaxMetrics is the maximum number of metrics to include. Default: 32.
	MaxMetrics int

	// AddTotal controls whether a "total" metric is added automatically. Default: true.
	AddTotal bool

	// Opaque if true hides durations from clients (adds "err" parameter). Default: false.
	Opaque bool

	// Next is an optional skip function.
	Next func(ctx fh.Ctx) bool
}

// DefaultConfig returns the default configuration.
var DefaultConfig = Config{
	MaxMetrics: 32,
	AddTotal:   true,
}

// New creates a Server-Timing middleware.
func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		if config[0].MaxMetrics > 0 {
			cfg.MaxMetrics = config[0].MaxMetrics
		}
		if config[0].Server != "" {
			cfg.Server = config[0].Server
		}
		cfg.AddTotal = config[0].AddTotal
		cfg.Opaque = config[0].Opaque
		cfg.Next = config[0].Next
	}

	return func(ctx fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(ctx) {
			return ctx.Next()
		}

		t := &Timings{
			started: time.Now(),
			metrics: make([]Metric, 0, 8),
		}
		ctx.Locals("_servertiming", t)

		err := ctx.Next()

		t.mu.Lock()
		if cfg.AddTotal && len(t.metrics) == 0 || (len(t.metrics) > 0 && t.metrics[0].Name != "total") {
			totalDur := time.Since(t.started)
			t.metrics = append([]Metric{{Name: "total", Dur: totalDur}}, t.metrics...)
		}
		t.mu.Unlock()

		header := buildHeader(t, cfg)
		if header != "" {
			ctx.Set("Server-Timing", header)
		}

		return err
	}
}

// Get returns the Timings object for the current request.
func Get(ctx fh.Ctx) *Timings {
	v := ctx.Locals("_servertiming")
	if v == nil {
		return nil
	}
	t, _ := v.(*Timings)
	return t
}

func buildHeader(t *Timings, cfg Config) string {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.metrics) == 0 {
		return ""
	}

	count := len(t.metrics)
	if count > cfg.MaxMetrics {
		count = cfg.MaxMetrics
	}

	// Estimate: each metric ~30 bytes average
	b := make([]byte, 0, count*30)

	for i := 0; i < count; i++ {
		m := t.metrics[i]
		if i > 0 {
			b = append(b, ',')
		}

		b = append(b, m.Name...)
		b = append(b, ';')

		if m.Dur > 0 {
			durMs := float64(m.Dur.Microseconds()) / 1000.0
			durMs = math.Round(durMs*100) / 100 // round to 2 decimal places
			b = append(b, "dur="...)
			b = strconv.AppendFloat(b, durMs, 'f', 2, 64)
			if cfg.Opaque {
				b = append(b, ";err"...)
			}
		}

		if m.Desc != "" {
			b = append(b, ";desc=\""...)
			b = appendEscaped(b, m.Desc)
			b = append(b, '"')
		}

		if m.Extra != "" {
			b = append(b, ';')
			b = append(b, m.Extra...)
		}
	}

	return string(b)
}

func appendEscaped(b []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' {
			b = append(b, '\\', c)
		} else {
			b = append(b, c)
		}
	}
	return b
}
