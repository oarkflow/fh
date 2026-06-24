package metrics

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type Metrics struct {
	started  time.Time
	requests atomic.Uint64
	inflight atomic.Int64
	status   sync.Map
	route    sync.Map
}

func New() *Metrics { return &Metrics{started: time.Now()} }

func (m *Metrics) Middleware() fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		m.requests.Add(1)
		m.inflight.Add(1)
		start := time.Now()
		defer func() {
			m.inflight.Add(-1)
			v, _ := m.status.LoadOrStore(strconv.Itoa(c.StatusCode()), &atomic.Uint64{})
			v.(*atomic.Uint64).Add(1)
			v, _ = m.route.LoadOrStore(c.Method()+" "+c.Path(), &atomic.Uint64{})
			v.(*atomic.Uint64).Add(1)
			c.Set("Server-Timing", fmt.Sprintf("app;dur=%d", time.Since(start).Milliseconds()))
		}()
		return c.Next()
	}
}

func (m *Metrics) Handler() fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		if c.Query("format") == "prometheus" {
			var b strings.Builder
			fmt.Fprintf(&b, "fh_requests_total %d\nfh_requests_inflight %d\n", m.requests.Load(), m.inflight.Load())
			return c.Type("text/plain; version=0.0.4").SendString(b.String())
		}
		return c.JSON(fh.Map{"uptime_seconds": int64(time.Since(m.started).Seconds()), "requests_total": m.requests.Load(), "requests_inflight": m.inflight.Load(), "status": snapshot(&m.status), "routes": snapshot(&m.route)})
	}
}

func snapshot(m *sync.Map) map[string]uint64 {
	out := map[string]uint64{}
	m.Range(func(k, v any) bool { out[fmt.Sprint(k)] = v.(*atomic.Uint64).Load(); return true })
	return out
}
