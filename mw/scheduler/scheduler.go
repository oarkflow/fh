package scheduler

import (
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

// Priority represents a request priority level. Lower numbers are higher priority.
type Priority int

const (
	PriorityCritical Priority = 0 // Health checks, admin endpoints
	PriorityHigh     Priority = 1 // Interactive user requests
	PriorityNormal   Priority = 2 // Standard requests
	PriorityLow      Priority = 3 // Background tasks, batch jobs
	PriorityLowest   Priority = 4 // Best-effort, can be shed
)

// Config controls the priority scheduler.
type Config struct {
	// MaxConcurrent limits total concurrent requests across all priorities.
	MaxConcurrent int

	// PerPriority limits concurrent requests per priority level.
	PerPriority map[Priority]int

	// QueueTimeout is the maximum time a request waits in queue before rejection.
	QueueTimeout time.Duration

	// QueueSize limits the maximum queue depth per priority level.
	QueueSize int

	// DefaultPriority is the priority assigned to requests without an explicit priority.
	DefaultPriority Priority

	// PriorityFunc extracts the priority from a request. Defaults to Normal.
	PriorityFunc func(fh.Ctx) Priority

	// OnShed is called when a low-priority request is shed during overload.
	OnShed func(fh.Ctx) error
}

// Scheduler is a priority-aware request scheduler.
type Scheduler struct {
	cfg       Config
	inFlight  [5]atomic.Int64
	totalInFl atomic.Int64
	rejected  atomic.Int64
	admitted  atomic.Int64
	shed      atomic.Int64
}

// New creates a priority scheduler.
func New(cfg ...Config) *Scheduler {
	c := Config{
		MaxConcurrent:   1024,
		QueueTimeout:    5 * time.Second,
		QueueSize:       1000,
		DefaultPriority: PriorityNormal,
	}
	if len(cfg) > 0 {
		merge := cfg[0]
		if merge.MaxConcurrent > 0 {
			c.MaxConcurrent = merge.MaxConcurrent
		}
		if merge.QueueTimeout > 0 {
			c.QueueTimeout = merge.QueueTimeout
		}
		if merge.QueueSize > 0 {
			c.QueueSize = merge.QueueSize
		}
		if merge.PerPriority != nil {
			c.PerPriority = merge.PerPriority
		}
		// NOTE: merge.DefaultPriority > 0, not >= 0. Priority is a plain int
		// with PriorityCritical == 0, so an unset field in a caller's Config
		// literal is bit-for-bit identical to an explicit PriorityCritical.
		// A ">= 0" check is therefore always true and unconditionally
		// clobbers the PriorityNormal baseline above with 0 (Critical) for
		// *any* call to New(Config{...}) that doesn't happen to set
		// DefaultPriority — which, combined with admit() always letting
		// Critical-priority requests bypass MaxConcurrent and PerPriority,
		// silently disabled all admission limits for every request lacking
		// an explicit per-request priority. Treating 0 as "not specified"
		// (matching every other field in this merge, which all use ">0")
		// keeps the documented Normal default intact; it does mean
		// DefaultPriority cannot be explicitly forced to Critical via
		// config, which is the safe direction for this trade-off.
		if merge.DefaultPriority > 0 {
			c.DefaultPriority = merge.DefaultPriority
		}
		if merge.PriorityFunc != nil {
			c.PriorityFunc = merge.PriorityFunc
		}
		if merge.OnShed != nil {
			c.OnShed = merge.OnShed
		}
	}

	return &Scheduler{cfg: c}
}

// Handler returns middleware that schedules requests by priority.
func (s *Scheduler) Handler() fh.HandlerFunc {
	return func(c fh.Ctx) error {
		priority := s.cfg.DefaultPriority
		if s.cfg.PriorityFunc != nil {
			priority = s.cfg.PriorityFunc(c)
		} else if c.Get(fh.HeaderPriority) != "" {
			priority = priorityFromHTTP(c.RequestPriority())
		}

		if !s.admit(priority) {
			s.rejected.Add(1)
			if s.cfg.OnShed != nil {
				return s.cfg.OnShed(c)
			}
			c.Set("Retry-After", "1")
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{
				"error":    "scheduler_overloaded",
				"priority": int(priority),
			})
		}

		s.admitted.Add(1)
		err := c.Next()
		s.release(priority)
		return err
	}
}

func priorityFromHTTP(priority fh.HTTPPriority) Priority {
	switch {
	case priority.Urgency <= 1:
		return PriorityHigh
	case priority.Urgency >= 6:
		return PriorityLow
	default:
		return PriorityNormal
	}
}

// admit atomically reserves capacity for one in-flight request, or reports
// that the request must be shed. Critical-priority requests always bypass
// both the global and per-priority limits.
//
// Each check-and-increment below uses a compare-and-swap retry loop rather
// than a plain Load() followed by Add(1). A Load-then-Add sequence is a
// classic time-of-check-to-time-of-use race: under concurrent callers,
// multiple goroutines can all observe totalInFl below MaxConcurrent before
// any of them increments it, and all be admitted — silently letting the
// in-flight count exceed MaxConcurrent (and PerPriority limits) under
// exactly the burst-of-concurrent-requests load this scheduler exists to
// bound. The CAS loop makes the observe-and-reserve step atomic so the
// configured limits are actually enforced under a race, not just when
// requests happen to arrive serially.
func (s *Scheduler) admit(priority Priority) bool {
	critical := priority <= PriorityCritical

	if s.cfg.MaxConcurrent > 0 && !critical {
		if !tryReserve(&s.totalInFl, int64(s.cfg.MaxConcurrent)) {
			return false
		}
	} else {
		s.totalInFl.Add(1)
	}

	pri := int(priority)
	if pri >= 0 && pri < 5 {
		if limit, ok := s.cfg.PerPriority[priority]; ok && !critical {
			if !tryReserve(&s.inFlight[pri], int64(limit)) {
				// Roll back the global reservation made above so a
				// per-priority rejection never leaks a global slot.
				s.totalInFl.Add(-1)
				return false
			}
		} else {
			s.inFlight[pri].Add(1)
		}
	}
	return true
}

// tryReserve atomically increments counter and reports true, unless
// counter is already at or above limit, in which case it reports false
// without modifying counter.
func tryReserve(counter *atomic.Int64, limit int64) bool {
	for {
		cur := counter.Load()
		if cur >= limit {
			return false
		}
		if counter.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (s *Scheduler) release(priority Priority) {
	s.totalInFl.Add(-1)
	pri := int(priority)
	if pri >= 0 && pri < 5 {
		s.inFlight[pri].Add(-1)
	}
}

// Stats returns scheduler statistics.
type Stats struct {
	TotalInFlight int64
	Rejected      int64
	Admitted      int64
	Shed          int64
	ByPriority    [5]int64
}

// Stats returns current scheduler statistics.
func (s *Scheduler) Stats() Stats {
	return Stats{
		TotalInFlight: s.totalInFl.Load(),
		Rejected:      s.rejected.Load(),
		Admitted:      s.admitted.Load(),
		Shed:          s.shed.Load(),
		ByPriority: [5]int64{
			s.inFlight[0].Load(),
			s.inFlight[1].Load(),
			s.inFlight[2].Load(),
			s.inFlight[3].Load(),
			s.inFlight[4].Load(),
		},
	}
}
