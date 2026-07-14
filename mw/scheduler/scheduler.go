package scheduler

import (
	"sync"
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

// entry is a queued request.
type entry struct {
	ctx     fh.Ctx
	priority Priority
	enqueued time.Time
	next     *entry
}

// Scheduler is a priority-aware request scheduler.
type Scheduler struct {
	cfg        Config
	inFlight   [5]atomic.Int64
	queues     [5]*entry
	queueLens  [5]atomic.Int64
	mu         [5]sync.Mutex
	totalInFl  atomic.Int64
	rejected   atomic.Int64
	admitted   atomic.Int64
	shed       atomic.Int64
}

// New creates a priority scheduler.
func New(cfg ...Config) *Scheduler {
	c := Config{
		MaxConcurrent:  1024,
		QueueTimeout:   5 * time.Second,
		QueueSize:      1000,
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
		if merge.DefaultPriority >= 0 {
			c.DefaultPriority = merge.DefaultPriority
		}
		if merge.PriorityFunc != nil {
			c.PriorityFunc = merge.PriorityFunc
		}
		if merge.OnShed != nil {
			c.OnShed = merge.OnShed
		}
	}

	s := &Scheduler{cfg: c}
	for i := 0; i < 5; i++ {
		s.queues[i] = &entry{}
	}
	return s
}

// Handler returns middleware that schedules requests by priority.
func (s *Scheduler) Handler() fh.HandlerFunc {
	return func(c fh.Ctx) error {
		priority := s.cfg.DefaultPriority
		if s.cfg.PriorityFunc != nil {
			priority = s.cfg.PriorityFunc(c)
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

func (s *Scheduler) admit(priority Priority) bool {
	// Check global limit.
	if s.cfg.MaxConcurrent > 0 {
		if s.totalInFl.Load() >= int64(s.cfg.MaxConcurrent) {
			// Allow critical priority to always admit.
			if priority > PriorityCritical {
				return false
			}
		}
	}

	// Check per-priority limit.
	pri := int(priority)
	if pri >= 0 && pri < 5 {
		if limit, ok := s.cfg.PerPriority[priority]; ok {
			if s.inFlight[pri].Load() >= int64(limit) {
				if priority > PriorityCritical {
					return false
				}
			}
		}
	}

	s.totalInFl.Add(1)
	if pri >= 0 && pri < 5 {
		s.inFlight[pri].Add(1)
	}
	return true
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
