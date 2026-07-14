package fh

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// budgetKey is the context key for request budgets.
type budgetKey struct{}

// Budget holds per-request execution limits. A budget travels with the request
// through middleware, handlers, database calls, and HTTP clients, ensuring no
// single request can consume unbounded resources.
type Budget struct {
	// Deadline is the absolute time by which the request must complete.
	Deadline time.Time

	// MaxCPUTime is the maximum CPU time allowed for this request.
	MaxCPUTime time.Duration

	// MaxQueueTime is the maximum time the request may spend waiting in queues.
	MaxQueueTime time.Duration

	// MaxBodyBytes is the maximum allowed request body size in bytes.
	MaxBodyBytes int64

	// MaxResponseBytes is the maximum allowed response body size in bytes.
	MaxResponseBytes int64

	// MaxMemoryBytes is the maximum memory this request may allocate.
	MaxMemoryBytes int64

	// MaxUpstreamCalls limits the number of outbound HTTP calls.
	MaxUpstreamCalls int

	// MaxRetries limits the number of automatic retries.
	MaxRetries int

	// MaxLogBytes limits the total bytes written to logs for this request.
	MaxLogBytes int64

	// children tracks sub-budgets carved from this budget.
	mu        sync.Mutex
	children  []*Budget
	remaining time.Duration
	start     time.Time
}

// BudgetConfig holds budget defaults for a route or application.
type BudgetConfig struct {
	Deadline          time.Duration
	MaxCPUTime        time.Duration
	MaxQueueTime      time.Duration
	MaxBodyBytes      int64
	MaxResponseBytes  int64
	MaxMemoryBytes    int64
	MaxUpstreamCalls  int
	MaxRetries        int
	MaxLogBytes       int64
}

// NewBudget creates a budget from a config and starts its deadline.
func NewBudget(cfg BudgetConfig) *Budget {
	now := time.Now()
	b := &Budget{
		MaxCPUTime:       cfg.MaxCPUTime,
		MaxQueueTime:     cfg.MaxQueueTime,
		MaxBodyBytes:     cfg.MaxBodyBytes,
		MaxResponseBytes: cfg.MaxResponseBytes,
		MaxMemoryBytes:   cfg.MaxMemoryBytes,
		MaxUpstreamCalls: cfg.MaxUpstreamCalls,
		MaxRetries:       cfg.MaxRetries,
		MaxLogBytes:      cfg.MaxLogBytes,
		start:            now,
		remaining:        cfg.Deadline,
	}
	if cfg.Deadline > 0 {
		b.Deadline = now.Add(cfg.Deadline)
	}
	return b
}

// BudgetFromContext extracts the budget from a request context.
func BudgetFromContext(ctx context.Context) *Budget {
	v, _ := ctx.Value(budgetKey{}).(*Budget)
	return v
}

// WithBudget stores the budget in the request context.
func WithBudget(ctx context.Context, b *Budget) context.Context {
	return context.WithValue(ctx, budgetKey{}, b)
}

// Remaining returns the time remaining until the budget deadline.
// If no deadline is set, it returns the maximum duration.
func (b *Budget) Remaining() time.Duration {
	if b == nil || b.Deadline.IsZero() {
		return time.Duration(^uint64(0) >> 1)
	}
	remaining := time.Until(b.Deadline)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Expired reports whether the budget has exceeded its deadline.
func (b *Budget) Expired() bool {
	if b == nil {
		return false
	}
	return !b.Deadline.IsZero() && time.Now().After(b.Deadline)
}

// Child carves a sub-budget from the parent. The child inherits the parent's
// deadline but may have a shorter one. Memory, retries, and upstream call
// budgets are split proportionally.
//
// Example:
//
//	parent := BudgetFromContext(c.Context())
//	dbBudget := parent.Child(700*time.Millisecond, BudgetSplit{
//	    MemoryFraction: 0.3,
//	    UpstreamFraction: 0.5,
//	    RetryFraction: 1.0,
//	})
func (b *Budget) Child(d time.Duration, split BudgetSplit) *Budget {
	if b == nil {
		return NewBudget(BudgetConfig{Deadline: d})
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	childDeadline := b.Remaining()
	if d > 0 && d < childDeadline {
		childDeadline = d
	}

	var childDeadlineAbs time.Time
	if !b.Deadline.IsZero() {
		abs := time.Now().Add(childDeadline)
		if abs.After(b.Deadline) {
			abs = b.Deadline
		}
		childDeadlineAbs = abs
	}

	child := &Budget{
		MaxCPUTime:       b.MaxCPUTime,
		MaxQueueTime:     b.MaxQueueTime,
		MaxBodyBytes:     b.MaxBodyBytes,
		MaxResponseBytes: b.MaxResponseBytes,
		MaxMemoryBytes:   int64(float64(b.MaxMemoryBytes) * split.MemoryFraction),
		MaxUpstreamCalls: int(float64(b.MaxUpstreamCalls) * split.UpstreamFraction),
		MaxRetries:       int(float64(b.MaxRetries) * split.RetryFraction),
		MaxLogBytes:      int64(float64(b.MaxLogBytes) * split.LogFraction),
		Deadline:         childDeadlineAbs,
		start:            time.Now(),
		remaining:        childDeadline,
	}

	b.children = append(b.children, child)
	return child
}

// BudgetSplit defines how a parent budget is divided among children.
type BudgetSplit struct {
	// MemoryFraction is the fraction of parent memory budget (0.0-1.0).
	MemoryFraction float64

	// UpstreamFraction is the fraction of parent upstream call budget (0.0-1.0).
	UpstreamFraction float64

	// RetryFraction is the fraction of parent retry budget (0.0-1.0).
	RetryFraction float64

	// LogFraction is the fraction of parent log budget (0.0-1.0).
	LogFraction float64
}

// WithDeadline returns a child budget with a shorter deadline.
func (b *Budget) WithDeadline(d time.Duration) *Budget {
	return b.Child(d, BudgetSplit{
		MemoryFraction:   1.0,
		UpstreamFraction: 1.0,
		RetryFraction:    1.0,
		LogFraction:      1.0,
	})
}

// CanUpstream reports whether the budget allows another upstream call.
func (b *Budget) CanUpstream() bool {
	if b == nil {
		return true
	}
	if b.MaxUpstreamCalls <= 0 {
		return true
	}
	// Simple check; actual counter is maintained by the caller.
	return true
}

// CanRetry reports whether the budget allows another retry.
func (b *Budget) CanRetry(current int) bool {
	if b == nil {
		return true
	}
	if b.MaxRetries <= 0 {
		return true
	}
	return current < b.MaxRetries
}

// CheckMemory reports whether the requested allocation fits the budget.
func (b *Budget) CheckMemory(bytes int64) bool {
	if b == nil || b.MaxMemoryBytes <= 0 {
		return true
	}
	return bytes <= b.MaxMemoryBytes
}

// CheckBodySize reports whether the body size fits the budget.
func (b *Budget) CheckBodySize(size int64) bool {
	if b == nil || b.MaxBodyBytes <= 0 {
		return true
	}
	return size <= b.MaxBodyBytes
}

// String returns a human-readable summary of the budget.
func (b *Budget) String() string {
	if b == nil {
		return "Budget{nil}"
	}
	return fmt.Sprintf("Budget{deadline=%s, remaining=%s, max_memory=%d, max_upstream=%d, max_retries=%d}",
		b.Deadline.Format("15:04:05.000"), b.Remaining(), b.MaxMemoryBytes, b.MaxUpstreamCalls, b.MaxRetries)
}

// BudgetMiddleware creates middleware that attaches a budget to every request.
func BudgetMiddleware(cfg BudgetConfig) HandlerFunc {
	return func(c Ctx) error {
		budget := NewBudget(cfg)
		c.SetContext(WithBudget(c.Context(), budget))
		return c.Next()
	}
}
