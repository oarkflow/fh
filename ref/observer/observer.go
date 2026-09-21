package observer

import (
	"sync/atomic"

	"github.com/oarkflow/fh/ref/graph"
)

// Observer receives lifecycle events from the execution kernel.
type Observer interface {
	NodeStarted(info graph.NodeInfo)
	NodeFinished(info graph.NodeInfo, err error)
	DecisionMade(policy string, verdict uint8, message string)
	EffectCommitted(name string, err error)
	ExecutionFinished(intent string, durationMs float64, err error)
}

// Tier controls how the scheduler delivers events.
type Tier uint8

const (
	// Critical observers run synchronously. A failure or slow down affects execution.
	// Use only for correctness-critical observation (e.g., audit compliance).
	Critical Tier = iota

	// Async observers receive events on a bounded channel.
	// Slow observers block only their own goroutine, not the scheduler.
	Async

	// Lossy observers receive events on a non-blocking ring buffer.
	// Dropped events are counted but never block execution.
	Lossy
)

// TieredObserver wraps an Observer with its delivery tier.
type TieredObserver struct {
	Observer Observer
	Tier     Tier
}

// AsyncDispatcher delivers events to Async-tier observers without blocking.
type AsyncDispatcher struct {
	ch      chan func()
	done    chan struct{}
	dropped atomic.Uint64
}

// NewAsyncDispatcher creates an async dispatcher with the specified channel buffer.
func NewAsyncDispatcher(bufSize int) *AsyncDispatcher {
	if bufSize <= 0 {
		bufSize = 1024
	}
	d := &AsyncDispatcher{
		ch:   make(chan func(), bufSize),
		done: make(chan struct{}),
	}
	go d.run()
	return d
}

func (d *AsyncDispatcher) run() {
	defer close(d.done)
	for fn := range d.ch {
		safeCall(fn)
	}
}

// Send attempts to enqueue fn. If buffer is full, it drops the event and records it.
func (d *AsyncDispatcher) Send(fn func()) {
	if d == nil {
		return
	}
	select {
	case d.ch <- fn:
	default:
		d.dropped.Add(1)
	}
}

// DroppedCount returns the count of dropped events due to buffer saturation.
func (d *AsyncDispatcher) DroppedCount() uint64 {
	if d == nil {
		return 0
	}
	return d.dropped.Load()
}

// Close closes the dispatcher and waits for in-flight queued events to finish.
func (d *AsyncDispatcher) Close() {
	if d == nil {
		return
	}
	close(d.ch)
	<-d.done
}

// safeCall invokes fn, recovering from panics to isolate observers from breaking the engine.
func safeCall(fn func()) {
	defer func() {
		_ = recover()
	}()
	fn()
}

// SafeCall is exported for test verification of panic recovery.
func SafeCall(fn func()) {
	safeCall(fn)
}

// CompositeObserver dispatches lifecycle calls across multiple observers safely.
type CompositeObserver struct {
	critical []Observer
	async    []Observer
	lossy    []Observer
	disp     *AsyncDispatcher
}

// NewCompositeObserver bundles tiered observers.
func NewCompositeObserver(disp *AsyncDispatcher, observers ...TieredObserver) *CompositeObserver {
	co := &CompositeObserver{
		disp: disp,
	}
	for _, to := range observers {
		switch to.Tier {
		case Critical:
			co.critical = append(co.critical, to.Observer)
		case Async:
			co.async = append(co.async, to.Observer)
		case Lossy:
			co.lossy = append(co.lossy, to.Observer)
		}
	}
	return co
}

func (c *CompositeObserver) NodeStarted(info graph.NodeInfo) {
	if c == nil {
		return
	}
	for _, o := range c.critical {
		safeCall(func() { o.NodeStarted(info) })
	}
	if len(c.async) > 0 && c.disp != nil {
		c.disp.Send(func() {
			for _, o := range c.async {
				safeCall(func() { o.NodeStarted(info) })
			}
		})
	}
	if len(c.lossy) > 0 && c.disp != nil {
		c.disp.Send(func() {
			for _, o := range c.lossy {
				safeCall(func() { o.NodeStarted(info) })
			}
		})
	}
}

func (c *CompositeObserver) NodeFinished(info graph.NodeInfo, err error) {
	if c == nil {
		return
	}
	for _, o := range c.critical {
		safeCall(func() { o.NodeFinished(info, err) })
	}
	if len(c.async) > 0 && c.disp != nil {
		c.disp.Send(func() {
			for _, o := range c.async {
				safeCall(func() { o.NodeFinished(info, err) })
			}
		})
	}
	if len(c.lossy) > 0 && c.disp != nil {
		c.disp.Send(func() {
			for _, o := range c.lossy {
				safeCall(func() { o.NodeFinished(info, err) })
			}
		})
	}
}

func (c *CompositeObserver) DecisionMade(policy string, verdict uint8, message string) {
	if c == nil {
		return
	}
	for _, o := range c.critical {
		safeCall(func() { o.DecisionMade(policy, verdict, message) })
	}
	if len(c.async) > 0 && c.disp != nil {
		c.disp.Send(func() {
			for _, o := range c.async {
				safeCall(func() { o.DecisionMade(policy, verdict, message) })
			}
		})
	}
}

func (c *CompositeObserver) EffectCommitted(name string, err error) {
	if c == nil {
		return
	}
	for _, o := range c.critical {
		safeCall(func() { o.EffectCommitted(name, err) })
	}
	if len(c.async) > 0 && c.disp != nil {
		c.disp.Send(func() {
			for _, o := range c.async {
				safeCall(func() { o.EffectCommitted(name, err) })
			}
		})
	}
}

func (c *CompositeObserver) ExecutionFinished(intent string, durationMs float64, err error) {
	if c == nil {
		return
	}
	for _, o := range c.critical {
		safeCall(func() { o.ExecutionFinished(intent, durationMs, err) })
	}
	if len(c.async) > 0 && c.disp != nil {
		c.disp.Send(func() {
			for _, o := range c.async {
				safeCall(func() { o.ExecutionFinished(intent, durationMs, err) })
			}
		})
	}
}

// Noop is a no-op observer for testing and defaults.
type Noop struct{}

func (Noop) NodeStarted(graph.NodeInfo)                               {}
func (Noop) NodeFinished(graph.NodeInfo, error)                       {}
func (Noop) DecisionMade(string, uint8, string)                       {}
func (Noop) EffectCommitted(string, error)                            {}
func (Noop) ExecutionFinished(string, float64, error)                 {}
