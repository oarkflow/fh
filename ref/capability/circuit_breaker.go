package capability

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/graph"
)

var (
	ErrCircuitOpen = errors.New("ref: circuit breaker is open")
)

// CircuitState represents the state of a circuit breaker.
type CircuitState uint8

const (
	CircuitClosed   CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// CircuitBreakerFunc checks whether the circuit allows execution for a given key.
// Returns (allowed, error).
type CircuitBreakerFunc func(key string) (bool, error)

// CircuitRecordFunc records the outcome of an execution attempt.
type CircuitRecordFunc func(key string, success bool) error

// CircuitStateFunc returns the current state of the circuit for a key.
type CircuitStateFunc func(key string) CircuitState

// NewCircuitBreakerCapability creates a circuit breaker policy capability (DecisionNode).
// The circuit breaker guards unreliable dependencies by tracking failures and
// tripping open after a threshold, preventing cascading failures.
func NewCircuitBreakerCapability(
	name string,
	keySelector func(nc *execution.NodeContext) string,
	onAllow CircuitBreakerFunc,
	onRecord CircuitRecordFunc,
	onState CircuitStateFunc,
	opts ...Option,
) Registration {
	if name == "" {
		name = "capability.circuit_breaker"
	}
	reg := NewRegistration(name, graph.DecisionNode, opts...)

	reg.Run = func(nc *execution.NodeContext) error {
		key := ""
		if keySelector != nil {
			key = keySelector(nc)
		} else {
			key = string(nc.Invocation().Intent)
		}

		if onAllow != nil {
			allowed, err := onAllow(key)
			if err != nil {
				nc.Decisions().RecordDeny(name, fmt.Sprintf("circuit breaker check error: %v", err))
				return err
			}
			if !allowed {
				nc.Decisions().RecordDeny(name, fmt.Sprintf("circuit breaker open for key %q", key))
				return ErrCircuitOpen
			}
		}

		nc.Decisions().RecordAllow(name, nil)
		return nil
	}

	return reg
}

// ---------------------------------------------------------------------------
// In-memory circuit breaker implementation
// ---------------------------------------------------------------------------

// InMemoryCircuitBreakerConfig configures an InMemoryCircuitBreaker.
type InMemoryCircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures before the circuit opens.
	FailureThreshold int
	// SuccessThreshold is the number of consecutive successes in half-open before closing.
	SuccessThreshold int
	// OpenTimeout is how long the circuit stays open before transitioning to half-open.
	OpenTimeout time.Duration
}

// DefaultInMemoryCircuitBreakerConfig returns sensible defaults.
func DefaultInMemoryCircuitBreakerConfig() InMemoryCircuitBreakerConfig {
	return InMemoryCircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenTimeout:      30 * time.Second,
	}
}

// InMemoryCircuitBreaker is a thread-safe, per-key circuit breaker backed by
// the standard three states (closed, open, half-open).
type InMemoryCircuitBreaker struct {
	mu      sync.Mutex
	config  InMemoryCircuitBreakerConfig
	breakers map[string]*circuitState
}

type circuitState struct {
	state           CircuitState
	failures        int
	successes       int
	openedAt        time.Time
}

// NewInMemoryCircuitBreaker creates a new in-memory circuit breaker.
func NewInMemoryCircuitBreaker(cfg InMemoryCircuitBreakerConfig) *InMemoryCircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 2
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 30 * time.Second
	}
	return &InMemoryCircuitBreaker{
		config:   cfg,
		breakers: make(map[string]*circuitState),
	}
}

func (cb *InMemoryCircuitBreaker) getOrCreate(key string) *circuitState {
	bs, ok := cb.breakers[key]
	if !ok {
		bs = &circuitState{state: CircuitClosed}
		cb.breakers[key] = bs
	}
	return bs
}

// Allow reports whether the circuit is closed (or half-open and admitting a probe).
func (cb *InMemoryCircuitBreaker) Allow(key string) (bool, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	bs := cb.getOrCreate(key)
	switch bs.state {
	case CircuitClosed:
		return true, nil
	case CircuitOpen:
		if time.Since(bs.openedAt) >= cb.config.OpenTimeout {
			bs.state = CircuitHalfOpen
			bs.successes = 0
			return true, nil
		}
		return false, nil
	case CircuitHalfOpen:
		// Allow one probe at a time
		return true, nil
	}
	return true, nil
}

// Record feeds the outcome of an execution attempt back into the breaker.
func (cb *InMemoryCircuitBreaker) Record(key string, success bool) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	bs := cb.getOrCreate(key)
	switch bs.state {
	case CircuitClosed:
		if success {
			bs.failures = 0
		} else {
			bs.failures++
			if bs.failures >= cb.config.FailureThreshold {
				bs.state = CircuitOpen
				bs.openedAt = time.Now()
			}
		}
	case CircuitHalfOpen:
		if success {
			bs.successes++
			if bs.successes >= cb.config.SuccessThreshold {
				bs.state = CircuitClosed
				bs.failures = 0
				bs.successes = 0
			}
		} else {
			bs.state = CircuitOpen
			bs.openedAt = time.Now()
			bs.successes = 0
		}
	case CircuitOpen:
		// Should not happen; caller should check Allow first.
	}
	return nil
}

// State returns the current state of the circuit for a key.
func (cb *InMemoryCircuitBreaker) State(key string) CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	bs := cb.getOrCreate(key)
	if bs.state == CircuitOpen && time.Since(bs.openedAt) >= cb.config.OpenTimeout {
		return CircuitHalfOpen
	}
	return bs.state
}

// NewInMemoryCircuitBreakerCapability creates a fully self-contained circuit
// breaker capability using an in-memory breaker. Useful for testing and single
// node deployments.
func NewInMemoryCircuitBreakerCapability(name string, cfg InMemoryCircuitBreakerConfig, opts ...Option) (Registration, *InMemoryCircuitBreaker) {
	cb := NewInMemoryCircuitBreaker(cfg)

	reg := NewCircuitBreakerCapability(
		name,
		nil, // default to intent name
		cb.Allow,
		cb.Record,
		cb.State,
		opts...,
	)

	return reg, cb
}
