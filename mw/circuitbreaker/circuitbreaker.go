package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

// State is the current circuit-breaker state.
type State uint32

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("state(%d)", uint32(s))
	}
}

const (
	reasonFailureThreshold = "failure_threshold"
	reasonFailureRate      = "failure_rate"
	reasonResetTimeout     = "reset_timeout"
	reasonProbeSucceeded   = "probe_succeeded"
	reasonProbeFailed      = "probe_failed"
	reasonManualOpen       = "manual_open"
	reasonManualClose      = "manual_close"
	reasonManualReset      = "manual_reset"
)

// Config controls breaker behavior. Zero values select production-safe defaults.
type Config struct {
	// FailureThreshold opens a closed circuit after this many consecutive
	// failures. Default: 5.
	FailureThreshold int

	// SuccessThreshold is the number of successful half-open probes required
	// before closing the circuit. Default: 1.
	SuccessThreshold int

	// HalfOpenMaxRequests limits the total concurrent probes admitted while
	// half-open. It must be >= SuccessThreshold. Default: SuccessThreshold.
	HalfOpenMaxRequests int

	// ResetAfter is the minimum time the breaker remains open before a probe is
	// allowed. Default: 30 seconds.
	ResetAfter time.Duration

	// Optional rolling failure-rate policy. It is disabled when
	// FailureRateThreshold is zero. A value of 0.5 means 50 percent.
	FailureRateThreshold float64
	MinimumRequests      int
	RollingWindow        time.Duration
	RollingBuckets       int

	// IsFailure overrides the default failure classifier. It is called after
	// the downstream handler completes. Panics are treated as failed requests
	// and then re-panicked.
	IsFailure func(fh.Ctx, error) bool

	// OnReject handles requests rejected by an open or saturated half-open
	// circuit. Returning nil is valid only when the callback has fully written
	// the response.
	OnReject func(fh.Ctx, Snapshot) error

	// OnStateChange is called synchronously after a transition and never while
	// an internal lock is held. Callback panics are recovered and counted.
	OnStateChange func(Transition)

	// Now is intended for deterministic tests. Default: time.Now.
	Now func() time.Time
}

// Transition describes one completed state transition.
type Transition struct {
	From       State
	To         State
	At         time.Time
	Reason     string
	Generation uint64
}

// Snapshot is a consistent operational view of a breaker.
type Snapshot struct {
	State      State
	Generation uint64
	OpenedAt   time.Time
	RetryAfter time.Duration

	ConsecutiveFailures uint64
	RollingRequests     uint64
	RollingFailures     uint64

	HalfOpenAdmitted  int
	HalfOpenInFlight  int
	HalfOpenSuccesses int
	HalfOpenFailures  int

	Accepted    uint64
	Rejected    uint64
	Succeeded   uint64
	Failed      uint64
	Panics      uint64
	Transitions uint64
	HookPanics  uint64
}

// Breaker is safe for concurrent use.
type Breaker struct {
	cfg Config

	// version is a seqlock protecting lock-free state snapshots. Odd values
	// indicate a transition write in progress.
	version    atomic.Uint64
	state      atomic.Uint32
	generation atomic.Uint64
	openedAtNS atomic.Int64

	// closedFailures packs the low 32 bits of the breaker generation in the
	// high word and the consecutive failure count in the low word. This makes
	// closed-state completion updates generation-safe without a mutex.
	closedFailures atomic.Uint64

	accepted    atomic.Uint64
	rejected    atomic.Uint64
	succeeded   atomic.Uint64
	failed      atomic.Uint64
	panics      atomic.Uint64
	transitions atomic.Uint64
	hookPanics  atomic.Uint64

	transitionMu sync.Mutex
	half         halfOpenBatch // protected by transitionMu
	rolling      *rollingWindow
}

type halfOpenBatch struct {
	admitted int
	inFlight int
	success  int
	failure  int
}

type admission struct {
	state      State
	generation uint64
}

// New creates a breaker and panics for invalid non-zero configuration. This
// preserves the original constructor shape while surfacing configuration bugs
// at startup rather than under traffic.
func New(cfg Config) *Breaker {
	b, err := NewChecked(cfg)
	if err != nil {
		panic(err)
	}
	return b
}

// NewChecked creates a breaker and returns configuration errors.
func NewChecked(cfg Config) (*Breaker, error) {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	b := &Breaker{cfg: normalized}
	b.state.Store(uint32(StateClosed))
	b.generation.Store(1)
	b.closedFailures.Store(packFailures(1, 0))
	if normalized.FailureRateThreshold > 0 {
		b.rolling = newRollingWindow(normalized.RollingWindow, normalized.RollingBuckets, 1)
	}
	return b, nil
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.FailureThreshold == 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.FailureThreshold < 0 {
		return Config{}, errors.New("circuitbreaker: FailureThreshold must be positive")
	}
	if uint64(cfg.FailureThreshold) > uint64(^uint32(0)) {
		return Config{}, errors.New("circuitbreaker: FailureThreshold exceeds supported maximum")
	}
	if cfg.SuccessThreshold == 0 {
		cfg.SuccessThreshold = 1
	}
	if cfg.SuccessThreshold < 0 {
		return Config{}, errors.New("circuitbreaker: SuccessThreshold must be positive")
	}
	if cfg.HalfOpenMaxRequests == 0 {
		cfg.HalfOpenMaxRequests = cfg.SuccessThreshold
	}
	if cfg.HalfOpenMaxRequests < 0 {
		return Config{}, errors.New("circuitbreaker: HalfOpenMaxRequests must be positive")
	}
	if cfg.HalfOpenMaxRequests < cfg.SuccessThreshold {
		return Config{}, errors.New("circuitbreaker: HalfOpenMaxRequests must be >= SuccessThreshold")
	}
	if cfg.ResetAfter == 0 {
		cfg.ResetAfter = 30 * time.Second
	}
	if cfg.ResetAfter < 0 {
		return Config{}, errors.New("circuitbreaker: ResetAfter must be positive")
	}
	if cfg.FailureRateThreshold < 0 || cfg.FailureRateThreshold > 1 {
		return Config{}, errors.New("circuitbreaker: FailureRateThreshold must be between 0 and 1")
	}
	if cfg.FailureRateThreshold > 0 {
		if cfg.MinimumRequests == 0 {
			cfg.MinimumRequests = maxInt(20, cfg.FailureThreshold)
		}
		if cfg.MinimumRequests < 1 {
			return Config{}, errors.New("circuitbreaker: MinimumRequests must be positive")
		}
		if cfg.RollingWindow == 0 {
			cfg.RollingWindow = 30 * time.Second
		}
		if cfg.RollingWindow < time.Millisecond {
			return Config{}, errors.New("circuitbreaker: RollingWindow must be at least 1ms")
		}
		if cfg.RollingBuckets == 0 {
			cfg.RollingBuckets = 10
		}
		if cfg.RollingBuckets < 2 {
			return Config{}, errors.New("circuitbreaker: RollingBuckets must be at least 2")
		}
		if cfg.RollingWindow/time.Duration(cfg.RollingBuckets) <= 0 {
			return Config{}, errors.New("circuitbreaker: RollingWindow is too small for RollingBuckets")
		}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return cfg, nil
}

func packFailures(generation uint64, count uint32) uint64 {
	return uint64(uint32(generation))<<32 | uint64(count)
}

func unpackFailures(value uint64) (uint32, uint32) {
	return uint32(value >> 32), uint32(value)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Middleware creates one breaker and returns its fh middleware handler.
func Middleware(cfg Config) fh.HandlerFunc { return New(cfg).Handler() }

// Handler returns reusable middleware backed by this breaker.
func (b *Breaker) Handler() fh.HandlerFunc {
	return b.handle
}

func (b *Breaker) handle(c fh.Ctx) (err error) {
	adm, ok := b.admit(b.cfg.Now())
	if !ok {
		return b.reject(c)
	}

	completed := false
	defer func() {
		if recovered := recover(); recovered != nil {
			if !completed {
				completed = true
				b.panics.Add(1)
				b.complete(adm, true, b.cfg.Now())
			}
			panic(recovered)
		}
	}()

	err = c.Next()
	failed := b.isFailure(c, err)
	completed = true
	b.complete(adm, failed, b.cfg.Now())
	return err
}

func (b *Breaker) isFailure(c fh.Ctx, err error) bool {
	if b.cfg.IsFailure != nil {
		return b.cfg.IsFailure(c, err)
	}

	statusFailure := c.StatusCode() >= 500
	if err == nil {
		return statusFailure
	}
	if errors.Is(err, context.Canceled) {
		return statusFailure
	}

	var httpErr *fh.HTTPError
	if errors.As(err, &httpErr) {
		return statusFailure || httpErr.Status >= 500
	}
	return true
}

func (b *Breaker) reject(c fh.Ctx) error {
	snapshot := b.Snapshot()
	if b.cfg.OnReject != nil {
		return b.cfg.OnReject(c, snapshot)
	}
	return fh.NewHTTPError(
		fh.StatusServiceUnavailable,
		"CIRCUIT_OPEN",
		"circuit breaker is open",
	)
}

func (b *Breaker) admit(now time.Time) (admission, bool) {
	for {
		state, generation, openedAt := b.atomicState()
		switch state {
		case StateClosed:
			b.accepted.Add(1)
			return admission{state: state, generation: generation}, true

		case StateOpen:
			if now.Sub(openedAt) < b.cfg.ResetAfter {
				b.rejected.Add(1)
				return admission{}, false
			}
			b.tryHalfOpen(generation, now)
			// A competing goroutine may have transitioned first. Re-read.

		case StateHalfOpen:
			b.transitionMu.Lock()
			if State(b.state.Load()) != StateHalfOpen || b.generation.Load() != generation {
				b.transitionMu.Unlock()
				continue
			}
			if b.half.admitted >= b.cfg.HalfOpenMaxRequests {
				b.transitionMu.Unlock()
				b.rejected.Add(1)
				return admission{}, false
			}
			b.half.admitted++
			b.half.inFlight++
			b.transitionMu.Unlock()
			b.accepted.Add(1)
			return admission{state: state, generation: generation}, true

		default:
			b.rejected.Add(1)
			return admission{}, false
		}
	}
}

func (b *Breaker) complete(adm admission, failed bool, now time.Time) {
	if failed {
		b.failed.Add(1)
	} else {
		b.succeeded.Add(1)
	}

	switch adm.state {
	case StateClosed:
		b.completeClosed(adm.generation, failed, now)
	case StateHalfOpen:
		b.completeHalfOpen(adm.generation, failed, now)
	}
}

func (b *Breaker) completeClosed(generation uint64, failed bool, now time.Time) {
	if State(b.state.Load()) != StateClosed || b.generation.Load() != generation {
		return // stale completion from an older breaker generation
	}

	tripRate := false
	if b.rolling != nil {
		requests, failures, applied := b.rolling.add(now, generation, failed)
		tripRate = applied && requests >= uint64(b.cfg.MinimumRequests) &&
			float64(failures)/float64(requests) >= b.cfg.FailureRateThreshold
	}

	if failed {
		failures, applied := b.incrementConsecutive(generation)
		if !applied {
			return
		}
		if failures >= uint32(b.cfg.FailureThreshold) {
			b.tryOpen(StateClosed, generation, now, reasonFailureThreshold)
			return
		}
	} else if !b.resetConsecutive(generation) {
		return
	}

	if tripRate {
		b.tryOpen(StateClosed, generation, now, reasonFailureRate)
	}
}

func (b *Breaker) incrementConsecutive(generation uint64) (uint32, bool) {
	wantGeneration := uint32(generation)
	for {
		current := b.closedFailures.Load()
		storedGeneration, count := unpackFailures(current)
		if storedGeneration != wantGeneration {
			return 0, false
		}
		if count == ^uint32(0) {
			return count, true
		}
		next := packFailures(generation, count+1)
		if b.closedFailures.CompareAndSwap(current, next) {
			return count + 1, true
		}
	}
}

func (b *Breaker) resetConsecutive(generation uint64) bool {
	wantGeneration := uint32(generation)
	for {
		current := b.closedFailures.Load()
		storedGeneration, count := unpackFailures(current)
		if storedGeneration != wantGeneration {
			return false
		}
		if count == 0 {
			return true
		}
		if b.closedFailures.CompareAndSwap(current, packFailures(generation, 0)) {
			return true
		}
	}
}

func (b *Breaker) completeHalfOpen(generation uint64, failed bool, now time.Time) {
	var transition *Transition

	b.transitionMu.Lock()
	if State(b.state.Load()) != StateHalfOpen || b.generation.Load() != generation {
		b.transitionMu.Unlock()
		return // another probe already completed the transition
	}
	if b.half.inFlight > 0 {
		b.half.inFlight--
	}
	if failed {
		b.half.failure++
		transition = b.setStateLocked(StateOpen, now, reasonProbeFailed)
	} else {
		b.half.success++
		// Do not close while another admitted probe is pending. A late failure
		// must win over an early success.
		if b.half.success >= b.cfg.SuccessThreshold && b.half.inFlight == 0 {
			transition = b.setStateLocked(StateClosed, now, reasonProbeSucceeded)
		}
	}
	b.transitionMu.Unlock()
	b.fireTransition(transition)
}

func (b *Breaker) tryHalfOpen(expectedGeneration uint64, now time.Time) {
	var transition *Transition
	b.transitionMu.Lock()
	if State(b.state.Load()) == StateOpen && b.generation.Load() == expectedGeneration {
		openedAt := time.Unix(0, b.openedAtNS.Load())
		if now.Sub(openedAt) >= b.cfg.ResetAfter {
			transition = b.setStateLocked(StateHalfOpen, now, reasonResetTimeout)
		}
	}
	b.transitionMu.Unlock()
	b.fireTransition(transition)
}

func (b *Breaker) tryOpen(from State, expectedGeneration uint64, now time.Time, reason string) {
	var transition *Transition
	b.transitionMu.Lock()
	if State(b.state.Load()) == from && b.generation.Load() == expectedGeneration {
		transition = b.setStateLocked(StateOpen, now, reason)
	}
	b.transitionMu.Unlock()
	b.fireTransition(transition)
}

func (b *Breaker) setStateLocked(to State, now time.Time, reason string) *Transition {
	from := State(b.state.Load())
	if from == to {
		return nil
	}

	b.version.Add(1) // writer active (odd)
	generation := b.generation.Add(1)
	b.state.Store(uint32(to))
	if to == StateOpen {
		b.openedAtNS.Store(now.UnixNano())
	} else if to == StateClosed {
		b.openedAtNS.Store(0)
	}
	b.closedFailures.Store(packFailures(generation, 0))
	b.half = halfOpenBatch{}
	if to == StateClosed && b.rolling != nil {
		b.rolling.reset(generation)
	}
	b.version.Add(1) // writer complete (even)
	b.transitions.Add(1)

	return &Transition{
		From:       from,
		To:         to,
		At:         now,
		Reason:     reason,
		Generation: generation,
	}
}

func (b *Breaker) fireTransition(transition *Transition) {
	if transition == nil || b.cfg.OnStateChange == nil {
		return
	}
	defer func() {
		if recover() != nil {
			b.hookPanics.Add(1)
		}
	}()
	b.cfg.OnStateChange(*transition)
}

// State returns the current state without taking the transition mutex.
func (b *Breaker) State() State {
	state, _, _ := b.atomicState()
	return state
}

func (b *Breaker) atomicState() (State, uint64, time.Time) {
	for {
		before := b.version.Load()
		if before&1 != 0 {
			continue
		}
		state := State(b.state.Load())
		generation := b.generation.Load()
		openedAtNS := b.openedAtNS.Load()
		after := b.version.Load()
		if before == after {
			var openedAt time.Time
			if openedAtNS != 0 {
				openedAt = time.Unix(0, openedAtNS)
			}
			return state, generation, openedAt
		}
	}
}

// Snapshot returns counters and transition state suitable for metrics and
// diagnostics.
func (b *Breaker) Snapshot() Snapshot {
	now := b.cfg.Now()
	state, generation, openedAt := b.atomicState()

	b.transitionMu.Lock()
	half := b.half
	b.transitionMu.Unlock()

	var rollingRequests, rollingFailures uint64
	if b.rolling != nil {
		rollingRequests, rollingFailures = b.rolling.snapshot(now)
	}

	var retryAfter time.Duration
	if state == StateOpen {
		retryAfter = b.cfg.ResetAfter - now.Sub(openedAt)
		if retryAfter < 0 {
			retryAfter = 0
		}
	}

	storedGeneration, consecutiveFailures := unpackFailures(b.closedFailures.Load())
	if storedGeneration != uint32(generation) {
		consecutiveFailures = 0
	}

	return Snapshot{
		State:               state,
		Generation:          generation,
		OpenedAt:            openedAt,
		RetryAfter:          retryAfter,
		ConsecutiveFailures: uint64(consecutiveFailures),
		RollingRequests:     rollingRequests,
		RollingFailures:     rollingFailures,
		HalfOpenAdmitted:    half.admitted,
		HalfOpenInFlight:    half.inFlight,
		HalfOpenSuccesses:   half.success,
		HalfOpenFailures:    half.failure,
		Accepted:            b.accepted.Load(),
		Rejected:            b.rejected.Load(),
		Succeeded:           b.succeeded.Load(),
		Failed:              b.failed.Load(),
		Panics:              b.panics.Load(),
		Transitions:         b.transitions.Load(),
		HookPanics:          b.hookPanics.Load(),
	}
}

// Open manually opens the circuit. It returns true when a transition occurred.
func (b *Breaker) Open() bool {
	now := b.cfg.Now()
	var transition *Transition
	b.transitionMu.Lock()
	if State(b.state.Load()) != StateOpen {
		transition = b.setStateLocked(StateOpen, now, reasonManualOpen)
	}
	b.transitionMu.Unlock()
	b.fireTransition(transition)
	return transition != nil
}

// Close manually closes the circuit without clearing lifetime metrics. It
// returns true when a transition occurred.
func (b *Breaker) Close() bool {
	now := b.cfg.Now()
	var transition *Transition
	b.transitionMu.Lock()
	if State(b.state.Load()) != StateClosed {
		transition = b.setStateLocked(StateClosed, now, reasonManualClose)
	}
	b.transitionMu.Unlock()
	b.fireTransition(transition)
	return transition != nil
}

// Reset closes the circuit and clears policy counters and rolling-window data.
// Lifetime observability counters are intentionally preserved.
func (b *Breaker) Reset() {
	now := b.cfg.Now()
	var transition *Transition
	b.transitionMu.Lock()
	if State(b.state.Load()) != StateClosed {
		transition = b.setStateLocked(StateClosed, now, reasonManualReset)
	} else {
		b.version.Add(1)
		generation := b.generation.Add(1)
		b.closedFailures.Store(packFailures(generation, 0))
		b.half = halfOpenBatch{}
		b.version.Add(1)
		if b.rolling != nil {
			b.rolling.reset(generation)
		}
	}
	b.transitionMu.Unlock()
	b.fireTransition(transition)
}
