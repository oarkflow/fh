package runtime

import (
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/observer"
)

// Option configures the REF Engine.
type Option func(*Engine)

// WithEffectStore configures a custom durable EffectStore.
func WithEffectStore(store effect.EffectStore) Option {
	return func(e *Engine) {
		e.effectRunner = effect.NewRunner(store)
	}
}

// WithObserver attaches an observer to all plan executions.
func WithObserver(obs observer.Observer) Option {
	return func(e *Engine) {
		e.observers = append(e.observers, obs)
	}
}

// WithCapability registers a capability at engine initialization.
// Panics if the registration fails (duplicate name or conflicting fact provider),
// since this is a programmer error that must be caught at startup.
func WithCapability(reg capability.Registration) Option {
	return func(e *Engine) {
		if err := e.capabilities.Register(reg); err != nil {
			panic("ref: WithCapability: " + err.Error())
		}
	}
}

// TryCapability registers a capability at engine initialization.
// Returns an error instead of panicking — use when registration failure
// is expected (e.g. optional capabilities).
func TryCapability(reg capability.Registration) Option {
	return func(e *Engine) {
		_ = e.capabilities.Register(reg)
	}
}
