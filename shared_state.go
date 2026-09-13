package fh

import (
	"context"
	"errors"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// SharedStateProvider is the application-level shared-state contract. It is an
// alias of kv.Provider so applications can configure state without depending
// on a concrete memory, file, Redis, or PostgreSQL implementation.
type SharedStateProvider = kv.Provider

// ErrSharedStateUnavailable is returned when an App has no shared-state
// provider configured.
var ErrSharedStateUnavailable = errors.New("fh: shared state provider is not configured")

// StateStore resolves an isolated feature namespace from the configured
// shared-state provider. The provider owns the returned store; do not close an
// individual namespace store.
func (a *App) StateStore(ctx context.Context, namespace string) (kv.Store, error) {
	if a == nil || a.cfg.SharedState == nil {
		return nil, ErrSharedStateUnavailable
	}
	return a.cfg.SharedState.Store(ctx, namespace)
}

// MustStateStore is the initialization-oriented form of StateStore. It panics
// when shared state is unavailable or the namespace cannot be opened.
func (a *App) MustStateStore(namespace string) kv.Store {
	if a == nil || a.cfg.SharedState == nil {
		panic(ErrSharedStateUnavailable)
	}
	return kv.MustStore(a.cfg.SharedState, namespace)
}
