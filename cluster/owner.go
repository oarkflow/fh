package cluster

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ManagedLease is a fail-closed local ownership handle. Each active lease
// runs periodic renewals against a shared authoritative LeaseStore.
// Operations MUST include the Fence token in their database commit, not only
// rely on a preflight Current check (which is vulnerable to TOCTOU).
// On any renewal failure, operations are canceled immediately and the handle
// is permanently fenced. The caller must reacquire a NEW lease.
type ManagedLease struct {
	store  LeaseStore
	mu     sync.RWMutex
	lease  Lease
	err    error
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func AcquireManaged(ctx context.Context, store LeaseStore, identity, node string, ttl time.Duration) (*ManagedLease, error) {
	if store == nil || ttl < 3*time.Second {
		return nil, ErrInvalid
	}
	lease, err := store.Acquire(ctx, identity, node, ttl)
	if err != nil {
		return nil, err
	}
	inner, cancel := context.WithCancel(context.Background())
	h := &ManagedLease{store: store, lease: lease, ctx: inner, cancel: cancel, done: make(chan struct{})}
	go h.renew(ttl)
	return h, nil
}
func (h *ManagedLease) renew(ttl time.Duration) {
	defer close(h.done)
	ticker := time.NewTicker(ttl / 3)
	defer ticker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			// Leave a bounded budget; do not allow a stuck database driver to
			// make a stale owner indefinitely appear healthy locally.
			ctx, cancel := context.WithTimeout(h.ctx, ttl/3)
			h.mu.RLock()
			before := h.lease
			h.mu.RUnlock()
			next, err := h.store.Renew(ctx, before, ttl)
			cancel()
			if err != nil {
				h.fence(err)
				return
			}
			h.mu.Lock()
			if h.err == nil {
				h.lease = next
			}
			h.mu.Unlock()
		}
	}
}
func (h *ManagedLease) fence(err error) {
	h.mu.Lock()
	if h.err == nil {
		h.err = err
	}
	h.mu.Unlock()
	h.cancel()
}
func (h *ManagedLease) Done() <-chan struct{} { return h.ctx.Done() }
func (h *ManagedLease) Err() error            { h.mu.RLock(); defer h.mu.RUnlock(); return h.err }

// Current rechecks the shared lease before authorizing a local operation.
// All downstream durable writes must still enforce the Fence in one DB txn.
func (h *ManagedLease) Current(ctx context.Context) (Lease, error) {
	if h.ctx.Err() != nil {
		return Lease{}, ErrFenced
	}
	h.mu.RLock()
	lease := h.lease
	h.mu.RUnlock()
	actual, err := h.store.Current(ctx, lease.Identity)
	if err != nil || actual.Owner != lease.Owner || actual.Fence != lease.Fence || !actual.Expires.After(time.Now()) {
		h.fence(ErrFenced)
		return Lease{}, ErrFenced
	}
	return actual, nil
}
func (h *ManagedLease) Close() error {
	h.once.Do(func() { h.fence(ErrFenced) })
	<-h.done
	h.mu.RLock()
	lease := h.lease
	h.mu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := h.store.Release(ctx, lease)
	if errors.Is(err, ErrFenced) {
		return nil
	}
	return err
}
