package cluster

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

// FencedHandler must enforce lease.Fence in the same transactional commit as
// the intended business effect. A preflight lease check alone is not sufficient.
type FencedHandler func(ctx context.Context, lease Lease, operation string, payload []byte) ([]byte, error)

// Router owns identities through renewable leases and dispatches to either a
// local fenced handler or the authenticated inter-node forwarding plane.
// It intentionally does NOT migrate live UDP session secrets or implicit state.
type Router struct {
	NodeID    string
	Members   []string
	Store     LeaseStore
	Forwarder *AuthenticatedForwarder
	Handler   FencedHandler
	TTL       time.Duration
	mu        sync.Mutex
	handles   map[string]*ManagedLease
}

func (r *Router) valid() bool {
	return r != nil && r.NodeID != "" && r.Store != nil && r.Handler != nil && r.TTL >= 3*time.Second
}

// Acquire claims identity only if this is the rendezvous-preferred node. Node
// membership must come from a consistent discovery snapshot, not user input.
func (r *Router) Acquire(ctx context.Context, identity string) error {
	if !r.valid() || identity == "" {
		return ErrInvalid
	}
	if PreferredOwner(identity, Membership(r.Members)) != r.NodeID {
		return ErrHeld
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handles == nil {
		r.handles = make(map[string]*ManagedLease)
	}
	if h := r.handles[identity]; h != nil {
		if h.Err() == nil {
			return nil
		}
		_ = h.Close()
		delete(r.handles, identity)
	}
	h, err := AcquireManaged(ctx, r.Store, identity, r.NodeID, r.TTL)
	if err != nil {
		return err
	}
	r.handles[identity] = h
	return nil
}

func (r *Router) local(ctx context.Context, identity, operation string, data []byte) ([]byte, error) {
	r.mu.Lock()
	h := r.handles[identity]
	r.mu.Unlock()
	if h == nil {
		return nil, ErrFenced
	}
	lease, err := h.Current(ctx)
	if err != nil {
		return nil, err
	}
	return r.Handler(ctx, lease, operation, data)
}

func (r *Router) Route(ctx context.Context, identity, operation string, data []byte) ([]byte, error) {
	if !r.valid() || len(identity) == 0 || len(identity) > 256 || len(operation) < 8 || len(operation) > 256 || len(data) > maxForward {
		return nil, ErrInvalid
	}
	lease, err := r.Store.Current(ctx, identity)
	if err != nil {
		return nil, err
	}
	if lease.Owner == r.NodeID {
		return r.local(ctx, identity, operation, data)
	}
	if r.Forwarder == nil {
		return nil, errors.New("cluster: no authenticated forwarding configured")
	}
	return r.Forwarder.Forward(ctx, lease.Owner, lease, operation, data)
}

// ForwardHandler validates mTLS identity and fenced ownership, and then runs
// the locally registered handler. Incoming calls never auto-acquire leases.
func (r *Router) ForwardHandler() http.Handler {
	if r == nil || r.Forwarder == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		})
	}
	forwarder := *r.Forwarder
	forwarder.NodeID = r.NodeID
	forwarder.Store = r.Store
	forwarder.HandlerFunc = func(ctx context.Context, identity, operation string, data []byte) ([]byte, error) {
		return r.local(ctx, identity, operation, data)
	}
	return forwarder.Handler()
}

func (r *Router) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	handles := r.handles
	r.handles = nil
	r.mu.Unlock()
	var failure error
	for _, h := range handles {
		if err := h.Close(); err != nil {
			failure = errors.Join(failure, err)
		}
	}
	return failure
}
