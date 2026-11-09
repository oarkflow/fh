package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type failRenewStore struct {
	LeaseStore
	mu   sync.Mutex
	fail bool
}

func (s *failRenewStore) Renew(ctx context.Context, l Lease, ttl time.Duration) (Lease, error) {
	s.mu.Lock()
	fail := s.fail
	s.mu.Unlock()
	if fail {
		return Lease{}, errors.New("database unavailable")
	}
	return s.LeaseStore.Renew(ctx, l, ttl)
}
func TestManagedOwnerFailClosed(t *testing.T) {
	store := &failRenewStore{LeaseStore: NewMemoryStore()}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	first, err := AcquireManaged(ctx, store, "device-1", "node-a", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := first.Current(ctx)
	if err != nil || lease.Fence == 0 {
		t.Fatal(lease, err)
	}
	if _, err := AcquireManaged(ctx, store, "device-1", "node-b", 3*time.Second); !errors.Is(err, ErrHeld) {
		t.Fatalf("expected ownership refusal: %v", err)
	}
	store.mu.Lock()
	store.fail = true
	store.mu.Unlock()
	select {
	case <-first.Done():
	case <-ctx.Done():
		t.Fatal("renewal did not fail closed")
	}
	if _, err := first.Current(ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("expected fencing, got %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := AcquireManaged(ctx, store, "device-1", "node-b", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	newLease, err := next.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Fence <= lease.Fence {
		t.Fatalf("fence not monotonic: %d <= %d", newLease.Fence, lease.Fence)
	}
}
