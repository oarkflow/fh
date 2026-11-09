package cluster

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestRouterFencingAndFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := NewMemoryStore()
	id := "device-abc"
	owner := PreferredOwner(id, []string{"a", "b"})
	secondary := "a"
	if owner == "a" {
		secondary = "b"
	}
	var firstLease, secondLease Lease
	first := &Router{NodeID: owner, Members: []string{owner, secondary}, Store: store, TTL: 3 * time.Second, Handler: func(_ context.Context, l Lease, op string, b []byte) ([]byte, error) {
		firstLease = l
		return append([]byte("ok:"), b...), nil
	}}
	defer first.Close()
	if err := first.Acquire(ctx, id); err != nil {
		t.Fatal(err)
	}
	other := &Router{NodeID: secondary, Members: []string{owner, secondary}, Store: store, TTL: 3 * time.Second, Handler: first.Handler}
	if err := other.Acquire(ctx, id); !errors.Is(err, ErrHeld) {
		t.Fatalf("expected preferred-owner refusal: %v", err)
	}
	out, err := first.Route(ctx, id, "operation123", []byte("data"))
	if err != nil || !bytes.Equal(out, []byte("ok:data")) {
		t.Fatalf("route result %q %v", out, err)
	}
	// On membership loss the second node cannot write using the first node's
	// token. A new owner must obtain a strictly increasing fence.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := &Router{NodeID: secondary, Members: []string{secondary}, Store: store, TTL: 3 * time.Second, Handler: func(_ context.Context, l Lease, _ string, _ []byte) ([]byte, error) {
		secondLease = l
		return []byte("new"), nil
	}}
	defer second.Close()
	if err := second.Acquire(ctx, id); err != nil {
		t.Fatal(err)
	}
	got, err := second.Route(ctx, id, "operation123", nil)
	if err != nil || string(got) != "new" {
		t.Fatalf("failover %q %v", got, err)
	}
	if secondLease.Fence <= firstLease.Fence {
		t.Fatal("fence did not increase")
	}
	if _, err := first.Route(ctx, id, "operation123", nil); err == nil {
		t.Fatalf("stale node could route: %v", err)
	}
}
