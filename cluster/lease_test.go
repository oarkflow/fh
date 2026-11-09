package cluster

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFenceAndFailover(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	a, err := s.Acquire(ctx, "tenant/device", "node-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire(ctx, "tenant/device", "node-b", time.Second); !errors.Is(err, ErrHeld) {
		t.Fatal("split-brain acquisition", err)
	}
	s.now = func() time.Time { return a.Expires.Add(time.Millisecond) }
	b, err := s.Acquire(ctx, "tenant/device", "node-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if b.Fence <= a.Fence {
		t.Fatal("fencing token not monotonic")
	}
	if _, err := s.Renew(ctx, a, time.Second); !errors.Is(err, ErrFenced) {
		t.Fatal("stale renewal", err)
	}
	if err := s.Release(ctx, a); !errors.Is(err, ErrFenced) {
		t.Fatal("stale release", err)
	}
	if _, err := s.Renew(ctx, b, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, b); err != nil {
		t.Fatal(err)
	}
	c, err := s.Acquire(ctx, "tenant/device", "node-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if c.Fence <= b.Fence {
		t.Fatal("ABA fencing regression")
	}
}
func TestRendezvousDeterministic(t *testing.T) {
	nodes := Membership([]string{"a", "c", "b", "a"})
	if len(nodes) != 3 {
		t.Fatal(nodes)
	}
	for i := 0; i < 1000; i++ {
		p := PreferredOwner("tenant/row", nodes)
		if p != PreferredOwner("tenant/row", []string{"c", "a", "b"}) {
			t.Fatal("unstable owner")
		}
	}
}
