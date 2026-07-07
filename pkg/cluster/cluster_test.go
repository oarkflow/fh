package cluster

import (
	"context"
	"testing"
	"time"
)

func TestMemoryCoordinatorLeaderLease(t *testing.T) {
	st := NewMemoryStore()
	a, err := New(Config{Store: st, Node: Node{ID: "a"}, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{Store: st, Node: Node{ID: "b"}, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	nodes, err := a.Nodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("nodes=%d", len(nodes))
	}
	if _, ok, err := a.TryLead(context.Background(), "jobs"); err != nil || !ok {
		t.Fatalf("a lead ok=%v err=%v", ok, err)
	}
	if _, ok, err := b.TryLead(context.Background(), "jobs"); err != nil || ok {
		t.Fatalf("b should not lead ok=%v err=%v", ok, err)
	}
	if err := a.ReleaseLeadership(context.Background(), "jobs"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := b.TryLead(context.Background(), "jobs"); err != nil || !ok {
		t.Fatalf("b lead ok=%v err=%v", ok, err)
	}
}
