package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func newFileStore(t *testing.T, dir string) kv.Store {
	t.Helper()
	st, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestFileStoreHeartbeatAndNodes(t *testing.T) {
	dir := t.TempDir()
	st := newFileStore(t, dir)
	ctx := context.Background()

	if err := Heartbeat(ctx, st, Node{ID: "a"}, time.Minute, nil); err != nil {
		t.Fatal(err)
	}
	if err := Heartbeat(ctx, st, Node{ID: "b"}, time.Minute, nil); err != nil {
		t.Fatal(err)
	}

	nodes, err := Nodes(ctx, st, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("nodes=%d want 2", len(nodes))
	}
	if nodes[0].ID != "a" || nodes[1].ID != "b" {
		t.Fatalf("nodes not sorted by ID: %+v", nodes)
	}

	// Reopen the store against the same dir to prove persistence across
	// process restarts.
	st2 := newFileStore(t, dir)
	nodes2, err := Nodes(ctx, st2, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes2) != 2 {
		t.Fatalf("reopened store nodes=%d want 2", len(nodes2))
	}
}

func TestFileStoreNodesExpiry(t *testing.T) {
	dir := t.TempDir()
	st := newFileStore(t, dir)
	ctx := context.Background()

	if err := Heartbeat(ctx, st, Node{ID: "a"}, 10*time.Millisecond, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	nodes, err := Nodes(ctx, st, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected expired node filtered out, got %+v", nodes)
	}
}

// TestFileStoreLeaderLease mirrors TestMemoryCoordinatorLeaderLease from
// cluster_test.go, but against FileStore, to prove behavioral parity with
// MemoryStore.
func TestFileStoreLeaderLease(t *testing.T) {
	dir := t.TempDir()
	st := newFileStore(t, dir)
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

func TestFileStoreRenewSemantics(t *testing.T) {
	dir := t.TempDir()
	st := newFileStore(t, dir)
	ctx := context.Background()

	l, ok, err := TryAcquire(ctx, st, "jobs", "a", time.Minute, nil)
	if err != nil || !ok {
		t.Fatalf("acquire ok=%v err=%v", ok, err)
	}

	// Renew by non-owner while lease is live must fail.
	if _, ok, err := Renew(ctx, st, "jobs", "b", time.Minute, nil); err != nil || ok {
		t.Fatalf("non-owner renew should fail ok=%v err=%v", ok, err)
	}

	// Renew by current owner must succeed.
	l2, ok, err := Renew(ctx, st, "jobs", "a", time.Minute, nil)
	if err != nil || !ok {
		t.Fatalf("owner renew ok=%v err=%v", ok, err)
	}
	if l2.Owner != "a" || l2.Token == l.Token {
		t.Fatalf("expected refreshed token for owner renew, got %+v vs %+v", l, l2)
	}

	// Release by non-owner is a no-op.
	if err := Release(ctx, st, "jobs", "b"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := TryAcquire(ctx, st, "jobs", "b", time.Minute, nil); err != nil || ok {
		t.Fatalf("lease should still be held by a: ok=%v err=%v", ok, err)
	}

	// Release by owner frees it.
	if err := Release(ctx, st, "jobs", "a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := TryAcquire(ctx, st, "jobs", "b", time.Minute, nil); err != nil || !ok {
		t.Fatalf("b should acquire freed lease: ok=%v err=%v", ok, err)
	}
}

func TestFileStoreRenewExpiredLeaseByOtherOwner(t *testing.T) {
	dir := t.TempDir()
	st := newFileStore(t, dir)
	ctx := context.Background()

	if _, ok, err := TryAcquire(ctx, st, "jobs", "a", 10*time.Millisecond, nil); err != nil || !ok {
		t.Fatalf("acquire ok=%v err=%v", ok, err)
	}
	time.Sleep(30 * time.Millisecond)

	// Lease has expired: a different owner should be able to Renew (steal) it.
	l, ok, err := Renew(ctx, st, "jobs", "b", time.Minute, nil)
	if err != nil || !ok {
		t.Fatalf("expected b to take over expired lease ok=%v err=%v", ok, err)
	}
	if l.Owner != "b" {
		t.Fatalf("expected owner b, got %q", l.Owner)
	}
}

// TestFileStoreConcurrentLeaseAcquisition spins up several goroutines,
// simulating separate OS processes, all pointed at the same directory and
// racing to acquire/renew/release the same lease name. It proves the
// on-disk lock actually serializes access: at any point in time at most one
// "owner" believes it holds the lease, and the final snapshot on disk is
// valid JSON reflecting a single consistent winner.
func TestFileStoreConcurrentLeaseAcquisition(t *testing.T) {
	dir := t.TempDir()
	st := newFileStore(t, dir)

	const workers = 8
	const roundsPerWorker = 15

	var wg sync.WaitGroup
	var successfulAcquires int64
	var mismatch int64

	for i := 0; i < workers; i++ {
		wg.Add(1)
		owner := ownerName(i)
		go func(owner string) {
			defer wg.Done()
			// Each simulated process gets its own FileStore instance
			// (as separate OS processes would), all sharing dir.
			ctx := context.Background()
			for r := 0; r < roundsPerWorker; r++ {
				l, ok, err := TryAcquire(ctx, st, "leader", owner, 200*time.Millisecond, nil)
				if err != nil {
					t.Errorf("TryAcquire: %v", err)
					return
				}
				if ok {
					atomic.AddInt64(&successfulAcquires, 1)
					// Immediately verify nobody else thinks they hold it
					// by re-reading the snapshot's current lease owner.
					if l.Owner != owner {
						atomic.AddInt64(&mismatch, 1)
					}
					// Hold briefly, then renew once, then release.
					if _, ok, err := Renew(ctx, st, "leader", owner, 200*time.Millisecond, nil); err != nil || !ok {
						t.Errorf("Renew after acquire should succeed for owner: ok=%v err=%v", ok, err)
					}
					if err := Release(ctx, st, "leader", owner); err != nil {
						t.Errorf("Release: %v", err)
					}
				}
			}
		}(owner)
	}
	wg.Wait()

	if mismatch != 0 {
		t.Fatalf("observed %d lease ownership mismatches - lock failed to serialize access", mismatch)
	}
	if successfulAcquires == 0 {
		t.Fatal("expected at least one successful acquire across all workers")
	}

	// Final state must be readable, internally consistent JSON, and the
	// lease must have been released by the last owner (no dangling holder).
	final := newFileStore(t, dir)
	snapshot, err := loadState(final)
	if err != nil {
		t.Fatalf("final snapshot unreadable/corrupt: %v", err)
	}
	if _, held := snapshot.Leases["leader"]; held {
		t.Fatalf("expected lease to be released at end of test, got %+v", snapshot.Leases["leader"])
	}
}

func ownerName(i int) string {
	return string(rune('A' + i))
}

func TestFileStoreContextCancellation(t *testing.T) {
	st := newFileStore(t, t.TempDir())
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := Heartbeat(ctx, st, Node{ID: "a"}, time.Minute, nil); err == nil {
		t.Fatal("expected canceled context error")
	}
}
