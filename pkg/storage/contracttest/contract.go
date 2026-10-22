// Package contracttest contains opt-in contract tests for storage adapters
// used by multiple fh replicas. Adapters should invoke these from their own
// integration tests against the database they deploy.
package contracttest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// QueueFixture opens independent handles to one isolated shared queue. Cut
// interrupts the backend, and Restore makes it available again. LeaseDuration
// must be the backend's configured claim lease duration.
type QueueFixture struct {
	Open          func() (fh.MultiReplicaQueueStorage, error)
	Cut           func() error
	Restore       func() error
	LeaseDuration time.Duration
}

// RunQueue verifies the multi-replica properties required by the deployment
// security gate. NewFixture must return a fresh empty backend for each subtest.
func RunQueue(t *testing.T, newFixture func(*testing.T) QueueFixture) {
	t.Helper()
	t.Run("atomic claim", func(t *testing.T) {
		f := newFixture(t)
		a, b := openQueuePair(t, f)
		defer a.Close()
		defer b.Close()
		job := &fh.QueueJob{ID: "contract-atomic", Type: "contract", VisibleAt: time.Now().Add(-time.Second)}
		if err := a.Enqueue(context.Background(), job); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		out := make(chan error, 2)
		for _, store := range []fh.MultiReplicaQueueStorage{a, b} {
			wg.Add(1)
			go func(s fh.MultiReplicaQueueStorage) {
				defer wg.Done()
				<-start
				got, err := s.Claim(context.Background(), time.Now())
				if err == nil && got != nil {
					if got.ID != job.ID {
						err = fmt.Errorf("claimed %q", got.ID)
					} else {
						out <- nil
						return
					}
				}
				if errors.Is(err, fh.ErrQueueEmpty) || (err == nil && got == nil) {
					out <- fh.ErrQueueEmpty
					return
				}
				out <- err
			}(store)
		}
		close(start)
		wg.Wait()
		close(out)
		claimed, empty := 0, 0
		for err := range out {
			if err == nil {
				claimed++
			} else if errors.Is(err, fh.ErrQueueEmpty) {
				empty++
			} else {
				t.Errorf("claim error: %v", err)
			}
		}
		if claimed != 1 || empty != 1 {
			t.Fatalf("claims=%d empty=%d", claimed, empty)
		}
	})
	t.Run("lease recovery and fencing", func(t *testing.T) {
		f := newFixture(t)
		if f.LeaseDuration <= 0 {
			t.Fatal("LeaseDuration must be positive")
		}
		a, b := openQueuePair(t, f)
		defer a.Close()
		defer b.Close()
		job := &fh.QueueJob{ID: "contract-lease", Type: "contract", VisibleAt: time.Now().Add(-time.Second)}
		if err := a.Enqueue(context.Background(), job); err != nil {
			t.Fatal(err)
		}
		first, err := a.Claim(context.Background(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got, err := b.Claim(context.Background(), time.Now()); !errors.Is(err, fh.ErrQueueEmpty) && !(err == nil && got == nil) {
			t.Fatalf("live lease claim = %v, %v", got, err)
		}
		time.Sleep(f.LeaseDuration + 50*time.Millisecond)
		if err := b.Recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		second, err := b.Claim(context.Background(), time.Now())
		if err != nil || second == nil || second.ID != job.ID {
			t.Fatalf("expired lease claim = %v, %v", second, err)
		}
		if err := a.Complete(context.Background(), first); err == nil {
			t.Fatal("stale owner completed a re-claimed job")
		}
		if err := b.Complete(context.Background(), second); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("outage and restart", func(t *testing.T) {
		f := newFixture(t)
		if f.Cut == nil || f.Restore == nil {
			t.Fatal("Cut and Restore are required")
		}
		a, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		job := &fh.QueueJob{ID: "contract-restart", Type: "contract", VisibleAt: time.Now().Add(-time.Second)}
		if err := a.Enqueue(context.Background(), job); err != nil {
			t.Fatal(err)
		}
		if err := f.Cut(); err != nil {
			t.Fatal(err)
		}
		if _, err := a.Claim(context.Background(), time.Now()); err == nil {
			t.Fatal("backend outage was reported as an empty queue")
		}
		if err := f.Restore(); err != nil {
			t.Fatal(err)
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		b, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		if err := b.Recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		got, err := b.Claim(context.Background(), time.Now())
		if err != nil || got == nil || got.ID != job.ID {
			t.Fatalf("job after restart = %v, %v", got, err)
		}
	})
}

func openQueuePair(t *testing.T, f QueueFixture) (fh.MultiReplicaQueueStorage, fh.MultiReplicaQueueStorage) {
	t.Helper()
	if f.Open == nil {
		t.Fatal("Open is required")
	}
	a, err := f.Open()
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.Open()
	if err != nil {
		_ = a.Close()
		t.Fatal(err)
	}
	if !a.SupportsMultiReplica() || !b.SupportsMultiReplica() {
		t.Fatal("adapter does not attest to multi-replica safety")
	}
	return a, b
}

// IdempotencyFixture opens independent repositories sharing one isolated
// backend. Cut and Restore simulate a store outage.
type IdempotencyFixture struct {
	Open    func() (fh.IdempotencyRepository, error)
	Cut     func() error
	Restore func() error
}

func RunIdempotency(t *testing.T, newFixture func(*testing.T) IdempotencyFixture) {
	t.Helper()
	t.Run("atomic decision, replay, restart", func(t *testing.T) {
		f := newFixture(t)
		if f.Open == nil {
			t.Fatal("Open is required")
		}
		a, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		b, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		start := make(chan struct{})
		type result struct {
			decision fh.IdempotencyDecision
			repo     fh.IdempotencyRepository
			err      error
		}
		out := make(chan result, 2)
		var wg sync.WaitGroup
		for _, repo := range []fh.IdempotencyRepository{a, b} {
			wg.Add(1)
			go func(r fh.IdempotencyRepository) {
				defer wg.Done()
				<-start
				d, _, e := r.Begin("contract-key", "hash", "POST", "/orders")
				out <- result{d, r, e}
			}(repo)
		}
		close(start)
		wg.Wait()
		close(out)
		fresh, processing := 0, 0
		var owner fh.IdempotencyRepository
		for result := range out {
			if result.err != nil {
				t.Error(result.err)
			}
			if result.decision == fh.IdempotencyNew {
				fresh++
				owner = result.repo
			} else if result.decision == fh.IdempotencyProcessing {
				processing++
			} else {
				t.Errorf("unexpected decision %v", result.decision)
			}
		}
		if fresh != 1 || processing != 1 {
			t.Fatalf("new=%d processing=%d", fresh, processing)
		}
		if err := owner.Complete("contract-key", "hash", 201, "application/json", nil, []byte(`{"ok":true}`)); err != nil {
			t.Fatal(err)
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		c, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		d, rec, err := c.Begin("contract-key", "hash", "POST", "/orders")
		if err != nil || d != fh.IdempotencyReplay || rec == nil || rec.StatusCode != 201 {
			t.Fatalf("replay after restart = %v, %v, %v", d, rec, err)
		}
		d, _, err = c.Begin("contract-key", "other-hash", "POST", "/orders")
		if err != nil || d != fh.IdempotencyConflict {
			t.Fatalf("hash collision = %v, %v", d, err)
		}
	})
	t.Run("outage fails closed", func(t *testing.T) {
		f := newFixture(t)
		if f.Open == nil || f.Cut == nil || f.Restore == nil {
			t.Fatal("Open, Cut, and Restore are required")
		}
		a, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		if err := f.Cut(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := a.Begin("outage-key", "hash", "POST", "/orders"); err == nil {
			t.Fatal("store outage admitted a request")
		}
		if err := f.Restore(); err != nil {
			t.Fatal(err)
		}
	})
}
