package session

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSessionShardLockWakesAllContendingCallers(t *testing.T) {
	var lock sessionShardLock
	lock.Lock()
	const callers = 64
	var wg sync.WaitGroup
	var acquired atomic.Int32
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := lock.LockContext(context.Background(), time.Second); err != nil {
				return
			}
			acquired.Add(1)
			lock.Unlock()
		}()
	}
	lock.Unlock()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("contending callers did not complete")
	}
	if got := acquired.Load(); got != callers {
		t.Fatalf("acquired %d of %d lock attempts", got, callers)
	}
}

func TestSessionShardLockCancellationWhileWaiting(t *testing.T) {
	var lock sessionShardLock
	lock.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- lock.LockContext(ctx, 0) }()
	deadline := time.Now().Add(time.Second)
	for lock.waiters.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if lock.waiters.Load() == 0 {
		lock.Unlock()
		cancel()
		t.Fatal("caller never entered the lock wait")
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			lock.Unlock()
			t.Fatalf("lock error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		lock.Unlock()
		t.Fatal("canceled lock waiter did not return")
	}
	lock.Unlock()
}
