package fh

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIdempotencyAtomicBeginAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idempotency.jsonl")
	store, err := OpenIdempotencyStore(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var fresh atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decision, _, err := store.Begin("key", "hash", "POST", "/orders")
			if err != nil {
				t.Error(err)
				return
			}
			if decision == IdempotencyNew {
				fresh.Add(1)
			} else if decision != IdempotencyProcessing {
				t.Errorf("decision = %v", decision)
			}
		}()
	}
	wg.Wait()
	if fresh.Load() != 1 {
		t.Fatalf("new decisions = %d", fresh.Load())
	}
	if err := store.Complete("key", "hash", 201, "application/json", nil, []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenIdempotencyStore(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	decision, rec, err := reopened.Begin("key", "hash", "POST", "/orders")
	if err != nil || decision != IdempotencyReplay || rec.StatusCode != 201 {
		t.Fatalf("replay after restart = %v, %v, %v", decision, rec, err)
	}
}

func TestIdempotencyStorageOutageFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idempotency.jsonl")
	store, err := OpenIdempotencyStore(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Begin("key", "hash", "POST", "/"); err == nil {
		t.Fatal("write outage admitted new request")
	}
	if _, _, err := store.Begin("key", "hash", "POST", "/"); err == nil {
		t.Fatal("outage did not remain fail-closed")
	}
	if err := os.WriteFile(path, []byte("{invalid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenIdempotencyStore(path, time.Hour); err == nil {
		t.Fatal("corrupt restart was accepted")
	}
}

func TestFileQueueClaimRecoveryAndOutage(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenFileQueueStorage(FileQueueStorageConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	job := &QueueJob{ID: "one", Type: "work", VisibleAt: time.Now().Add(-time.Second)}
	if err := store.Enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(context.Background(), time.Now())
	if err != nil || claimed.ID != "one" {
		t.Fatalf("claim = %v, %v", claimed, err)
	}
	if again, err := store.Claim(context.Background(), time.Now()); !errors.Is(err, ErrQueueEmpty) || again != nil {
		t.Fatalf("duplicate claim = %v, %v", again, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFileQueueStorage(FileQueueStorageConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	claimed, err = reopened.Claim(context.Background(), time.Now())
	if err != nil || claimed.ID != "one" {
		t.Fatalf("claim after restart = %v, %v", claimed, err)
	}
	if err := os.RemoveAll(filepath.Join(dir, "pending")); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Claim(context.Background(), time.Now()); err == nil {
		t.Fatal("store outage was treated as empty queue")
	}
}
