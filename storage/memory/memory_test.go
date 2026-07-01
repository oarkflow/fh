package memory

import (
	"context"
	"github.com/oarkflow/fh"
	"testing"
	"time"
)

func TestAdapters(t *testing.T) {
	s := New()
	if err := s.Journal.Append(fh.RequestJournalEntry{RequestID: "r", Event: "e"}); err != nil {
		t.Fatal(err)
	}
	d, _, err := s.Idempotency.Begin("k", "h", "POST", "/")
	if err != nil || d != fh.IdempotencyNew {
		t.Fatalf("%v %v", d, err)
	}
	if err := s.Idempotency.Complete("k", "h", 200, "text/plain", nil, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := s.Queue.Enqueue(context.Background(), &fh.QueueJob{ID: "j", Type: "t", VisibleAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Queue.Claim(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestPurgeExpiredIdempotency(t *testing.T) {
	s := NewIdempotencyStore(time.Millisecond)
	if d, _, err := s.Begin("k", "h", "POST", "/"); err != nil || d != fh.IdempotencyNew {
		t.Fatalf("begin decision=%v err=%v", d, err)
	}
	purged, err := s.PurgeExpired(context.Background(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if purged != 1 {
		t.Fatalf("purged=%d", purged)
	}
}

func TestPurgeTerminalQueueJobs(t *testing.T) {
	s := NewQueueStorage()
	job := &fh.QueueJob{ID: "done-job", Type: "email", VisibleAt: time.Now(), UpdatedAt: time.Now().Add(-time.Hour)}
	if err := s.Enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Claim(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	claimed.UpdatedAt = time.Now().Add(-time.Hour)
	if err := s.Complete(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	purged, err := s.PurgeJobs(context.Background(), "done", time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if purged != 1 {
		t.Fatalf("purged=%d", purged)
	}
	stats, _ := s.Stats(context.Background())
	if stats.Done != 0 {
		t.Fatalf("done=%d", stats.Done)
	}
}
