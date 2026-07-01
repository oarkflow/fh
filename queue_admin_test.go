package fh_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestDurableQueueAdminListRetryDiscardFileStorage(t *testing.T) {
	store, err := fh.OpenFileQueueStorage(fh.FileQueueStorageConfig{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	q := fh.NewDurableQueue(fh.DurableQueueConfig{MaxAttempts: 1}, store)
	id, err := q.EnqueueJob(fh.QueueJob{Type: "email", MaxAttempts: 1}, map[string]string{"to": "user@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.Claim(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != id {
		t.Fatalf("claimed %q, want %q", job.ID, id)
	}
	if err := store.Fail(context.Background(), job, errors.New("smtp down")); err != nil {
		t.Fatal(err)
	}
	jobs, err := q.ListJobs(context.Background(), "failed", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != id || jobs[0].State != "failed" || jobs[0].PayloadBytes == 0 {
		t.Fatalf("unexpected failed jobs: %#v", jobs)
	}
	if err := q.RetryFailed(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	pending, err := q.ListJobs(context.Background(), "pending", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ID != id {
		t.Fatalf("unexpected pending jobs: %#v", pending)
	}
	job, err = store.Claim(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Fail(context.Background(), job, errors.New("still down")); err != nil {
		t.Fatal(err)
	}
	if err := q.DiscardFailed(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	failed, err := q.ListJobs(context.Background(), "failed", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 0 {
		t.Fatalf("failed job was not discarded: %#v", failed)
	}
}
