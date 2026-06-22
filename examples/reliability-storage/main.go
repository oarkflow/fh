package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

type memoryJournal struct {
	mu      sync.Mutex
	entries []fh.RequestJournalEntry
}

func (j *memoryJournal) Append(e fh.RequestJournalEntry) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, e)
	return nil
}
func (j *memoryJournal) Close() error { return nil }

type memoryIdempotency struct {
	mu      sync.Mutex
	records map[string]*fh.IdempotencyRecord
	ttl     time.Duration
}

func newMemoryIdempotency(ttl time.Duration) *memoryIdempotency {
	return &memoryIdempotency{records: map[string]*fh.IdempotencyRecord{}, ttl: ttl}
}
func (s *memoryIdempotency) Begin(key, reqHash, method, path string) (fh.IdempotencyDecision, *fh.IdempotencyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if rec := s.records[key]; rec != nil {
		if !rec.ExpiresAt.IsZero() && rec.ExpiresAt.Before(now) {
			delete(s.records, key)
		} else {
			if rec.RequestHash != reqHash {
				return fh.IdempotencyConflict, clone(rec), nil
			}
			if rec.State == "completed" {
				return fh.IdempotencyReplay, clone(rec), nil
			}
			return fh.IdempotencyProcessing, clone(rec), nil
		}
	}
	rec := &fh.IdempotencyRecord{Key: key, RequestHash: reqHash, Method: method, Path: path, State: "processing", CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(s.ttl)}
	s.records[key] = rec
	return fh.IdempotencyNew, clone(rec), nil
}
func (s *memoryIdempotency) Complete(key, reqHash string, status int, contentType string, headers map[string][]string, response []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.records[key]
	if rec == nil {
		return nil
	}
	if rec.RequestHash != reqHash {
		return errors.New("idempotency hash mismatch")
	}
	rec.State = "completed"
	rec.StatusCode = status
	rec.ContentType = contentType
	rec.Headers = headers
	rec.Response = append([]byte(nil), response...)
	rec.UpdatedAt = time.Now().UTC()
	return nil
}
func (s *memoryIdempotency) Close() error { return nil }
func clone(r *fh.IdempotencyRecord) *fh.IdempotencyRecord {
	if r == nil {
		return nil
	}
	cp := *r
	cp.Response = append([]byte(nil), r.Response...)
	return &cp
}

type memoryQueueStorage struct {
	mu           sync.Mutex
	pending      []*fh.QueueJob
	processing   map[string]*fh.QueueJob
	done, failed int
}

func newMemoryQueueStorage() *memoryQueueStorage {
	return &memoryQueueStorage{processing: map[string]*fh.QueueJob{}}
}
func (s *memoryQueueStorage) Enqueue(ctx context.Context, j *fh.QueueJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *j
	cp.Payload = append([]byte(nil), j.Payload...)
	s.pending = append(s.pending, &cp)
	return nil
}
func (s *memoryQueueStorage) Claim(ctx context.Context, now time.Time) (*fh.QueueJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, j := range s.pending {
		if !j.VisibleAt.After(now) {
			s.pending = append(s.pending[:i], s.pending[i+1:]...)
			s.processing[j.ID] = j
			return j, nil
		}
	}
	return nil, fh.ErrQueueEmpty
}
func (s *memoryQueueStorage) Complete(ctx context.Context, j *fh.QueueJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.processing, j.ID)
	s.done++
	return nil
}
func (s *memoryQueueStorage) Retry(ctx context.Context, j *fh.QueueJob, err error, backoff time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j.Attempts++
	if err != nil {
		j.LastError = err.Error()
	}
	delete(s.processing, j.ID)
	if j.Attempts >= j.MaxAttempts {
		s.failed++
		return nil
	}
	j.VisibleAt = time.Now().UTC().Add(backoff * time.Duration(j.Attempts))
	s.pending = append(s.pending, j)
	return nil
}
func (s *memoryQueueStorage) Fail(ctx context.Context, j *fh.QueueJob, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.processing, j.ID)
	s.failed++
	return nil
}
func (s *memoryQueueStorage) Recover(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, j := range s.processing {
		delete(s.processing, id)
		s.pending = append(s.pending, j)
	}
	return nil
}
func (s *memoryQueueStorage) Stats(ctx context.Context) (fh.QueueStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fh.QueueStats{Pending: len(s.pending), Processing: len(s.processing), Done: s.done, Failed: s.failed}, nil
}
func (s *memoryQueueStorage) Close() error { return nil }

type emailJob struct {
	To, Subject, Message string `json:",omitempty"`
}

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()
	journal := &memoryJournal{}
	idem := newMemoryIdempotency(24 * time.Hour)
	queueStore := newMemoryQueueStorage()
	app := fh.New(fh.Config{Reliability: fh.ReliabilityConfig{
		Enabled: true, JournalEnabled: true, IdempotencyEnabled: true, QueueEnabled: true,
		JournalStore: journal, IdempotencyRepository: idem, QueueStorage: queueStore,
		QueueWorkers: 1, QueuePollInterval: 50 * time.Millisecond,
	}})
	app.Queue().Register("email.send", func(ctx context.Context, job *fh.QueueJob) error {
		var e emailJob
		if err := json.Unmarshal(job.Payload, &e); err != nil {
			return err
		}
		log.Printf("sending email to=%s subject=%s", e.To, e.Subject)
		return nil
	})
	app.Post("/email", func(c *fh.Ctx) error {
		var e emailJob
		if err := c.BodyParser(&e); err != nil {
			return c.Status(400).JSON(fh.Map{"error": "invalid_json"})
		}
		jobID, err := app.Queue().Enqueue("email.send", e)
		if err != nil {
			return err
		}
		return c.Status(202).JSON(fh.Map{"status": "accepted", "job_id": jobID, "request_id": c.Locals("request_id")})
	})
	app.Get("/stats", func(c *fh.Ctx) error {
		st, _ := app.Queue().Stats()
		return c.JSON(fh.Map{"queue": st, "journal_entries": len(journal.entries)})
	})
	log.Fatal(app.Listen(*addr))
}
