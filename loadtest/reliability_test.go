package loadtest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/replay"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

// TestQueueRecoversPendingJobAfterSimulatedRestart claims a job (moving it to
// "processing") and then, instead of completing or failing it, closes that
// storage handle and reopens a fresh one against the same directory — a
// faithful simulation of a process restart with real files on disk, not a
// mocked-out recovery call. It asserts the job is recovered back to pending
// exactly once: not lost, not duplicated (concern #9).
func TestQueueRecoversPendingJobAfterSimulatedRestart(t *testing.T) {
	dir := t.TempDir()
	storage, err := fh.OpenFileQueueStorage(fh.FileQueueStorageConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	q := fh.NewDurableQueue(fh.DurableQueueConfig{MaxAttempts: 5}, storage)

	id, err := q.EnqueueJob(fh.QueueJob{Type: "email"}, map[string]string{"to": "user@example.com"})
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := storage.Claim(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != id {
		t.Fatalf("claimed job %q, want %q", claimed.ID, id)
	}

	// Simulate a crash: the worker never calls Complete/Fail/Retry. Close
	// this handle (the real equivalent of the process dying) and reopen a
	// fresh storage instance against the same on-disk directory, exactly as
	// a restarted process would.
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := fh.OpenFileQueueStorage(fh.FileQueueStorageConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	q2 := fh.NewDurableQueue(fh.DurableQueueConfig{MaxAttempts: 5}, reopened)
	if err := q2.Recover(); err != nil {
		t.Fatal(err)
	}

	pending, err := q2.ListJobs(context.Background(), "pending", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ID != id {
		t.Fatalf("job not recovered to pending after simulated restart: %#v", pending)
	}
	processing, err := q2.ListJobs(context.Background(), "processing", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(processing) != 0 {
		t.Fatalf("job still stuck in processing after recovery: %#v", processing)
	}
	done, _ := q2.ListJobs(context.Background(), "done", 10)
	failed, _ := q2.ListJobs(context.Background(), "failed", 10)
	if total := len(pending) + len(processing) + len(done) + len(failed); total != 1 {
		t.Fatalf("expected exactly 1 job across all queue states after recovery, found %d", total)
	}
}

// TestQueueJobLandsInDLQAfterMaxAttempts enqueues a job whose handler always
// fails and asserts it is retried a bounded number of times (exactly
// MaxAttempts) and then lands in the "failed" state — the DLQ — rather than
// retrying forever (concern #9).
func TestQueueJobLandsInDLQAfterMaxAttempts(t *testing.T) {
	dir := t.TempDir()
	storage, err := fh.OpenFileQueueStorage(fh.FileQueueStorageConfig{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	const maxAttempts = 3
	q := fh.NewDurableQueue(fh.DurableQueueConfig{
		MaxAttempts:  maxAttempts,
		Workers:      1,
		PollInterval: 10 * time.Millisecond,
		Backoff:      10 * time.Millisecond,
	}, storage)

	var attempts atomic.Int64
	q.Register("always-fails", func(ctx context.Context, job *fh.QueueJob) error {
		attempts.Add(1)
		return errors.New("boom")
	})
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	id, err := q.EnqueueJob(fh.QueueJob{Type: "always-fails", MaxAttempts: maxAttempts}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if !waitUntil(3*time.Second, 20*time.Millisecond, func() bool {
		failed, _ := q.ListJobs(context.Background(), "failed", 10)
		return len(failed) == 1 && failed[0].ID == id
	}) {
		t.Fatalf("job never reached the failed/DLQ state after %d repeated failures (attempts so far: %d)", maxAttempts, attempts.Load())
	}
	if got := attempts.Load(); got != int64(maxAttempts) {
		t.Fatalf("handler ran %d times, want exactly MaxAttempts=%d before landing in the DLQ", got, maxAttempts)
	}

	// It must stay there — no further silent retries once in the DLQ.
	time.Sleep(150 * time.Millisecond)
	if got := attempts.Load(); got != int64(maxAttempts) {
		t.Fatalf("handler kept retrying after the job reached the DLQ: %d calls, want %d", got, maxAttempts)
	}
}

// doIdempotentPost issues a real HTTP POST with the given Idempotency-Key.
func doIdempotentPost(t *testing.T, addr, path, key, body string) (status int, respBody string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	return resp.StatusCode, string(buf[:n])
}

// TestIdempotencyConcurrentRaceExactlyOneSideEffect fires two genuinely
// concurrent requests with the same Idempotency-Key and the same body at a
// real running server and asserts the handler (the "side effect") ran
// exactly once. Per reliability.go's actual Begin/Complete design (read
// before writing this test) the loser of a real race sees
// IdempotencyProcessing — not an instant replay, since the winner hasn't
// called Complete yet — so this test accepts either 201 (won the race, or
// raced in late enough to see the winner's completed replay) or 409 (lost
// the race while the winner was still in flight) for each racer, and then
// separately proves the documented steady-state replay guarantee with a
// follow-up request once both have finished (concern #9).
func TestIdempotencyConcurrentRaceExactlyOneSideEffect(t *testing.T) {
	dir := t.TempDir()
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{
		Enabled:            true,
		IdempotencyEnabled: true,
		DataDir:            dir,
	}))
	var calls atomic.Int64
	app.Post("/orders", func(c fh.Ctx) error {
		n := calls.Add(1)
		time.Sleep(150 * time.Millisecond) // widen the race window
		return c.Status(fh.StatusCreated).JSON(fh.Map{"order_id": n})
	})
	addr := startApp(t, app)

	const key = "race-key-1"
	const body = `{"item":"widget"}`

	statuses := make([]int, 2)
	var wg sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start.Wait() // release both racers at (as close to) the same instant
			status, _ := doIdempotentPost(t, addr, "/orders", key, body)
			statuses[i] = status
		}(i)
	}
	start.Done()
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times for a genuine concurrent race on one idempotency key, want exactly 1", calls.Load())
	}
	for i, status := range statuses {
		if status != http.StatusCreated && status != http.StatusConflict {
			t.Fatalf("racer %d got status %d, want 201 (won / late replay) or 409 (raced in while still processing)", i, status)
		}
	}

	// Steady state: once both racers have finished, the documented replay
	// guarantee must hold cleanly.
	time.Sleep(50 * time.Millisecond)
	status, _ := doIdempotentPost(t, addr, "/orders", key, body)
	if status != http.StatusCreated {
		t.Fatalf("post-race replay status = %d, want 201", status)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times total; the post-race request should have been a pure replay with no new side effect", calls.Load())
	}
}

// TestReplayStoreCapacityRejectsPastLimit fills mw/replay's store to a
// configured MaxEntries ceiling and asserts the documented policy — reading
// replay.go, that is a clean ErrStoreFull rejection, not silent unbounded
// growth — actually takes effect, and that the underlying store's size never
// exceeds the configured cap (concern #9).
func TestReplayStoreCapacityRejectsPastLimit(t *testing.T) {
	const capacity = 5
	store := kv.NewMemoryStore(kv.WithShardCount(1))
	t.Cleanup(func() { _ = store.Close() })

	app := fh.New()
	app.Use(replay.New(replay.Config{
		Store:      store,
		MaxEntries: capacity,
		Key:        func(c fh.Ctx) string { return c.Get("X-Nonce") },
	}))
	app.Post("/webhook", func(c fh.Ctx) error { return c.SendString("accepted") })
	addr := startApp(t, app)

	client := &http.Client{Timeout: 2 * time.Second}
	post := func(nonce string) int {
		req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/webhook", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Nonce", nonce)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	for i := 0; i < capacity; i++ {
		nonce := fmt.Sprintf("nonce-%d", i)
		if status := post(nonce); status != http.StatusOK {
			t.Fatalf("request %d (filling capacity %d) = status %d, want 200", i, capacity, status)
		}
	}

	// One more, distinct nonce beyond the configured cap must be rejected
	// per the documented policy, not silently admitted.
	if status := post("nonce-overflow"); status == http.StatusOK {
		t.Fatalf("replay store admitted an entry past its configured MaxEntries=%d cap", capacity)
	}

	n, err := store.Len()
	if err != nil {
		t.Fatal(err)
	}
	if n > capacity {
		t.Fatalf("replay store holds %d entries, past its configured MaxEntries=%d cap — unbounded growth", n, capacity)
	}
}
