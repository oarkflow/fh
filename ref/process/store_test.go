package process

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// The store conformance suite.
//
// Every store must behave identically, because the engine relies on that: it does
// not know which one it has, and a behavioural difference between them would show
// up as an engine bug that only reproduces in production. So the whole suite runs
// against each implementation, and the assertions are about the contract rather
// than about any one backend's mechanics.
//
// The SQL store is exercised through the same suite by a caller that has a driver
// imported; see TestSQLStoreConformance's skip, which says plainly why it is
// skipped rather than silently passing.

func TestMemoryStoreConformance(t *testing.T) {
	runStoreConformance(t, func(*testing.T) Store { return NewMemoryStore() })
}

func TestSQLStoreConformance(t *testing.T) {
	// This package imports no SQL driver on purpose — the SQLite conformance
	// suite lives in store_sql_test.go with modernc.org/sqlite imported.
	// MySQL conformance should be added in a separate build-tagged file when
	// a MySQL driver is available.
	t.Skip("SQL store conformance is in store_sql_test.go (SQLite) and platform's own tests (MySQL/Postgres)")
}

func runStoreConformance(t *testing.T, open func(*testing.T) Store) {
	t.Helper()
	for _, testCase := range []struct {
		name string
		run  func(*testing.T, Store)
	}{
		{"runs round-trip", storeRunRoundTrip},
		{"optimistic revisions reject a stale write", storeRevisionConflict},
		{"idempotency finds an existing run", storeIdempotency},
		{"steps keep their completion order", storeStepOrdering},
		{"due timers are claimed exactly once", storeTimerClaim},
		{"subscriptions match by correlation", storeSubscriptionMatching},
		{"leases are exclusive until they expire", storeLeaseExclusion},
		{"tasks reject a concurrent claim", storeTaskRevision},
		{"joins accumulate across calls", storeJoinAccumulation},
		{"purge removes a run and everything belonging to it", storePurge},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := open(t)
			t.Cleanup(func() { _ = store.Close() })
			if err := store.Migrate(context.Background()); err != nil {
				t.Fatalf("migrate: %v", err)
			}
			testCase.run(t, store)
		})
	}
}

func newTestRun(id string) *Run {
	now := time.Now().UTC()
	return &Run{
		ID:        id,
		Process:   "order.fulfil",
		Version:   1,
		Status:    StatusPending,
		Input:     json.RawMessage(`{"order_id":"A-1"}`),
		Visits:    map[string]int{},
		CreatedAt: now,
		UpdatedAt: now,
		Frames:    []Frame{{Step: "reserve", Attempt: 1}},
	}
}

func storeRunRoundTrip(t *testing.T, store Store) {
	ctx := context.Background()
	run := newTestRun("run-1")
	if err := store.CreateRun(ctx, run); err != nil {
		t.Fatalf("create: %v", err)
	}

	loaded, err := store.GetRun(ctx, "run-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if loaded.Process != "order.fulfil" || loaded.Status != StatusPending {
		t.Fatalf("round-trip lost fields: %+v", loaded)
	}
	// The cursor is the one field whose loss would strand a run silently, so it is
	// asserted specifically rather than trusted to a struct comparison.
	if len(loaded.Frames) != 1 || loaded.Frames[0].Step != "reserve" {
		t.Fatalf("cursor did not round-trip: %+v", loaded.Frames)
	}

	if _, err := store.GetRun(ctx, "missing"); err == nil {
		t.Fatal("expected ErrRunNotFound for an unknown run")
	}
}

func storeRevisionConflict(t *testing.T, store Store) {
	ctx := context.Background()
	run := newTestRun("run-2")
	if err := store.CreateRun(ctx, run); err != nil {
		t.Fatalf("create: %v", err)
	}

	first, err := store.GetRun(ctx, "run-2")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	second, err := store.GetRun(ctx, "run-2")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	first.Status = StatusRunning
	if err := store.SaveRun(ctx, first); err != nil {
		t.Fatalf("first save: %v", err)
	}
	// The second writer read the same revision the first one did. Letting it through
	// would silently discard the first write — which is exactly the interleaving two
	// replicas produce when a lease lapses.
	second.Status = StatusFailed
	if err := store.SaveRun(ctx, second); err != ErrRevisionConflict {
		t.Fatalf("stale write = %v, want ErrRevisionConflict", err)
	}

	latest, err := store.GetRun(ctx, "run-2")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if latest.Status != StatusRunning {
		t.Fatalf("the losing write was applied: status = %s", latest.Status)
	}
	if latest.Revision <= run.Revision {
		t.Fatalf("revision did not advance: %d", latest.Revision)
	}
}

func storeIdempotency(t *testing.T, store Store) {
	ctx := context.Background()
	run := newTestRun("run-3")
	run.IdempotencyKey = "order-A-1"
	if err := store.CreateRun(ctx, run); err != nil {
		t.Fatalf("create: %v", err)
	}

	found, err := store.FindRunByIdempotency(ctx, "order.fulfil", "order-A-1")
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if found.ID != "run-3" {
		t.Fatalf("found %s, want run-3", found.ID)
	}
	// A key belonging to another process must not match: two processes may
	// legitimately use the same business identifier.
	if _, err := store.FindRunByIdempotency(ctx, "other.process", "order-A-1"); err == nil {
		t.Fatal("a key matched across processes")
	}
}

func storeStepOrdering(t *testing.T, store Store) {
	ctx := context.Background()
	run := newTestRun("run-4")
	if err := store.CreateRun(ctx, run); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Completion order, not declaration order: compensation walks this backwards,
	// and getting it wrong refunds before un-reserving.
	for _, step := range []string{"reserve", "charge", "ship"} {
		sequence, err := store.NextStepSequence(ctx, run.ID)
		if err != nil {
			t.Fatalf("sequence: %v", err)
		}
		if err := store.SaveStep(ctx, &StepState{
			RunID: run.ID, Step: step, Key: step, Status: StepCompleted,
			Sequence: sequence, StartedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("save step: %v", err)
		}
	}

	states, err := store.ListSteps(ctx, run.ID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(states) != 3 {
		t.Fatalf("got %d steps, want 3", len(states))
	}
	for i, want := range []string{"reserve", "charge", "ship"} {
		if states[i].Step != want {
			t.Fatalf("step[%d] = %s, want %s", i, states[i].Step, want)
		}
	}
	if states[0].Sequence >= states[2].Sequence {
		t.Fatal("sequences are not monotonic")
	}

	// Re-saving a step must update it rather than duplicate it: a retry writes the
	// same key again.
	states[0].Attempt = 2
	if err := store.SaveStep(ctx, states[0]); err != nil {
		t.Fatalf("resave: %v", err)
	}
	after, _ := store.ListSteps(ctx, run.ID)
	if len(after) != 3 {
		t.Fatalf("re-saving a step created a duplicate: %d rows", len(after))
	}
}

func storeTimerClaim(t *testing.T, store Store) {
	ctx := context.Background()
	past := time.Now().UTC().Add(-time.Minute)
	future := time.Now().UTC().Add(time.Hour)

	for i, fire := range []time.Time{past, past, future} {
		if err := store.AddTimer(ctx, &Timer{
			ID: string(rune('a' + i)), RunID: "run-5", Fire: fire, Kind: "delayed", CreatedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("add timer: %v", err)
		}
	}

	due, err := store.DueTimers(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	if len(due) != 2 {
		t.Fatalf("claimed %d timers, want the 2 that were due", len(due))
	}
	// A second claim must find nothing: a timer that fired twice is a step that ran
	// twice.
	again, err := store.DueTimers(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("second due: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("a claimed timer was claimed again: %d", len(again))
	}

	remaining, err := store.ListTimers(ctx, "run-5")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(remaining) != 1 {
		t.Fatalf("the future timer should remain: got %d", len(remaining))
	}
}

func storeSubscriptionMatching(t *testing.T, store Store) {
	ctx := context.Background()
	for _, subscription := range []*Subscription{
		{ID: "s1", RunID: "r1", Event: "payment.settled", Correlation: "order-1", Step: "charge", CreatedAt: time.Now().UTC()},
		{ID: "s2", RunID: "r2", Event: "payment.settled", Correlation: "order-2", Step: "charge", CreatedAt: time.Now().UTC()},
		{ID: "s3", RunID: "r3", Event: "payment.settled", Step: "charge", CreatedAt: time.Now().UTC()},
	} {
		if err := store.Subscribe(ctx, subscription); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	}

	matched, err := store.MatchSubscriptions(ctx, "payment.settled", "order-1")
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	// The correlated subscription for order-1 and the uncorrelated broadcast one;
	// never order-2's, whose run has nothing to do with this payment.
	if len(matched) != 2 {
		t.Fatalf("matched %d subscriptions, want 2", len(matched))
	}
	for _, subscription := range matched {
		if subscription.RunID == "r2" {
			t.Fatal("an event reached a run correlated to a different order")
		}
	}

	if err := store.DeleteRunSubscriptions(ctx, "r1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	after, _ := store.MatchSubscriptions(ctx, "payment.settled", "order-1")
	if len(after) != 1 {
		t.Fatalf("after deleting r1's subscription, got %d", len(after))
	}
}

func storeLeaseExclusion(t *testing.T, store Store) {
	ctx := context.Background()

	taken, err := store.AcquireLease(ctx, "run-6", "replica-a", time.Minute)
	if err != nil || !taken {
		t.Fatalf("first acquire = %v, %v", taken, err)
	}
	// A second replica must not get it. This is the property that makes Advance
	// safe to call from every replica at once.
	taken, err = store.AcquireLease(ctx, "run-6", "replica-b", time.Minute)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if taken {
		t.Fatal("two replicas hold the same lease")
	}
	// Not even the holder re-acquires: one replica advancing a run from two
	// goroutines is exactly the interleaving this prevents, and the engine never
	// needs a re-entrant acquire — it holds the lease for the whole pass.
	taken, err = store.AcquireLease(ctx, "run-6", "replica-a", time.Minute)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if taken {
		t.Fatal("the holder re-acquired its own live lease")
	}
	// Refreshing, on the other hand, is the holder's own operation and must work.
	held, err := store.RefreshLease(ctx, "run-6", "replica-a", time.Minute)
	if err != nil || !held {
		t.Fatalf("refresh by the holder = %v, %v", held, err)
	}
	if held, _ := store.RefreshLease(ctx, "run-6", "replica-b", time.Minute); held {
		t.Fatal("a non-holder refreshed the lease")
	}

	// Releasing somebody else's lease must do nothing.
	if err := store.ReleaseLease(ctx, "run-6", "replica-b"); err != nil {
		t.Fatalf("release: %v", err)
	}
	taken, _ = store.AcquireLease(ctx, "run-6", "replica-b", time.Minute)
	if taken {
		t.Fatal("a non-holder released the lease")
	}

	if err := store.ReleaseLease(ctx, "run-6", "replica-a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	taken, err = store.AcquireLease(ctx, "run-6", "replica-b", time.Minute)
	if err != nil || !taken {
		t.Fatalf("acquire after release = %v, %v", taken, err)
	}

	// An expired lease is available again: this is what recovers a crashed replica.
	if _, err := store.AcquireLease(ctx, "run-7", "replica-a", time.Nanosecond); err != nil {
		t.Fatalf("short lease: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	taken, err = store.AcquireLease(ctx, "run-7", "replica-b", time.Minute)
	if err != nil || !taken {
		t.Fatalf("an expired lease was not reclaimable: %v, %v", taken, err)
	}
}

func storeTaskRevision(t *testing.T, store Store) {
	ctx := context.Background()
	task := &Task{
		ID: "task-1", RunID: "run-8", Process: "order.fulfil", Step: "approve",
		Status: TaskOpen, Role: "approver", CreatedAt: time.Now().UTC(),
	}
	if err := store.SaveTask(ctx, task); err != nil {
		t.Fatalf("save: %v", err)
	}

	first, err := store.GetTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	second, err := store.GetTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	first.Status, first.ClaimedBy = TaskClaimed, "alice"
	if err := store.SaveTask(ctx, first); err != nil {
		t.Fatalf("alice claims: %v", err)
	}
	// Two people clicking claim at the same moment: one wins, one is told.
	second.Status, second.ClaimedBy = TaskClaimed, "bob"
	if err := store.SaveTask(ctx, second); err != ErrRevisionConflict {
		t.Fatalf("concurrent claim = %v, want ErrRevisionConflict", err)
	}

	latest, _ := store.GetTask(ctx, "task-1")
	if latest.ClaimedBy != "alice" {
		t.Fatalf("claimed_by = %q, want alice", latest.ClaimedBy)
	}
}

func storeJoinAccumulation(t *testing.T, store Store) {
	ctx := context.Background()
	join := &Join{
		RunID: "run-9", Edge: "gather", Sources: []string{"a", "b"},
		Results: map[string]json.RawMessage{"a": json.RawMessage(`{"ok":true}`)},
		Errors:  map[string]string{},
	}
	if err := store.SaveJoin(ctx, join); err != nil {
		t.Fatalf("save: %v", err)
	}

	// A join accumulates across advance passes, possibly on different replicas
	// minutes apart, so it must reload exactly.
	loaded, err := store.GetJoin(ctx, "run-9", "gather")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(loaded.Results) != 1 || loaded.Emitted {
		t.Fatalf("join did not round-trip: %+v", loaded)
	}
	if loaded.Complete("all", 0) {
		t.Fatal("a join with one of two sources reported complete")
	}

	loaded.Results["b"] = json.RawMessage(`{"ok":true}`)
	if !loaded.Complete("all", 0) {
		t.Fatal("a join with both sources reported incomplete")
	}
	loaded.Emitted = true
	if err := store.SaveJoin(ctx, loaded); err != nil {
		t.Fatalf("resave: %v", err)
	}
	after, _ := store.GetJoin(ctx, "run-9", "gather")
	if !after.Emitted {
		t.Fatal("the emitted flag did not persist, so a late source would fire the join twice")
	}

	missing, err := store.GetJoin(ctx, "run-9", "absent")
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}
	if missing != nil {
		t.Fatal("an absent join should read as nil, not as an empty one")
	}
}

func storePurge(t *testing.T, store Store) {
	ctx := context.Background()
	run := newTestRun("run-10")
	completed := time.Now().UTC().Add(-48 * time.Hour)
	run.Status = StatusCompleted
	run.CompletedAt = &completed
	if err := store.CreateRun(ctx, run); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.SaveStep(ctx, &StepState{RunID: run.ID, Step: "reserve", Key: "reserve", Status: StepCompleted, StartedAt: completed}); err != nil {
		t.Fatalf("save step: %v", err)
	}
	if err := store.SaveTask(ctx, &Task{ID: "task-2", RunID: run.ID, Process: run.Process, Step: "approve", Status: TaskCompleted, CreatedAt: completed}); err != nil {
		t.Fatalf("save task: %v", err)
	}

	// A run younger than the cutoff must survive.
	purged, err := store.PurgeRuns(ctx, "", time.Now().UTC().Add(-72*time.Hour), 10)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if purged != 0 {
		t.Fatalf("purged %d runs that were inside the retention window", purged)
	}

	purged, err = store.PurgeRuns(ctx, "", time.Now().UTC().Add(-24*time.Hour), 10)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if purged != 1 {
		t.Fatalf("purged %d runs, want 1", purged)
	}
	if _, err := store.GetRun(ctx, run.ID); err == nil {
		t.Fatal("the run survived its purge")
	}
	// The dependents must go with it: an orphan step row pointing at a run that no
	// longer exists is the sort of leftover that grows unnoticed.
	states, _ := store.ListSteps(ctx, run.ID)
	if len(states) != 0 {
		t.Fatalf("%d step rows survived the purge", len(states))
	}
	tasks, _ := store.ListTasks(ctx, TaskFilter{RunID: run.ID, Status: TaskCompleted, Limit: 10})
	if len(tasks) != 0 {
		t.Fatalf("%d task rows survived the purge", len(tasks))
	}
}
