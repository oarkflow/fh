package process

import (
	"context"
	"errors"
	"time"
)

// The store contract.
//
// Two implementations ship: store_sql.go, which is correct across replicas, and
// store_kv.go, which is correct within one process. A host can add a third by
// implementing this interface — it is deliberately small, and every method's
// contract is stated here rather than implied by the SQL one's behaviour.
//
// Two rules hold for every implementation and are what the engine relies on:
//
//  1. SaveRun is optimistic. It must fail with ErrRevisionConflict when the
//     stored revision differs from the one the caller read. The engine treats a
//     conflict as "somebody else advanced this run" and re-reads, which is the
//     only safe response.
//  2. AcquireLease is atomic. Two replicas calling it for the same run must not
//     both succeed. A store that cannot guarantee that must say so by refusing to
//     be used for a multi-replica deployment.

// ErrRevisionConflict reports a lost optimistic-concurrency race.
var ErrRevisionConflict = errors.New("ref/process: the run was modified concurrently")

// ErrRunNotFound reports an unknown run id.
var ErrRunNotFound = errors.New("ref/process: run not found")

// ErrTaskNotFound reports an unknown task id.
var ErrTaskNotFound = errors.New("ref/process: task not found")

// Store persists everything a durable run needs.
type Store interface {
	// Migrate creates whatever schema the store needs. It must be safe to call
	// repeatedly and concurrently, because every replica calls it at startup.
	Migrate(ctx context.Context) error

	// CreateRun inserts a new run. It must fail if the id already exists.
	CreateRun(ctx context.Context, run *Run) error
	// GetRun loads a run, returning ErrRunNotFound when it does not exist.
	GetRun(ctx context.Context, id string) (*Run, error)
	// FindRunByIdempotency finds an existing run for a process and key, so a
	// repeated start returns the original run rather than creating a second.
	FindRunByIdempotency(ctx context.Context, process, key string) (*Run, error)
	// SaveRun writes a run, asserting its revision. On success the run's Revision
	// is bumped in place, so a caller can save repeatedly within one pass.
	SaveRun(ctx context.Context, run *Run) error
	// ListRuns returns runs matching a filter, newest first.
	ListRuns(ctx context.Context, filter RunFilter) ([]*Run, error)
	// CountRuns counts runs matching a filter, for concurrency caps and for an
	// operations view's totals.
	CountRuns(ctx context.Context, filter RunFilter) (int, error)

	// SaveStep upserts a step state by (run, key).
	SaveStep(ctx context.Context, state *StepState) error
	// ListSteps returns a run's step states in completion order.
	ListSteps(ctx context.Context, runID string) ([]*StepState, error)
	// NextStepSequence allocates the next completion sequence for a run. It must
	// be monotonic per run; gaps are fine, reuse is not.
	NextStepSequence(ctx context.Context, runID string) (int64, error)

	// AddTimer schedules a wake-up.
	AddTimer(ctx context.Context, timer *Timer) error
	// DueTimers claims timers that are due, removing them from further claims.
	// It must be atomic so two replicas do not both fire the same timer.
	DueTimers(ctx context.Context, now time.Time, limit int) ([]*Timer, error)
	// DeleteTimer removes one timer.
	DeleteTimer(ctx context.Context, id string) error
	// DeleteRunTimers removes every timer for a run, which is how a resumed run
	// cancels the deadline it was waiting on.
	DeleteRunTimers(ctx context.Context, runID string, kinds ...string) error
	// ListTimers returns a run's pending timers, for the status view.
	ListTimers(ctx context.Context, runID string) ([]*Timer, error)

	// Subscribe records a parked run's interest in an event.
	Subscribe(ctx context.Context, subscription *Subscription) error
	// MatchSubscriptions finds subscriptions an event satisfies. An event with a
	// correlation matches subscriptions with that correlation or with none.
	MatchSubscriptions(ctx context.Context, event, correlation string) ([]*Subscription, error)
	// DeleteSubscription removes one subscription.
	DeleteSubscription(ctx context.Context, id string) error
	// DeleteRunSubscriptions removes every subscription for a run.
	DeleteRunSubscriptions(ctx context.Context, runID string) error
	// ListSubscriptions returns a run's subscriptions, for the status view.
	ListSubscriptions(ctx context.Context, runID string) ([]*Subscription, error)

	// SaveJoin upserts a join's accumulated state.
	SaveJoin(ctx context.Context, join *Join) error
	// GetJoin loads a join, returning nil when it does not exist yet.
	GetJoin(ctx context.Context, runID, edge string) (*Join, error)

	// AcquireLease claims exclusive advancing rights. It returns false when
	// somebody else holds a live lease.
	AcquireLease(ctx context.Context, runID, owner string, ttl time.Duration) (bool, error)
	// RefreshLease extends a lease the caller still owns.
	RefreshLease(ctx context.Context, runID, owner string, ttl time.Duration) (bool, error)
	// ReleaseLease drops a lease the caller owns. Releasing one held by somebody
	// else must be a no-op.
	ReleaseLease(ctx context.Context, runID, owner string) error

	// SaveTask upserts a task, asserting its revision like SaveRun does.
	SaveTask(ctx context.Context, task *Task) error
	// GetTask loads a task, returning ErrTaskNotFound when it does not exist.
	GetTask(ctx context.Context, id string) (*Task, error)
	// ListTasks returns tasks matching a filter.
	ListTasks(ctx context.Context, filter TaskFilter) ([]*Task, error)
	// CountOpenTasks counts a user's outstanding tasks, for least-loaded routing.
	CountOpenTasks(ctx context.Context, assignee string) (int, error)
	// DueTasks returns open tasks past an instant, for reminders and escalation.
	DueTasks(ctx context.Context, now time.Time, limit int) ([]*Task, error)

	// RecordEvent stores an inbound event, for replay and for auditing what woke
	// a run. Stores may bound the retention themselves.
	RecordEvent(ctx context.Context, event *Event) error

	// PurgeRuns deletes terminal runs older than an instant, with everything
	// belonging to them. It returns how many it removed.
	PurgeRuns(ctx context.Context, process string, before time.Time, limit int) (int, error)

	// Close releases resources the store itself owns. A store built on a borrowed
	// database must not close it.
	Close() error
}

// Enqueuer schedules a run to be advanced.
//
// This is how the engine scales past one replica: advancing is a job, and any
// replica that claims the job advances the run. A deployment without an enqueuer
// still works — the engine advances in the calling goroutine — which is right for
// tests and wrong for production, so the compiler requires one for any process
// that parks.
type Enqueuer interface {
	// EnqueueAdvance schedules an advance of a run.
	EnqueueAdvance(ctx context.Context, runID string) error
	// EnqueueAdvanceAt schedules an advance for later, which is how a timer
	// becomes a wake-up without a polling loop per run.
	EnqueueAdvanceAt(ctx context.Context, runID string, at time.Time) error
}

// StepRunner executes a step's body. ref/platform implements it by dispatching
// the step's intent, which is what makes the whole action catalog available
// inside a process.
type StepRunner interface {
	// RunStep executes a step and returns its result. The context carries the
	// step's timeout; the runner must honour it.
	RunStep(ctx context.Context, call StepCall) (any, error)
}

// StepCall is everything a runner needs to execute one step.
type StepCall struct {
	Run   *Run
	Step  *Step
	Frame Frame
	// Intent is the intent to run: the step's own, or its compensation when the
	// frame is a compensation frame.
	Intent string
	// Input is the already-shaped payload.
	Input any
	// Scope is the expression scope, so a runner that needs to render something
	// does not have to rebuild it.
	Scope Scope
	// Compensating reports that this call is undoing the step rather than running
	// it, which a runner may want to log differently.
	Compensating bool
}

// Notifier sends an operational notification: an SLA breach, an escalation, a
// task reminder. ref/platform routes it to a channel resource.
type Notifier interface {
	NotifyProcess(ctx context.Context, event NotifyEvent) error
}

// NotifyEvent is one operational notification.
type NotifyEvent struct {
	// Kind is "sla_breach", "escalation", "task_assigned", "task_reminder",
	// "task_overdue", "run_failed" or "compensation_failed".
	Kind    string
	Channel string
	Run     *Run
	Task    *Task
	Step    string
	Detail  string
}

// Locker and RateLimiter mirror the SPI contracts, declared here so this package
// does not import ref/platform and create a cycle. ref/platform passes its own
// implementations straight through.
type Locker interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (token string, acquired bool, err error)
	Release(ctx context.Context, key, token string) error
}

// RateLimiter throttles step execution.
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (allowed bool, remaining int, resetAt time.Time, err error)
}

// Observer is notified of run lifecycle transitions, for metrics and logs. Every
// method must be safe for concurrent use and must not block.
type Observer interface {
	RunStarted(run *Run)
	RunFinished(run *Run)
	StepFinished(run *Run, state *StepState)
	RunParked(run *Run, wait WaitState)
}
