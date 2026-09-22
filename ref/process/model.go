// Package process is the durable orchestration tier of the REF no-code platform.
//
// A REF intent lives and dies inside one request. A process run is a row. It
// parks on a timer, an external event, an operator gate or a human task; it
// survives a crash, a deploy and a replica rescheduling; and when a late step
// fails it compensates the earlier ones that already committed. Those properties
// cannot be added to a request-scoped dependency graph by labelling its edges —
// they need persisted state, leases and optimistic versions, which is what this
// package is.
//
// The split of responsibility with ref/platform is deliberate:
//
//   - This package owns the state machine: the row shapes, the store contract,
//     the advance loop, the edge semantics, timers, events, joins, leases,
//     compensation and human tasks. It knows nothing about BCL.
//   - ref/platform owns the compiler: it turns `process` blocks into Definitions
//     and supplies a StepRunner that invokes intents.
//
// A step's body is an intent, so every node type, resource and action available
// to a request is available to a process. There is no second, weaker action
// catalog to learn.
package process

import (
	"encoding/json"
	"time"
)

// Status is a run's lifecycle state.
type Status string

// The run statuses. A run in any of the terminal states never advances again.
const (
	// StatusPending is a created run that has not executed a step yet.
	StatusPending Status = "pending"
	// StatusRunning means a replica holds the lease and is executing steps.
	StatusRunning Status = "running"
	// StatusWaiting means the run is parked: on a timer, an event, an operator
	// gate, a human task or a rate limit. Something external must wake it.
	StatusWaiting Status = "waiting"
	// StatusCompensating means a step failed and the engine is undoing the
	// committed work behind it, newest first.
	StatusCompensating Status = "compensating"

	// StatusCompleted is terminal success.
	StatusCompleted Status = "completed"
	// StatusFailed is terminal failure. Compensation, if any, has already run.
	StatusFailed Status = "failed"
	// StatusCancelled is terminal by decision — a cancel edge or an operator.
	StatusCancelled Status = "cancelled"
)

// Terminal reports whether a status never advances again.
func (s Status) Terminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// Active reports whether a run is still the engine's responsibility.
func (s Status) Active() bool { return !s.Terminal() }

// StepStatus is one step execution's state.
type StepStatus string

// The step statuses.
const (
	StepRunning     StepStatus = "running"
	StepCompleted   StepStatus = "completed"
	StepFailed      StepStatus = "failed"
	StepSkipped     StepStatus = "skipped"
	StepWaiting     StepStatus = "waiting"
	StepCompensated StepStatus = "compensated"
)

// Run is one execution of a process definition.
//
// Revision is the optimistic-concurrency guard. Every write asserts the revision
// it read and bumps it; a mismatch means another replica advanced the run
// concurrently, and the loser re-reads rather than overwriting. Without it, two
// replicas that both believed they held the lease — which a lease expiry makes
// possible — would interleave their cursor writes and lose steps.
type Run struct {
	ID      string `json:"id"`
	Process string `json:"process"`
	// Version is the definition version this run started on. A run keeps
	// executing its own version unless the definition's migration policy says
	// otherwise, because silently migrating an in-flight run through a
	// redesigned graph is how money goes missing.
	Version int    `json:"version"`
	Status  Status `json:"status"`

	Input  json.RawMessage `json:"input,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
	// FailedStep names the step whose failure ended the run, which is the first
	// thing an operator looking at a failed run wants to know.
	FailedStep string `json:"failed_step,omitempty"`

	TenantID       string `json:"tenant_id,omitempty"`
	PrincipalID    string `json:"principal_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// CorrelationID ties a run to whatever started it, for tracing across
	// systems.
	CorrelationID string `json:"correlation_id,omitempty"`

	// Frames is the cursor: the work this run still has to do. An empty cursor
	// on an active run means the run has finished its graph.
	Frames []Frame `json:"frames,omitempty"`
	// Visits counts executions per step, enforcing the definition's MaxVisits
	// and bounding loop_until edges.
	Visits map[string]int `json:"visits,omitempty"`
	// Steps counts total step executions against the definition's MaxSteps.
	Steps int `json:"steps"`

	// Compensating is the reverse-order list of completed steps still to be
	// compensated. It is persisted so an interrupted rollback resumes rather
	// than leaving half-undone work.
	Compensating []string `json:"compensating,omitempty"`

	Revision  int64     `json:"revision"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	// DeadlineAt is when the run's own timeout expires.
	DeadlineAt *time.Time `json:"deadline_at,omitempty"`
	// SLATargetAt and SLABreachAt drive the at-risk and breached states an
	// operations view needs before anything has actually failed.
	SLATargetAt *time.Time `json:"sla_target_at,omitempty"`
	SLABreachAt *time.Time `json:"sla_breach_at,omitempty"`
	// SLABreached records that the breach action already fired, so it fires once.
	SLABreached bool `json:"sla_breached,omitempty"`

	// Waiting describes why a parked run is parked, for the status API.
	Waiting *WaitState `json:"waiting,omitempty"`
}

// WaitState explains a parked run.
type WaitState struct {
	// Reason is "timer", "event", "manual", "task", "rate_limit" or "child".
	Reason string     `json:"reason"`
	Step   string     `json:"step,omitempty"`
	Edge   string     `json:"edge,omitempty"`
	Event  string     `json:"event,omitempty"`
	TaskID string     `json:"task_id,omitempty"`
	Until  *time.Time `json:"until,omitempty"`
	Detail string     `json:"detail,omitempty"`
}

// Frame is one unit of pending work: run this step with this input.
//
// A frame carries where it came from, because edge semantics are a property of
// the traversal rather than of the target step: the same step reached by a retry
// edge and by a simple edge behaves differently.
type Frame struct {
	Step  string          `json:"step"`
	Input json.RawMessage `json:"input,omitempty"`
	From  string          `json:"from,omitempty"`
	Edge  string          `json:"edge,omitempty"`
	// Key distinguishes iterations of the same step, e.g. "notify[3]". Step
	// state is recorded per key, so an iterator's twelfth item has its own row.
	Key string `json:"key,omitempty"`
	// Attempt is the retry attempt number this frame represents, starting at 1.
	Attempt int `json:"attempt,omitempty"`
	// Compensate marks a frame that runs a step's compensation rather than the
	// step itself.
	Compensate bool `json:"compensate,omitempty"`
	// NotBefore parks this frame until an instant, without a separate timer row —
	// used by retry backoff, where the delay is short and a row would be waste.
	NotBefore *time.Time `json:"not_before,omitempty"`
}

// StateKey is the step-state key this frame writes to.
func (f Frame) StateKey() string {
	if f.Key != "" {
		return f.Key
	}
	return f.Step
}

// StepState is one step execution's record. It is the audit trail of what
// happened inside a run, and the input to compensation.
type StepState struct {
	RunID string `json:"run_id"`
	Step  string `json:"step"`
	// Key is the state key, distinguishing iterations.
	Key    string     `json:"key"`
	Status StepStatus `json:"status"`
	// Attempt is how many times this step has been tried.
	Attempt int             `json:"attempt"`
	Input   json.RawMessage `json:"input,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   string          `json:"error,omitempty"`
	// Sequence is the global completion order within the run. Compensation walks
	// it backwards, which is the only correct order to undo committed work in.
	Sequence   int64      `json:"sequence"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// Compensated records that this step's compensation has run, so a resumed
	// rollback does not undo the same work twice.
	Compensated bool `json:"compensated,omitempty"`
}

// Timer is a scheduled wake-up for a run.
type Timer struct {
	ID    string    `json:"id"`
	RunID string    `json:"run_id"`
	Fire  time.Time `json:"fire_at"`
	// Kind is "delayed", "timeout", "wait_event", "escalation", "sla" or
	// "run_timeout". The engine dispatches on it when the timer fires.
	Kind    string          `json:"kind"`
	Step    string          `json:"step,omitempty"`
	Edge    string          `json:"edge,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	// OnFire names the step to go to when this timer fires, for a timeout edge's
	// on_timeout target.
	OnFire    string    `json:"on_fire,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Subscription is a parked run's interest in an external event.
//
// Correlation is what makes a signal reach the right run: an event named
// "payment.settled" with correlation "order-4821" wakes the one run waiting for
// that order, not every run waiting for a settlement.
type Subscription struct {
	ID          string     `json:"id"`
	RunID       string     `json:"run_id"`
	Event       string     `json:"event"`
	Correlation string     `json:"correlation,omitempty"`
	Step        string     `json:"step,omitempty"`
	Edge        string     `json:"edge,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
}

// Join accumulates the results of a fan-in, join or quorum edge.
//
// It is a row rather than in-memory state because the sources may complete in
// different advance passes, on different replicas, minutes apart.
type Join struct {
	RunID   string   `json:"run_id"`
	Edge    string   `json:"edge"`
	Sources []string `json:"sources"`
	// Results holds each completed source's result, keyed by source step.
	Results map[string]json.RawMessage `json:"results,omitempty"`
	// Errors holds each failed source's error, so a partial_success strategy can
	// report what did not work.
	Errors map[string]string `json:"errors,omitempty"`
	// Emitted records that the join has already continued, so a late source does
	// not fire the downstream step a second time.
	Emitted   bool      `json:"emitted"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Complete reports whether the join's strategy is satisfied.
//
// The strategies match DAGFlow's, because they are the ones real processes need:
// wait for everything, wait for anything, wait for a quorum, or accept whatever
// finished and carry the failures forward.
func (j *Join) Complete(strategy string, quorum int) bool {
	done := len(j.Results)
	failed := len(j.Errors)
	total := len(j.Sources)
	switch strategy {
	case "any", "first", "any_success", "first_success":
		return done >= 1
	case "quorum":
		if quorum <= 0 {
			quorum = total/2 + 1
		}
		return done >= quorum
	case "partial_success":
		// Every source has reported one way or the other; at least one worked.
		return done+failed >= total && done >= 1
	case "first_failure":
		return failed >= 1
	default: // "all" and the empty default
		return done >= total
	}
}

// Lease is one replica's exclusive claim on advancing a run.
//
// The expiry is what makes a crashed replica recoverable: the lease lapses and
// another replica picks the run up. The optimistic Revision on Run is what makes
// that safe even if the original replica comes back and still thinks it holds
// the lease.
type Lease struct {
	RunID     string    `json:"run_id"`
	Owner     string    `json:"owner"`
	ExpiresAt time.Time `json:"expires_at"`
}

// TaskStatus is a human task's state.
type TaskStatus string

// The task statuses.
const (
	TaskOpen      TaskStatus = "open"
	TaskClaimed   TaskStatus = "claimed"
	TaskCompleted TaskStatus = "completed"
	TaskCancelled TaskStatus = "cancelled"
	TaskEscalated TaskStatus = "escalated"
)

// Task is a unit of work for a person. It is a row, so it survives restarts and
// appears in a work list until somebody deals with it.
type Task struct {
	ID      string     `json:"id"`
	RunID   string     `json:"run_id"`
	Process string     `json:"process"`
	Step    string     `json:"step"`
	Status  TaskStatus `json:"status"`

	Title        string `json:"title,omitempty"`
	Instructions string `json:"instructions,omitempty"`

	// Assignee, Role and Queue address the task. A task with an assignee is for
	// one person; one with a role or queue is for whoever claims it first.
	Assignee string   `json:"assignee,omitempty"`
	Role     string   `json:"role,omitempty"`
	Queue    string   `json:"queue,omitempty"`
	Skills   []string `json:"skills,omitempty"`
	// ForbidPrincipals are users who must not take this task — the four-eyes
	// control that stops the raiser of a request from approving it.
	ForbidPrincipals []string `json:"forbid_principals,omitempty"`

	// Actions are the outcomes a person may choose. The chosen one is published
	// as "action" on the step result, which is what an outgoing branch edge tests.
	Actions []string `json:"actions,omitempty"`
	// FormSchema names the schema a completion payload is validated against.
	FormSchema string `json:"form_schema,omitempty"`

	Priority int             `json:"priority,omitempty"`
	TenantID string          `json:"tenant_id,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`

	ClaimedBy   string     `json:"claimed_by,omitempty"`
	ClaimedAt   *time.Time `json:"claimed_at,omitempty"`
	CompletedBy string     `json:"completed_by,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	// Action is what the person chose.
	Action string          `json:"action,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`

	DueAt      *time.Time `json:"due_at,omitempty"`
	ReminderAt *time.Time `json:"reminder_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	Revision   int64      `json:"revision"`
}

// Open reports whether the task is still outstanding.
func (t *Task) Open() bool {
	return t.Status == TaskOpen || t.Status == TaskClaimed || t.Status == TaskEscalated
}

// Event is an inbound external signal.
type Event struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Correlation string          `json:"correlation,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	ReceivedAt  time.Time       `json:"received_at"`
}

// RunFilter narrows a run listing for an operations view.
type RunFilter struct {
	Process  string
	Status   Status
	TenantID string
	// Waiting selects only parked runs, whatever they are parked on.
	Waiting bool
	// Overdue selects runs past their SLA breach time, which is the query an
	// operations dashboard actually opens with.
	Overdue bool
	Since   *time.Time
	Limit   int
	Offset  int
}

// TaskFilter narrows a task listing for a work list.
type TaskFilter struct {
	Process  string
	Status   TaskStatus
	Assignee string
	// Roles lists the roles the viewer holds; a task addressed to any of them
	// matches. This is what makes "my queue" a single query.
	Roles    []string
	Queue    string
	TenantID string
	RunID    string
	// Overdue selects tasks past their due time.
	Overdue bool
	Limit   int
	Offset  int
}

// Snapshot is the operator-facing view of a run: its own state plus every step
// that has executed and whatever it is waiting on.
type Snapshot struct {
	Run           *Run            `json:"run"`
	Steps         []*StepState    `json:"steps,omitempty"`
	Tasks         []*Task         `json:"tasks,omitempty"`
	Timers        []*Timer        `json:"timers,omitempty"`
	Subscriptions []*Subscription `json:"subscriptions,omitempty"`
}
