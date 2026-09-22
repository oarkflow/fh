package platform

// ProcessSpec describes one durable, long-running orchestration: a persisted
// state machine whose steps invoke intents and whose edges carry explicit
// traversal semantics.
//
// A process is what an intent cannot be. An intent lives and dies inside one
// request; a process run is a row. It parks on a timer, an external event, a
// manual gate or a human task; it survives a deploy, a crash and a replica
// rescheduling; it compensates already-committed work when a later step fails.
// Those semantics need persisted state, leases and optimistic versions — which
// is why they are a separate tier rather than extra labels on REF's
// dependency edges.
type ProcessSpec struct {
	Name        string `bcl:",id"`
	Description string `bcl:"description"`
	// Store names a process-store resource (store.sql or store.file). It holds
	// runs, step states, timers, event subscriptions, joins, leases and human
	// tasks.
	Store string `bcl:"store"`
	// Queue names the durable queue that carries advance work. Without it a run
	// can only be advanced in-process by the caller, which is fine for tests
	// and wrong for production — the compiler warns by requiring it for any
	// process that declares a timer, wait_event or human task.
	Queue string `bcl:"queue"`
	// Start names the first step. Required: a process with no entry point is a
	// configuration error, not an empty process.
	Start string `bcl:"start"`

	// Version lets a deployment decide what happens to runs started by an
	// earlier definition. See MigrationPolicy.
	Version int `bcl:"version"`
	// MigrationPolicy is "pin" (default: an in-flight run keeps executing the
	// definition it started on), "migrate" (adopt the new definition at the
	// next step boundary) or "fail" (refuse to advance and surface the run for
	// an operator). "pin" is the default because silently migrating a run
	// through a redesigned graph is how money goes missing.
	MigrationPolicy string `bcl:"migration_policy,ident"`

	// Timeout bounds one whole run. MaxSteps bounds total step executions,
	// which is the backstop against a loop_until edge that never converges.
	Timeout  Duration `bcl:"timeout"`
	MaxSteps int      `bcl:"max_steps"`
	// MaxVisits caps how many times any single step may execute in one run.
	MaxVisits int `bcl:"max_visits"`

	InputSchema string `bcl:"input_schema"`
	// Idempotency names the input path whose value deduplicates run creation,
	// e.g. "order.id". Starting twice with the same key returns the existing
	// run instead of creating a second one.
	Idempotency string `bcl:"idempotency"`

	// Retry is the default retry policy for every step that does not set its
	// own. Unlike an intent node's retry, these attempts are persisted and
	// survive a restart.
	Retry *RetrySpec `bcl:"retry"`

	// SLA declares when a run is late and what to do about it.
	SLA *SLASpec `bcl:"sla"`

	// Concurrency caps simultaneously advancing runs of this process across the
	// whole deployment, enforced through the store's leases. Use it to protect
	// a downstream dependency that cannot take the full fan-out.
	Concurrency int `bcl:"concurrency"`

	// Retention removes terminal runs after this age. Zero keeps them forever,
	// which is a deliberate choice an operator should make rather than a
	// default that quietly grows a table without bound.
	Retention Duration `bcl:"retention"`

	InputData  *DataSpec `bcl:"input_data"`
	OutputData *DataSpec `bcl:"output_data"`

	Steps []StepSpec `bcl:"step,block"`
	Edges []EdgeSpec `bcl:"edge,block"`
}

// StepSpec is one durable step. Its body is an intent, so every node type,
// resource and action available to a request is available to a process — there
// is no second, weaker action catalog to learn.
type StepSpec struct {
	Name        string `bcl:",id"`
	Description string `bcl:"description"`
	// Intent names the intent this step runs.
	Intent string `bcl:"intent"`
	// Process, when set instead of Intent, runs a child process as a sub-run.
	// The parent parks until the child reaches a terminal state.
	Process string `bcl:"process"`

	// Family is catalog metadata for an editor, mirroring NodeSpec.Family. BCL
	// cannot bind a field named `type`.
	Family string `bcl:"family,ident"`

	// Compensate names the intent that undoes this step's committed effect. On
	// terminal failure the engine runs the compensations of every completed
	// step in reverse completion order — a saga, persisted, so an interrupted
	// rollback resumes rather than leaving half-undone work.
	Compensate string `bcl:"compensate"`

	Retry   *RetrySpec `bcl:"retry"`
	Timeout Duration   `bcl:"timeout"`

	// Terminal marks a step after which the run completes successfully, even if
	// it has outgoing edges that do not traverse.
	Terminal bool `bcl:"terminal"`

	// SkipWhen skips this step when the expression is true, publishing
	// SkipResult in place of the intent's output.
	SkipWhen   string         `bcl:"skip_when"`
	SkipResult map[string]any `bcl:"skip_result"`

	// Task turns this step into a human task: the run parks and a work item
	// appears in the task list until somebody completes it.
	Task *TaskSpec `bcl:"task"`

	// Authz gates the step. A denied step fails the run rather than skipping
	// it, unless OnDeny says otherwise.
	Authz *AuthzSpec `bcl:"authz"`

	InputData  *DataSpec `bcl:"input_data"`
	OutputData *DataSpec `bcl:"output_data"`

	// Lock names a lock resource and Key the lock key expression, so a step
	// that must not run concurrently with itself across runs (adjusting shared
	// inventory, say) holds a real lease while it does.
	Lock     string   `bcl:"lock"`
	LockKey  string   `bcl:"lock_key"`
	LockTTL  Duration `bcl:"lock_ttl"`
	LockWait Duration `bcl:"lock_wait"`

	// RateLimit names a rate-limiter resource; Limit and Window bound how often
	// this step may execute. When the limit is hit the run parks and retries
	// rather than failing, because a rate limit is backpressure, not an error.
	RateLimit      string   `bcl:"rate_limit"`
	RateLimitKey   string   `bcl:"rate_limit_key"`
	Limit          int      `bcl:"limit"`
	Window         Duration `bcl:"window"`
	CircuitBreaker string   `bcl:"circuit_breaker"`
}

// TaskSpec turns a step into a human task. The run parks durably; the task is a
// row that survives restarts and appears in the task API until completed,
// cancelled or escalated.
type TaskSpec struct {
	// Title and Instructions are shown to the assignee. Both support {{ }}
	// templating over the run's own data.
	Title        string `bcl:"title"`
	Instructions string `bcl:"instructions"`

	// Assignee pins the task to one user. Role and Queue address a group
	// instead; Strategy then decides who inside that group gets it.
	Assignee string `bcl:"assignee"`
	Role     string `bcl:"role"`
	Queue    string `bcl:"queue"`
	// Skills requires the assignee to hold every listed skill, for
	// skill-matched routing.
	Skills []string `bcl:"skills"`
	// Strategy is "queue" (default: anybody addressed may claim it),
	// "round_robin", "least_loaded" or "direct".
	Strategy string `bcl:"strategy,ident"`

	// FormSchema names a schema block the completion payload is validated
	// against, so a human cannot submit a shape the next step cannot read.
	FormSchema string `bcl:"form_schema"`
	// Actions are the named outcomes a human may choose, e.g. [approve reject].
	// The chosen action is published as "action" on the step result, which is
	// what an outgoing branch edge tests.
	Actions []string `bcl:"actions"`

	Priority int `bcl:"priority"`
	// Due is how long the assignee has. On expiry the escalation edge from this
	// step fires, or the run fails if there is none.
	Due Duration `bcl:"due"`
	// Reminder sends a notification this long before Due.
	Reminder Duration `bcl:"reminder"`
	// Escalate names the role the task is reassigned to when Due passes, as an
	// alternative to an escalation edge.
	Escalate string `bcl:"escalate"`
	// AllowReassign lets an assignee hand the task to somebody else.
	AllowReassign bool `bcl:"allow_reassign"`
	// AllowSelfAssign lets any addressed user claim it. When false only an
	// administrator may assign it, which is what a segregation-of-duties
	// control usually needs.
	AllowSelfAssign *bool `bcl:"allow_self_assign"`
	// ForbidPrincipals lists run-data paths whose users must not be the
	// assignee — the four-eyes control: forbid_principals [run.input.created_by]
	// stops the person who raised a request from approving it.
	ForbidPrincipals []string `bcl:"forbid_principals"`
}

// SLASpec declares when work is late and what happens then.
type SLASpec struct {
	// Target is the intended duration; Breach is when it is definitively late.
	// Between the two a run is "at risk", which is what an operations dashboard
	// wants to show before anything has actually failed.
	Target Duration `bcl:"target"`
	Breach Duration `bcl:"breach"`
	// OnBreach is "notify" (default), "escalate", "cancel" or "fail".
	OnBreach string `bcl:"on_breach,ident"`
	// Notify names a notification channel resource.
	Notify string `bcl:"notify"`
	// Escalate names the role a breached run's tasks are reassigned to.
	Escalate string `bcl:"escalate"`
	// BusinessHours restricts elapsed-time accounting to working hours, so a
	// 4-hour SLA raised on Friday evening is not breached by Monday morning.
	// Format is "Mon-Fri 09:00-17:00" with an optional trailing timezone.
	BusinessHours string `bcl:"business_hours"`
}

// EdgeSpec is one durable transition between steps. Its Type carries the
// traversal semantics; every type is listed in the edge-type catalog and
// implemented by ref/process.
//
// Conditions: every edge type, without exception, evaluates When and Condition
// before it traverses. They are two names for the same check — When reads
// better for a guard, Condition for a data test — and both must pass. This is
// one contract across all edge types so a fan-out or manual edge cannot bypass
// a guard that a simple edge would honour.
type EdgeSpec struct {
	Name string `bcl:",id"`
	// Kind is the edge type. BCL cannot bind a field named `type`, so the
	// traversal semantic is spelled `kind`.
	Kind string `bcl:"kind,ident"`

	// From/To are the single-source, single-target form. Sources/Targets are
	// the multi form used by fan-in, fan-out, parallel, race and quorum.
	From    string   `bcl:"from"`
	To      string   `bcl:"to"`
	Sources []string `bcl:"sources"`
	Targets []string `bcl:"targets"`

	// Condition must hold for the edge to traverse. Every edge type evaluates it,
	// without exception. BCL reserves `when` as a parser keyword, so `condition`
	// is the only spelling.
	Condition string `bcl:"condition"`

	// Strategy selects the completion rule for fan-in, join and quorum edges:
	// "all" (default), "any", "quorum", "partial_success" or "best_score".
	Strategy string `bcl:"strategy,ident"`
	// Quorum is how many sources must complete for a quorum strategy. Zero
	// means a simple majority of Sources.
	Quorum int `bcl:"quorum"`

	// MaxConcurrency bounds simultaneous targets for parallel and fan-out
	// edges, and is the cycle cap for loop_until.
	MaxConcurrency int `bcl:"max_concurrency"`
	// FailFast cancels sibling branches as soon as one fails.
	FailFast bool `bcl:"fail_fast"`
	// ContinueOnError keeps traversing when a branch fails, collecting the
	// error instead of failing the run.
	ContinueOnError bool `bcl:"continue_on_error"`
	// CancelLosers cancels the losing branches of a race once a winner is
	// known. Leave it off when the losers have side effects worth completing.
	CancelLosers bool `bcl:"cancel_losers"`

	// Timeout means different things per type, each documented in the edge
	// catalog: the park duration for delayed, the deadline for timeout and
	// wait_event, the backoff for retry, the spacing for rate_limited.
	Timeout Duration `bcl:"timeout"`
	// Attempts is the retry count for a retry edge.
	Attempts int `bcl:"attempts"`

	// Event is the external event name a wait_event edge waits for.
	Event string `bcl:"event"`
	// Correlation is an expression producing the key that matches an inbound
	// event to this waiting run, e.g. "run.input.order_id".
	Correlation string `bcl:"correlation"`
	// OnTimeout names the step to go to when a wait_event or timeout edge
	// expires. Without it, expiry fails the run.
	OnTimeout string `bcl:"on_timeout"`

	// Weight is the relative share of a weighted edge; Priority selects the
	// winner among priority edges (lowest value wins).
	Weight   float64 `bcl:"weight"`
	Priority int     `bcl:"priority"`

	// RateLimit names a rate-limiter resource for a rate_limited edge, with
	// Limit and Window. A rate_limited edge parks the run until the window
	// admits it; it never sleeps a worker thread.
	RateLimit string   `bcl:"rate_limit"`
	Limit     int      `bcl:"limit"`
	Window    Duration `bcl:"window"`

	// ItemsPath is the collection an iterator or batch_iterator walks, relative
	// to the source step's result. BatchSize groups a batch_iterator.
	ItemsPath string `bcl:"items_path"`
	BatchSize int    `bcl:"batch_size"`
	// TargetsPath is where a dynamic_fanout edge reads its runtime target list.
	TargetsPath string `bcl:"targets_path"`

	// Thresholds route by numeric range for a threshold edge. The first rule
	// whose bounds contain the value wins.
	Thresholds []ThresholdSpec `bcl:"threshold,block"`

	// Escalate names the role an escalation edge reassigns work to.
	Escalate string `bcl:"escalate"`
	// Notify names a channel an escalation edge notifies.
	Notify string `bcl:"notify"`

	// Data shapes the payload handed to the targets. Filters inside it decide
	// whether the edge traverses at all, which is how a filter edge works.
	Data *DataSpec `bcl:"data"`
	// Extract is a shorthand for Data.Extract when all you need is renaming. BCL
	// cannot bind a field named `map`.
	Extract map[string]string `bcl:"extract"`

	Description string `bcl:"description"`
}

// ThresholdSpec is one numeric range rule of a threshold edge. Min is
// inclusive, Max exclusive, and either may be omitted for an open bound.
type ThresholdSpec struct {
	Name   string   `bcl:",id"`
	Min    *float64 `bcl:"min"`
	Max    *float64 `bcl:"max"`
	Value  string   `bcl:"value"`
	Target string   `bcl:"target"`
	// Reason is recorded on the run so an auditor can see which band applied
	// and why, rather than inferring it from the path taken.
	Reason string         `bcl:"reason"`
	Data   map[string]any `bcl:"data"`
}
