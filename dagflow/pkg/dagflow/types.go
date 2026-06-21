package dagflow

import "time"

type NodeType string
type EdgeType string
type TaskStatus string
type NodeStatus string
type RunMode string
type RouteMode string
type WorkflowMigrationPolicy string

type ErrorStrategy string

const (
	NodeFunction NodeType = "function"
	NodePage     NodeType = "page"
	NodeJoin     NodeType = "join"
	NodeNoop     NodeType = "noop"
	NodeWorkflow NodeType = "workflow"
	NodeScript   NodeType = "script"

	EdgeSimple     EdgeType = "simple"
	EdgeBranch     EdgeType = "branch"
	EdgeIterator   EdgeType = "iterator"
	EdgeError      EdgeType = "error"
	EdgeFanOut     EdgeType = "fanout"
	EdgeFanIn      EdgeType = "fanin"
	EdgeParallel   EdgeType = "parallel"
	EdgeJoin       EdgeType = "join"
	EdgeRace       EdgeType = "race"
	EdgeFallback   EdgeType = "fallback"
	EdgeCompensate EdgeType = "compensate"
	EdgeTimeout    EdgeType = "timeout"
	EdgeRetry      EdgeType = "retry"

	ModeInline      RunMode = "inline"
	ModeBackground  RunMode = "background"
	ModeDistributed RunMode = "distributed"

	RouteSync     RouteMode = "sync"
	RouteAsync    RouteMode = "async"
	RouteDetached RouteMode = "detached"
	RouteStream   RouteMode = "stream"
	RouteWebhook  RouteMode = "webhook"

	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskWaiting   TaskStatus = "waiting"
	TaskPaused    TaskStatus = "paused"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskCancelled TaskStatus = "cancelled"

	NodePending   NodeStatus = "pending"
	NodeQueued    NodeStatus = "queued"
	NodeRunning   NodeStatus = "running"
	NodeCompleted NodeStatus = "completed"
	NodeFailed    NodeStatus = "failed"
	NodeSkipped   NodeStatus = "skipped"
	NodeWaiting   NodeStatus = "waiting"
	NodeCancelled NodeStatus = "cancelled"

	ContinueSkip       ErrorStrategy = "skip"
	ContinueRetry      ErrorStrategy = "retry"
	ContinueResult     ErrorStrategy = "result"
	ContinueErrorEdge  ErrorStrategy = "error_edge"
	ContinueCompensate ErrorStrategy = "compensate"

	MigrationStrict     WorkflowMigrationPolicy = "strict"
	MigrationCompatible WorkflowMigrationPolicy = "compatible"
	MigrationRestart    WorkflowMigrationPolicy = "restart"
	MigrationManual     WorkflowMigrationPolicy = "manual"
)

type Handler func(*ExecutionContext, any) (any, error)
type FinalCallback func(*Task)

type Workflow struct {
	ID              string
	Name            string
	Version         string
	First           string
	Debug           bool
	MaxVisits       int
	Mode            RunMode
	Nodes           map[string]*Node
	Edges           []*Edge
	Outgoing        map[string][]*Edge
	Incoming        map[string][]*Edge
	FanIn           map[string][]*Edge
	Hash            string
	Metadata        map[string]any
	MigrationPolicy WorkflowMigrationPolicy
}

type Node struct {
	ID              string
	Type            NodeType
	Handler         string
	Workflow        string
	Mode            RunMode
	Await           bool
	Timeout         time.Duration
	Retry           int
	Last            bool
	Pause           bool
	When            string
	Condition       string
	SkipOnFalse     bool
	ContinueOnError bool
	Compensate      string
	OnError         string
	OnTimeout       string
	Pool            string
	Priority        int
	RateLimit       *RateLimitPolicy
	CircuitBreaker  *CircuitBreakerPolicy
	RetryPolicy     RetryPolicy
	Params          map[string]any
	Script          string
	InputSchema     string
	OutputSchema    string
	FailurePolicy   FailurePolicy
}

type Edge struct {
	ID             string
	From           string
	To             string
	Sources        []string
	Targets        []string
	Type           EdgeType
	When           string
	Condition      string
	Strategy       string
	MaxConcurrency int
	FailFast       bool
	Await          bool
	Timeout        time.Duration
	Quorum         int
	CancelLosers   bool
	Map            map[string]string
}

type RetryPolicy struct {
	MaxAttempts  int           `json:"max_attempts"`
	Strategy     string        `json:"strategy"`
	InitialDelay time.Duration `json:"initial_delay"`
	MaxDelay     time.Duration `json:"max_delay"`
	Jitter       bool          `json:"jitter"`
}

type RateLimitPolicy struct {
	Limit  int           `json:"limit"`
	Window time.Duration `json:"window"`
}

type CircuitBreakerPolicy struct {
	FailureThreshold int           `json:"failure_threshold"`
	ResetAfter       time.Duration `json:"reset_after"`
}

type FailurePolicy struct {
	Strategy     string `json:"strategy,omitempty"`
	ErrorNode    string `json:"error_node,omitempty"`
	FallbackNode string `json:"fallback_node,omitempty"`
}

type RouteGroup struct {
	ID          string        `json:"id"`
	Prefix      string        `json:"prefix"`
	Middlewares []string      `json:"middlewares,omitempty"`
	Groups      []*RouteGroup `json:"groups,omitempty"`
	Routes      []RouteConfig `json:"routes,omitempty"`
}

type JoinState struct {
	EdgeID           string            `json:"edge_id"`
	Sources          []string          `json:"sources"`
	CompletedSources map[string]bool   `json:"completed_sources"`
	Results          map[string]any    `json:"results"`
	Errors           map[string]string `json:"errors,omitempty"`
	Emitted          bool              `json:"emitted"`
}

type DLQItem struct {
	ID         string    `json:"id"`
	TaskID     string    `json:"task_id"`
	WorkflowID string    `json:"workflow_id"`
	NodeID     string    `json:"node_id"`
	Input      any       `json:"input,omitempty"`
	Error      string    `json:"error"`
	Attempts   int       `json:"attempts"`
	CreatedAt  time.Time `json:"created_at"`
}

type IdempotencyRecord struct {
	Key        string    `json:"key"`
	WorkflowID string    `json:"workflow_id"`
	InputHash  string    `json:"input_hash"`
	TaskID     string    `json:"task_id"`
	CreatedAt  time.Time `json:"created_at"`
}

type Chain struct {
	ID           string   `json:"id"`
	Name         string   `json:"name,omitempty"`
	Workflows    []string `json:"workflows"`
	Debug        bool     `json:"debug,omitempty"`
	When         string   `json:"when,omitempty"`
	Condition    string   `json:"condition,omitempty"`
	InputSchema  string   `json:"input_schema,omitempty"`
	OutputSchema string   `json:"output_schema,omitempty"`
	Tags         []string `json:"tags,omitempty"`
}

type ExecutionContext struct {
	ContextID    string         `json:"context_id"`
	TaskID       string         `json:"task_id"`
	WorkflowID   string         `json:"workflow_id"`
	NodeID       string         `json:"node_id"`
	RequestID    string         `json:"request_id,omitempty"`
	Attempt      int            `json:"attempt"`
	Values       map[string]any `json:"values,omitempty"`
	NodeParams   map[string]any `json:"node_params,omitempty"`
	TaskInput    any            `json:"task_input,omitempty"`
	LastResult   any            `json:"last_result,omitempty"`
	NodeResults  map[string]any `json:"node_results,omitempty"`
	PreviousNode string         `json:"previous_node,omitempty"`
}

type RunItem struct {
	NodeID string `json:"node_id"`
	Input  any    `json:"input,omitempty"`
	From   string `json:"from,omitempty"`
	EdgeID string `json:"edge_id,omitempty"`
}

type Task struct {
	ID              string                `json:"id"`
	WorkflowID      string                `json:"workflow_id"`
	Status          TaskStatus            `json:"status"`
	Input           any                   `json:"input,omitempty"`
	Result          any                   `json:"result,omitempty"`
	Error           string                `json:"error,omitempty"`
	LastError       string                `json:"last_error,omitempty"`
	LastResult      any                   `json:"last_result,omitempty"`
	CurrentNode     string                `json:"current_node,omitempty"`
	CurrentNodes    []string              `json:"current_nodes,omitempty"`
	PreviousNode    string                `json:"previous_node,omitempty"`
	PreviousNodes   []string              `json:"previous_nodes,omitempty"`
	Cursor          []RunItem             `json:"cursor,omitempty"`
	WaitingNodeID   string                `json:"waiting_node_id,omitempty"`
	ResumeToken     string                `json:"resume_token,omitempty"`
	FailedNodeID    string                `json:"failed_node_id,omitempty"`
	FailedInput     any                   `json:"failed_input,omitempty"`
	NodeStates      map[string]*NodeState `json:"node_states"`
	NodeResults     map[string]any        `json:"node_results,omitempty"`
	JoinStates      map[string]*JoinState `json:"join_states,omitempty"`
	Visits          map[string]int        `json:"visits,omitempty"`
	Audit           []AuditEvent          `json:"audit,omitempty"`
	Errors          []TaskError           `json:"errors,omitempty"`
	ParentTaskID    string                `json:"parent_task_id,omitempty"`
	IdempotencyKey  string                `json:"idempotency_key,omitempty"`
	WorkflowVersion string                `json:"workflow_version,omitempty"`
	DefinitionHash  string                `json:"definition_hash,omitempty"`
	TenantID        string                `json:"tenant_id,omitempty"`
	UserID          string                `json:"user_id,omitempty"`
	TraceID         string                `json:"trace_id,omitempty"`
	RestartedFrom   string                `json:"restarted_from,omitempty"`
	CreatedAt       time.Time             `json:"created_at"`
	UpdatedAt       time.Time             `json:"updated_at"`
	CompletedAt     *time.Time            `json:"completed_at,omitempty"`
}

type NodeState struct {
	NodeID      string           `json:"node_id"`
	ExecutionID string           `json:"execution_id,omitempty"`
	AttemptID   string           `json:"attempt_id,omitempty"`
	DedupKey    string           `json:"dedup_key,omitempty"`
	Mode        RunMode          `json:"mode"`
	Status      NodeStatus       `json:"status"`
	Input       any              `json:"input,omitempty"`
	Result      any              `json:"result,omitempty"`
	Error       string           `json:"error,omitempty"`
	JobID       string           `json:"job_id,omitempty"`
	Attempts    int              `json:"attempts"`
	StartedAt   time.Time        `json:"started_at,omitempty"`
	FinishedAt  time.Time        `json:"finished_at,omitempty"`
	Duration    time.Duration    `json:"duration"`
	Iterations  []IterationState `json:"iterations,omitempty"`
}

type IterationState struct {
	Index      int           `json:"index"`
	Input      any           `json:"input,omitempty"`
	Result     any           `json:"result,omitempty"`
	Error      string        `json:"error,omitempty"`
	Status     NodeStatus    `json:"status"`
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt time.Time     `json:"finished_at"`
	Duration   time.Duration `json:"duration"`
}

type AuditEvent struct {
	ID         string         `json:"id"`
	TaskID     string         `json:"task_id"`
	WorkflowID string         `json:"workflow_id"`
	NodeID     string         `json:"node_id,omitempty"`
	Event      string         `json:"event"`
	Message    string         `json:"message,omitempty"`
	Data       map[string]any `json:"data,omitempty"`
	At         time.Time      `json:"at"`
}

type TaskError struct {
	NodeID  string    `json:"node_id,omitempty"`
	Error   string    `json:"error"`
	Input   any       `json:"input,omitempty"`
	At      time.Time `json:"at"`
	Attempt int       `json:"attempt,omitempty"`
}

type ChainRun struct {
	ID          string       `json:"id"`
	ChainID     string       `json:"chain_id"`
	WorkflowIDs []string     `json:"workflow_ids"`
	Status      TaskStatus   `json:"status"`
	Input       any          `json:"input,omitempty"`
	Result      any          `json:"result,omitempty"`
	Error       string       `json:"error,omitempty"`
	Tasks       []*Task      `json:"tasks,omitempty"`
	Audit       []AuditEvent `json:"audit,omitempty"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
	CompletedAt *time.Time   `json:"completed_at,omitempty"`
}

type SchemaDef struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Required []string               `json:"required,omitempty"`
	Fields   map[string]SchemaField `json:"fields,omitempty"`
}

type SchemaField struct {
	Type     string `json:"type"`
	Required bool   `json:"required,omitempty"`
	Format   string `json:"format,omitempty"`
}

type WorkflowSnapshot struct {
	WorkflowID string    `json:"workflow_id"`
	Version    string    `json:"version"`
	Hash       string    `json:"hash"`
	Workflow   *Workflow `json:"workflow"`
	CreatedAt  time.Time `json:"created_at"`
}

type OutboxEvent struct {
	ID        string    `json:"id"`
	TaskID    string    `json:"task_id,omitempty"`
	NodeID    string    `json:"node_id,omitempty"`
	Topic     string    `json:"topic"`
	Payload   any       `json:"payload,omitempty"`
	Status    string    `json:"status"`
	Attempts  int       `json:"attempts"`
	NextRunAt time.Time `json:"next_run_at"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type WorkerLease struct {
	ID        string    `json:"id"`
	TaskID    string    `json:"task_id"`
	NodeID    string    `json:"node_id"`
	JobID     string    `json:"job_id"`
	WorkerID  string    `json:"worker_id"`
	ExpiresAt time.Time `json:"expires_at"`
	BeatAt    time.Time `json:"beat_at"`
}

type MetricSnapshot struct {
	Counters map[string]uint64 `json:"counters"`
	Gauges   map[string]int64  `json:"gauges"`
}
