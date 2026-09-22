package process

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// A compiled process definition.
//
// Everything a run needs to execute is resolved here, once, at load time: step
// lookups, adjacency, edge classification, and the validation that turns a
// misconfigured graph into a deployment that refuses to start rather than a run
// that wedges at 3am. ref/platform builds these from BCL; a host could build one
// by hand.

// EdgeType is one traversal semantic.
type EdgeType string

// The edge types. These are the full set DAGFlow arrived at, because they are the
// set real processes turn out to need — and each one here is implemented, not
// merely named.
const (
	// Sequencing.
	EdgeSimple          EdgeType = "simple"
	EdgeBranch          EdgeType = "branch"
	EdgeSwitch          EdgeType = "switch"
	EdgeConditionalFork EdgeType = "conditional_fork"
	EdgeThreshold       EdgeType = "threshold"
	EdgeWeighted        EdgeType = "weighted"
	EdgePriority        EdgeType = "priority"

	// Concurrency.
	EdgeFanOut        EdgeType = "fanout"
	EdgeDynamicFanOut EdgeType = "dynamic_fanout"
	EdgeFanIn         EdgeType = "fanin"
	EdgeJoin          EdgeType = "join"
	EdgeQuorum        EdgeType = "quorum"
	EdgeParallel      EdgeType = "parallel"
	EdgeRace          EdgeType = "race"

	// Iteration.
	EdgeIterator      EdgeType = "iterator"
	EdgeBatchIterator EdgeType = "batch_iterator"
	EdgeLoopUntil     EdgeType = "loop_until"

	// Reliability.
	EdgeRetry       EdgeType = "retry"
	EdgeTimeout     EdgeType = "timeout"
	EdgeRateLimited EdgeType = "rate_limited"
	EdgeError       EdgeType = "error"
	EdgeFallback    EdgeType = "fallback"
	EdgeCompensate  EdgeType = "compensate"

	// Suspension.
	EdgeDelayed    EdgeType = "delayed"
	EdgeWaitEvent  EdgeType = "wait_event"
	EdgeManual     EdgeType = "manual"
	EdgeEscalation EdgeType = "escalation"

	// Termination and shaping.
	EdgeCancel     EdgeType = "cancel"
	EdgeTransform  EdgeType = "transform"
	EdgeFilter     EdgeType = "filter"
	EdgeStreamPipe EdgeType = "stream_pipe"
)

// errorPathEdges are the types that fire on failure rather than success. Keeping
// this as one list is what guarantees a success-path resolution can never
// traverse an error edge and vice versa — the bug class where a compensation
// runs on the happy path.
var errorPathEdges = map[EdgeType]bool{
	EdgeError:      true,
	EdgeFallback:   true,
	EdgeCompensate: true,
}

// ErrorPath reports whether this edge type fires on failure.
func (t EdgeType) ErrorPath() bool { return errorPathEdges[t] }

// Parks reports whether traversing this edge suspends the run.
func (t EdgeType) Parks() bool {
	switch t {
	case EdgeDelayed, EdgeWaitEvent, EdgeManual, EdgeEscalation, EdgeRateLimited:
		return true
	default:
		return false
	}
}

// Multi reports whether the type addresses several sources or targets.
func (t EdgeType) Multi() bool {
	switch t {
	case EdgeFanOut, EdgeDynamicFanOut, EdgeFanIn, EdgeJoin, EdgeQuorum, EdgeParallel, EdgeRace, EdgeConditionalFork:
		return true
	default:
		return false
	}
}

// KnownEdgeType reports whether a name is an implemented edge type.
func KnownEdgeType(name string) bool {
	_, ok := edgeTypeNames[EdgeType(strings.ToLower(strings.TrimSpace(name)))]
	return ok
}

var edgeTypeNames = func() map[EdgeType]struct{} {
	all := []EdgeType{
		EdgeSimple, EdgeBranch, EdgeSwitch, EdgeConditionalFork, EdgeThreshold, EdgeWeighted, EdgePriority,
		EdgeFanOut, EdgeDynamicFanOut, EdgeFanIn, EdgeJoin, EdgeQuorum, EdgeParallel, EdgeRace,
		EdgeIterator, EdgeBatchIterator, EdgeLoopUntil,
		EdgeRetry, EdgeTimeout, EdgeRateLimited, EdgeError, EdgeFallback, EdgeCompensate,
		EdgeDelayed, EdgeWaitEvent, EdgeManual, EdgeEscalation,
		EdgeCancel, EdgeTransform, EdgeFilter, EdgeStreamPipe,
	}
	set := make(map[EdgeType]struct{}, len(all))
	for _, item := range all {
		set[item] = struct{}{}
	}
	return set
}()

// Guard is a compiled condition. ref/platform supplies the implementation so the
// process engine and the request tier share one expression language.
type Guard interface {
	// Eval evaluates the guard against the scope. An empty guard is nil, and a nil
	// Guard is always true.
	Eval(scope Scope) (bool, error)
	// Source returns the guard's text, for error messages and introspection.
	Source() string
}

// Shaper transforms a payload in transit. ref/platform supplies a DataSpec-backed
// implementation.
type Shaper interface {
	// Shape returns the reshaped payload, or ErrFiltered when a filter rejected
	// it — which for an edge means the edge does not traverse.
	Shape(value any, scope Scope) (any, error)
}

// Valuer evaluates an expression to a value, for threshold values, correlation
// keys and rate-limit keys.
type Valuer interface {
	Value(scope Scope) (any, error)
	Source() string
}

// Scope is what a guard, shaper or valuer sees. It is a plain map so this package
// does not need to know the expression implementation.
//
// The documented keys are: run, step, input, result, principal, tenant, now, and
// every step's result under results.<step>.
type Scope map[string]any

// Step is one durable step.
type Step struct {
	Name        string
	Description string
	// Intent is the intent this step runs. Exactly one of Intent, Process or Task
	// is set, and the compiler enforces that.
	Intent string
	// Process runs a child process as a sub-run, parking the parent until it ends.
	Process string
	// Type is catalog metadata.
	Type string

	// Compensate names the intent that undoes this step's committed effect.
	Compensate string

	Retry   *RetryPolicy
	Timeout time.Duration

	// Terminal marks a step after which the run completes successfully even if it
	// has outgoing edges that do not traverse.
	Terminal bool

	SkipWhen   Guard
	SkipResult map[string]any

	// Task, when set, makes this step a human task: the run parks and a work item
	// appears until somebody completes it.
	Task *TaskDefinition

	InputShaper  Shaper
	OutputShaper Shaper

	// Lock serialises this step against itself across runs.
	Lock     string
	LockKey  Valuer
	LockTTL  time.Duration
	LockWait time.Duration

	// RateLimit throttles this step. Hitting the limit parks the run rather than
	// failing it, because a rate limit is backpressure, not an error.
	RateLimit    string
	RateLimitKey Valuer
	Limit        int
	Window       time.Duration

	CircuitBreaker string
}

// TaskDefinition is the human-task configuration of a step.
type TaskDefinition struct {
	Title        Renderer
	Instructions Renderer
	Assignee     Renderer
	Role         string
	Queue        string
	Skills       []string
	// Strategy is "queue" (anybody addressed may claim), "round_robin",
	// "least_loaded" or "direct".
	Strategy         string
	FormSchema       string
	Actions          []string
	Priority         int
	Due              time.Duration
	Reminder         time.Duration
	Escalate         string
	AllowReassign    bool
	AllowSelfAssign  bool
	ForbidPrincipals []Valuer
}

// Renderer produces a string from a scope, for titles and instructions.
type Renderer interface {
	Render(scope Scope) (string, error)
}

// Edge is one compiled transition.
type Edge struct {
	Name string
	Type EdgeType

	From    string
	To      string
	Sources []string
	Targets []string

	// Guard must pass for the edge to traverse. Every edge type evaluates it,
	// without exception, so a fan-out or a manual gate cannot bypass a condition
	// that a simple edge would honour.
	Guard Guard

	Strategy string
	Quorum   int

	MaxConcurrency  int
	FailFast        bool
	ContinueOnError bool
	CancelLosers    bool

	Timeout   time.Duration
	Attempts  int
	OnTimeout string

	Event       string
	Correlation Valuer

	Weight   float64
	Priority int

	RateLimit string
	Limit     int
	Window    time.Duration

	ItemsPath   string
	BatchSize   int
	TargetsPath string

	Thresholds []Threshold

	Escalate string
	Notify   string

	Shaper Shaper
}

// Threshold is one numeric band of a threshold edge.
type Threshold struct {
	Name   string
	Min    *float64
	Max    *float64
	Value  Valuer
	Target string
	Reason string
	Data   map[string]any
}

// Contains reports whether a value falls in this band. Min is inclusive and Max
// exclusive, so adjacent bands tile without overlap or gaps — which is the
// property that makes a set of thresholds total.
func (t Threshold) Contains(value float64) bool {
	if t.Min != nil && value < *t.Min {
		return false
	}
	if t.Max != nil && value >= *t.Max {
		return false
	}
	return true
}

// RetryPolicy is a step's retry configuration. Unlike a request-scoped retry,
// these attempts are persisted and survive a restart.
type RetryPolicy struct {
	MaxAttempts  int
	Strategy     string
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Jitter       bool
	// Retriable decides whether a given failure is worth another attempt.
	// ref/platform supplies it so the categories match the request tier's.
	Retriable func(error) bool
}

// SLA declares when a run is late and what happens then.
type SLA struct {
	Target   time.Duration
	Breach   time.Duration
	OnBreach string
	Notify   string
	Escalate string
}

// Definition is one compiled process.
type Definition struct {
	Name        string
	Description string
	Version     int
	Start       string

	// MigrationPolicy is "pin" (default), "migrate" or "fail".
	MigrationPolicy string

	Timeout   time.Duration
	MaxSteps  int
	MaxVisits int
	Retention time.Duration

	// Concurrency caps simultaneously advancing runs of this process across the
	// deployment. Zero is unbounded.
	Concurrency int

	InputSchema string
	// Idempotency is the input path whose value deduplicates run creation.
	Idempotency string

	Retry *RetryPolicy
	SLA   *SLA

	InputShaper  Shaper
	OutputShaper Shaper

	Steps map[string]*Step
	Edges []*Edge

	// outgoing and incoming are adjacency indexes built by Compile, so resolving
	// a step's edges is a map lookup rather than a scan of every edge.
	outgoing map[string][]*Edge
	incoming map[string][]*Edge
	byName   map[string]*Edge
}

// Step returns a step by name.
func (d *Definition) Step(name string) (*Step, bool) {
	step, ok := d.Steps[name]
	return step, ok
}

// Outgoing returns the edges leaving a step, in declaration order. Order matters:
// it decides which branch wins when several conditions hold, which makes the
// behaviour of a given configuration deterministic.
func (d *Definition) Outgoing(step string) []*Edge { return d.outgoing[step] }

// Incoming returns the edges arriving at a step.
func (d *Definition) Incoming(step string) []*Edge { return d.incoming[step] }

// Edge returns an edge by name.
func (d *Definition) Edge(name string) (*Edge, bool) {
	edge, ok := d.byName[name]
	return edge, ok
}

// Compile validates a definition and builds its indexes.
//
// The validation is where a no-code platform earns trust. Every one of these
// checks corresponds to a failure that would otherwise appear as a stuck run, a
// silently skipped step or a rollback that did not roll back — and every one of
// them fails the deployment instead.
func Compile(d *Definition) error {
	if d == nil {
		return fmt.Errorf("ref/process: nil definition")
	}
	if strings.TrimSpace(d.Name) == "" {
		return fmt.Errorf("ref/process: a process needs a name")
	}
	if len(d.Steps) == 0 {
		return fmt.Errorf("process %q: a process needs at least one step", d.Name)
	}
	if d.Start == "" {
		return fmt.Errorf("process %q: a process needs a start step — an entry point is not optional", d.Name)
	}
	if _, ok := d.Steps[d.Start]; !ok {
		return fmt.Errorf("process %q: start step %q is not declared", d.Name, d.Start)
	}
	if d.Version <= 0 {
		d.Version = 1
	}
	switch d.MigrationPolicy {
	case "", "pin":
		d.MigrationPolicy = "pin"
	case "migrate", "fail":
	default:
		return fmt.Errorf("process %q: migration_policy must be pin, migrate or fail", d.Name)
	}
	if d.MaxSteps <= 0 {
		// A bound is not optional: a loop_until whose condition never holds, or a
		// pair of steps pointing at each other, would otherwise advance forever.
		d.MaxSteps = 1000
	}
	if d.MaxVisits <= 0 {
		d.MaxVisits = 100
	}

	d.outgoing = make(map[string][]*Edge, len(d.Steps))
	d.incoming = make(map[string][]*Edge, len(d.Steps))
	d.byName = make(map[string]*Edge, len(d.Edges))

	for name, step := range d.Steps {
		if step.Name == "" {
			step.Name = name
		}
		bodies := 0
		for _, set := range []bool{step.Intent != "", step.Process != "", step.Task != nil} {
			if set {
				bodies++
			}
		}
		if bodies == 0 {
			return fmt.Errorf("process %q step %q: a step needs an intent, a process or a task block", d.Name, name)
		}
		if step.Process != "" && step.Task != nil {
			return fmt.Errorf("process %q step %q: a step cannot be both a child process and a human task", d.Name, name)
		}
		if step.Task != nil {
			if step.Task.Role == "" && step.Task.Queue == "" && step.Task.Assignee == nil {
				return fmt.Errorf("process %q step %q: a task needs an assignee, a role or a queue — otherwise nobody can ever see it", d.Name, name)
			}
			if step.Task.Strategy == "" {
				step.Task.Strategy = "queue"
			}
			if !slices.Contains([]string{"queue", "round_robin", "least_loaded", "direct"}, step.Task.Strategy) {
				return fmt.Errorf("process %q step %q: task strategy must be queue, round_robin, least_loaded or direct", d.Name, name)
			}
		}
		if step.Lock != "" && step.LockKey == nil {
			return fmt.Errorf("process %q step %q: a lock needs a lock_key", d.Name, name)
		}
		if step.RateLimit != "" && (step.Limit <= 0 || step.Window <= 0) {
			return fmt.Errorf("process %q step %q: a rate limit needs a positive limit and window", d.Name, name)
		}
	}

	seen := make(map[string]bool, len(d.Edges))
	for index, edge := range d.Edges {
		if edge.Name == "" {
			edge.Name = fmt.Sprintf("%s_edge_%d", d.Name, index)
		}
		if seen[edge.Name] {
			return fmt.Errorf("process %q: duplicate edge %q", d.Name, edge.Name)
		}
		seen[edge.Name] = true
		if edge.Type == "" {
			edge.Type = EdgeSimple
		}
		if !KnownEdgeType(string(edge.Type)) {
			return fmt.Errorf("process %q edge %q: unknown edge type %q", d.Name, edge.Name, edge.Type)
		}
		if err := validateEdge(d, edge); err != nil {
			return err
		}
		d.byName[edge.Name] = edge

		for _, source := range edge.allSources() {
			d.outgoing[source] = append(d.outgoing[source], edge)
		}
		for _, target := range edge.allTargets() {
			d.incoming[target] = append(d.incoming[target], edge)
		}
	}

	// A step with no outgoing success edge must be able to end the run, or the
	// run reaches it and stops without completing — which looks exactly like a
	// bug in the engine and is not.
	for name, step := range d.Steps {
		if step.Terminal {
			continue
		}
		hasSuccessPath := slices.ContainsFunc(d.outgoing[name], func(edge *Edge) bool {
			return !edge.Type.ErrorPath() && edge.Type != EdgeCompensate
		})
		if !hasSuccessPath {
			return fmt.Errorf("process %q step %q has no outgoing success edge and is not marked terminal, so a run reaching it would stop without completing",
				d.Name, name)
		}
	}

	// Every step must be reachable from the start. An unreachable step is dead
	// configuration, and saying so is more useful than letting it sit there.
	reachable := map[string]bool{d.Start: true}
	frontier := []string{d.Start}
	for len(frontier) > 0 {
		current := frontier[0]
		frontier = frontier[1:]
		for _, edge := range d.outgoing[current] {
			// Every way an edge can hand control to a step counts: its ordinary
			// targets, the step it goes to when a deadline expires, and the bands of
			// a threshold edge. Counting only the first would report a perfectly
			// reachable timeout handler as dead configuration.
			targets := edge.allTargets()
			if edge.OnTimeout != "" {
				targets = append(targets, edge.OnTimeout)
			}
			for _, band := range edge.Thresholds {
				if band.Target != "" {
					targets = append(targets, band.Target)
				}
			}
			for _, target := range targets {
				if !reachable[target] {
					reachable[target] = true
					frontier = append(frontier, target)
				}
			}
		}
	}
	unreachable := make([]string, 0)
	for name := range d.Steps {
		if !reachable[name] {
			unreachable = append(unreachable, name)
		}
	}
	if len(unreachable) > 0 {
		slices.Sort(unreachable)
		return fmt.Errorf("process %q: these steps cannot be reached from %q: %s",
			d.Name, d.Start, strings.Join(unreachable, ", "))
	}

	if d.SLA != nil {
		if d.SLA.OnBreach == "" {
			d.SLA.OnBreach = "notify"
		}
		if !slices.Contains([]string{"notify", "escalate", "cancel", "fail"}, d.SLA.OnBreach) {
			return fmt.Errorf("process %q: sla on_breach must be notify, escalate, cancel or fail", d.Name)
		}
		if d.SLA.Breach > 0 && d.SLA.Target > d.SLA.Breach {
			return fmt.Errorf("process %q: the sla target must not be later than its breach", d.Name)
		}
	}
	return nil
}

func validateEdge(d *Definition, edge *Edge) error {
	label := fmt.Sprintf("process %q edge %q", d.Name, edge.Name)

	sources := edge.allSources()
	targets := edge.allTargets()
	if len(sources) == 0 {
		return fmt.Errorf("%s: needs from or sources", label)
	}
	for _, name := range sources {
		if _, ok := d.Steps[name]; !ok {
			return fmt.Errorf("%s: source step %q is not declared", label, name)
		}
	}
	// A cancel edge deliberately has no target: it ends the run.
	if len(targets) == 0 && edge.Type != EdgeCancel {
		return fmt.Errorf("%s: needs to or targets", label)
	}
	for _, name := range targets {
		if _, ok := d.Steps[name]; !ok {
			return fmt.Errorf("%s: target step %q is not declared", label, name)
		}
	}
	if edge.OnTimeout != "" {
		if _, ok := d.Steps[edge.OnTimeout]; !ok {
			return fmt.Errorf("%s: on_timeout step %q is not declared", label, edge.OnTimeout)
		}
	}

	switch edge.Type {
	case EdgeFanIn, EdgeJoin, EdgeQuorum:
		if len(edge.Sources) < 2 {
			return fmt.Errorf("%s: a %s edge needs at least two sources", label, edge.Type)
		}
		if edge.Strategy == "" {
			edge.Strategy = "all"
		}
		if !slices.Contains([]string{"all", "any", "quorum", "partial_success", "first_failure"}, edge.Strategy) {
			return fmt.Errorf("%s: strategy must be all, any, quorum, partial_success or first_failure", label)
		}
		if edge.Type == EdgeQuorum && edge.Strategy == "all" {
			edge.Strategy = "quorum"
		}
		if edge.Strategy == "quorum" && edge.Quorum > len(edge.Sources) {
			return fmt.Errorf("%s: a quorum of %d cannot be met by %d sources", label, edge.Quorum, len(edge.Sources))
		}

	case EdgeParallel, EdgeFanOut, EdgeRace, EdgeConditionalFork:
		if len(targets) < 2 {
			return fmt.Errorf("%s: a %s edge needs at least two targets", label, edge.Type)
		}

	case EdgeDynamicFanOut:
		if edge.TargetsPath == "" && len(targets) == 0 {
			return fmt.Errorf("%s: a dynamic_fanout edge needs targets_path or static targets as a fallback", label)
		}

	case EdgeWaitEvent:
		if edge.Event == "" {
			return fmt.Errorf("%s: a wait_event edge needs an event name", label)
		}
		if edge.Correlation == nil {
			// Without a correlation key, any event of that name would wake every
			// run waiting for it. That is almost never what an author means, and
			// when it is, they can correlate on a constant.
			return fmt.Errorf("%s: a wait_event edge needs a correlation expression, so the event reaches the run it belongs to", label)
		}

	case EdgeDelayed:
		if edge.Timeout <= 0 {
			return fmt.Errorf("%s: a delayed edge needs a positive timeout", label)
		}

	case EdgeTimeout:
		if edge.Timeout <= 0 {
			return fmt.Errorf("%s: a timeout edge needs a positive timeout", label)
		}

	case EdgeRetry:
		if edge.Attempts <= 0 {
			edge.Attempts = 3
		}
		if edge.Timeout <= 0 {
			edge.Timeout = 500 * time.Millisecond
		}

	case EdgeRateLimited:
		if edge.RateLimit == "" || edge.Limit <= 0 || edge.Window <= 0 {
			return fmt.Errorf("%s: a rate_limited edge needs rate_limit, limit and window", label)
		}

	case EdgeIterator, EdgeBatchIterator:
		if len(targets) != 1 {
			return fmt.Errorf("%s: an %s edge needs exactly one target", label, edge.Type)
		}
		if edge.Type == EdgeBatchIterator && edge.BatchSize <= 0 {
			edge.BatchSize = 25
		}

	case EdgeLoopUntil:
		if edge.Guard == nil {
			return fmt.Errorf("%s: a loop_until edge needs a condition, or it would never stop", label)
		}
		if edge.MaxConcurrency <= 0 {
			edge.MaxConcurrency = 10
		}

	case EdgeThreshold:
		if len(edge.Thresholds) == 0 {
			return fmt.Errorf("%s: a threshold edge needs at least one threshold block", label)
		}
		for i, band := range edge.Thresholds {
			if band.Target == "" {
				return fmt.Errorf("%s: threshold[%d] needs a target step", label, i)
			}
			if _, ok := d.Steps[band.Target]; !ok {
				return fmt.Errorf("%s: threshold[%d] target %q is not declared", label, i, band.Target)
			}
			if band.Min != nil && band.Max != nil && *band.Min >= *band.Max {
				return fmt.Errorf("%s: threshold[%d] has min >= max", label, i)
			}
		}

	case EdgeWeighted:
		if edge.Weight <= 0 {
			edge.Weight = 1
		}

	case EdgeEscalation:
		if edge.Timeout <= 0 {
			return fmt.Errorf("%s: an escalation edge needs a positive timeout", label)
		}

	case EdgeCompensate:
		if step, ok := d.Steps[edge.From]; ok && step.Compensate == "" {
			return fmt.Errorf("%s: step %q has no compensate intent, so there is nothing for this edge to run", label, edge.From)
		}
	}
	return nil
}

func (e *Edge) allSources() []string {
	if len(e.Sources) > 0 {
		return e.Sources
	}
	if e.From != "" {
		return []string{e.From}
	}
	return nil
}

func (e *Edge) allTargets() []string {
	if len(e.Targets) > 0 {
		return e.Targets
	}
	if e.To != "" {
		return []string{e.To}
	}
	return nil
}
