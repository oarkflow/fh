package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/platform/spi"
	"github.com/oarkflow/fh/ref/process"
)

// Compiling `process` blocks into the durable engine.
//
// This file is the bridge, and the bridge is deliberately thin. ref/process owns
// the state machine and knows nothing about BCL; this package owns the language and
// knows nothing about row shapes. What crosses between them is four small
// interfaces — a guard, a shaper, a valuer and a renderer — each implemented here
// over the platform's own expression engine, so a process edge's condition and an
// intent node's condition are the same language evaluated the same way.
//
// The other thing that crosses is the step runner: ref/process asks "run this
// step", and the answer is "dispatch this intent". That single adapter is what makes
// the entire action catalog — every database, cache, queue, service, decision and
// flow action — available inside a durable process without a second implementation.

// ---------------------------------------------------------------------------
// Expression bridges
// ---------------------------------------------------------------------------

// processGuard adapts a compiled expression to process.Guard.
type processGuard struct{ expr *Expression }

// Eval implements process.Guard.
func (g processGuard) Eval(scope process.Scope) (bool, error) {
	return g.expr.Bool(Env(scope))
}

// Source implements process.Guard.
func (g processGuard) Source() string { return g.expr.Raw() }

func compileGuard(what, raw string) (process.Guard, error) {
	expr, err := CompileExpr(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	if expr == nil {
		return nil, nil
	}
	return processGuard{expr: expr}, nil
}

// processValuer adapts a compiled expression to process.Valuer.
type processValuer struct{ expr *Expression }

// Value implements process.Valuer.
func (v processValuer) Value(scope process.Scope) (any, error) { return v.expr.Eval(Env(scope)) }

// Source implements process.Valuer.
func (v processValuer) Source() string { return v.expr.Raw() }

func compileValuer(what, raw string) (process.Valuer, error) {
	expr, err := CompileExpr(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	if expr == nil {
		return nil, nil
	}
	return processValuer{expr: expr}, nil
}

// processRenderer adapts a compiled template to process.Renderer.
type processRenderer struct{ tmpl *Template }

// Render implements process.Renderer.
func (r processRenderer) Render(scope process.Scope) (string, error) {
	return r.tmpl.Render(Env(scope))
}

func compileRenderer(what, raw string) (process.Renderer, error) {
	if raw == "" {
		return nil, nil
	}
	tmpl, err := CompileTemplate(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	return processRenderer{tmpl: tmpl}, nil
}

// processShaper adapts a data pipeline to process.Shaper, translating the
// platform's filtered sentinel into the engine's.
type processShaper struct{ pipeline *DataPipeline }

// Shape implements process.Shaper.
func (s processShaper) Shape(value any, scope process.Scope) (any, error) {
	shaped, err := s.pipeline.Apply(value, Env(scope))
	if errors.Is(err, ErrDataFiltered) {
		return nil, process.ErrFiltered
	}
	return shaped, err
}

func compileShaper(what string, spec *DataSpec, schemas map[string]*CompiledSchema) (process.Shaper, error) {
	pipeline, err := compileDataSpec(what, spec, schemas)
	if err != nil {
		return nil, err
	}
	if pipeline == nil {
		return nil, nil
	}
	return processShaper{pipeline: pipeline}, nil
}

// ---------------------------------------------------------------------------
// Definition compilation
// ---------------------------------------------------------------------------

// compileProcesses turns every `process` block into a registered definition.
func (p *Platform) compileProcesses(doc Document) error {
	if len(doc.Processes) == 0 {
		return nil
	}
	for _, spec := range doc.Processes {
		engine, err := p.engineFor(spec)
		if err != nil {
			return err
		}
		definition, err := p.compileProcess(doc, spec)
		if err != nil {
			return err
		}
		if err := engine.Register(definition); err != nil {
			return err
		}
		p.processes[spec.Name] = engine
	}
	return nil
}

// engineFor resolves (and lazily builds) the engine for a process's store.
//
// One engine per store rather than per process: the engines share timers, leases and
// the advance queue, and running two over the same tables would mean two tickers
// competing for the same due timers.
func (p *Platform) engineFor(spec ProcessSpec) (*process.Engine, error) {
	if spec.Store == "" {
		return nil, fmt.Errorf("ref/platform: process %q needs a store — a durable process without persisted state is not durable", spec.Name)
	}
	if engine, ok := p.engines[spec.Store]; ok {
		return engine, nil
	}

	resource, ok := p.resources[spec.Store]
	if !ok {
		return nil, fmt.Errorf("ref/platform: process %q names undeclared store %q", spec.Name, spec.Store)
	}
	store, ok := resource.(process.Store)
	if !ok {
		return nil, fmt.Errorf("ref/platform: resource %q is not a process store. Use store.sql or store.memory", spec.Store)
	}

	options := process.Options{
		Store:    store,
		Runner:   &intentStepRunner{platform: p},
		Owner:    p.replicaID,
		Notifier: &processNotifier{platform: p},
	}
	if spec.Queue != "" {
		queue, ok := p.resources[spec.Queue]
		if !ok {
			return nil, fmt.Errorf("ref/platform: process %q names undeclared queue %q", spec.Name, spec.Queue)
		}
		enqueuer, err := newQueueEnqueuer(spec.Store, queue)
		if err != nil {
			return nil, fmt.Errorf("ref/platform: process %q: %w", spec.Name, err)
		}
		options.Enqueuer = enqueuer
	}
	// A lock or limiter is resolved from whatever the application declared; a
	// process that uses neither gets neither.
	for _, resource := range p.resources {
		if options.Locker == nil {
			if locker, ok := resource.(spi.Locker); ok {
				options.Locker = locker
			}
		}
		if options.Limiter == nil {
			if limiter, ok := resource.(spi.RateLimiter); ok {
				options.Limiter = limiter
			}
		}
	}

	engine, err := process.New(options)
	if err != nil {
		return nil, err
	}
	if err := engine.Migrate(context.Background()); err != nil {
		return nil, fmt.Errorf("ref/platform: prepare process store %q: %w", spec.Store, err)
	}
	p.engines[spec.Store] = engine
	return engine, nil
}

func (p *Platform) compileProcess(doc Document, spec ProcessSpec) (*process.Definition, error) {
	label := "process " + spec.Name
	timeout, err := durationField(label, "timeout", spec.Timeout, 0)
	if err != nil {
		return nil, err
	}
	retention, err := durationField(label, "retention", spec.Retention, 0)
	if err != nil {
		return nil, err
	}
	definition := &process.Definition{
		Name:            spec.Name,
		Description:     spec.Description,
		Version:         spec.Version,
		Start:           spec.Start,
		MigrationPolicy: spec.MigrationPolicy,
		Timeout:         timeout,
		MaxSteps:        spec.MaxSteps,
		MaxVisits:       spec.MaxVisits,
		Retention:       retention,
		Concurrency:     spec.Concurrency,
		InputSchema:     spec.InputSchema,
		Idempotency:     spec.Idempotency,
		Steps:           make(map[string]*process.Step, len(spec.Steps)),
	}

	if definition.Retry, err = p.compileProcessRetry(label, spec.Retry); err != nil {
		return nil, err
	}
	if definition.InputShaper, err = compileShaper(label+" input_data", spec.InputData, p.schemas); err != nil {
		return nil, err
	}
	if definition.OutputShaper, err = compileShaper(label+" output_data", spec.OutputData, p.schemas); err != nil {
		return nil, err
	}
	if spec.InputSchema != "" {
		if _, ok := p.schemas[spec.InputSchema]; !ok {
			return nil, fmt.Errorf("%s: input_schema %q is not declared", label, spec.InputSchema)
		}
	}
	if spec.SLA != nil {
		slaTarget, err := durationField(label, "sla target", spec.SLA.Target, 0)
		if err != nil {
			return nil, err
		}
		slaBreach, err := durationField(label, "sla breach", spec.SLA.Breach, 0)
		if err != nil {
			return nil, err
		}
		definition.SLA = &process.SLA{
			Target:   slaTarget,
			Breach:   slaBreach,
			OnBreach: spec.SLA.OnBreach,
			Notify:   spec.SLA.Notify,
			Escalate: spec.SLA.Escalate,
		}
		if spec.SLA.Notify != "" {
			if _, ok := p.resources[spec.SLA.Notify]; !ok {
				return nil, fmt.Errorf("%s: sla notify names undeclared resource %q", label, spec.SLA.Notify)
			}
		}
	}

	intents := make(map[string]bool, len(doc.Intents))
	for _, item := range doc.Intents {
		intents[item.Name] = true
	}
	processes := make(map[string]bool, len(doc.Processes))
	for _, item := range doc.Processes {
		processes[item.Name] = true
	}

	for i := range spec.Steps {
		stepSpec := spec.Steps[i]
		step, err := p.compileStep(label, stepSpec, intents, processes)
		if err != nil {
			return nil, err
		}
		if _, exists := definition.Steps[step.Name]; exists {
			return nil, fmt.Errorf("%s: duplicate step %q", label, step.Name)
		}
		definition.Steps[step.Name] = step
	}

	for i := range spec.Edges {
		edge, err := p.compileEdge(label, spec.Edges[i])
		if err != nil {
			return nil, err
		}
		definition.Edges = append(definition.Edges, edge)
	}
	return definition, nil
}

func (p *Platform) compileStep(label string, spec StepSpec, intents, processes map[string]bool) (*process.Step, error) {
	what := fmt.Sprintf("%s step %q", label, spec.Name)
	if spec.Name == "" {
		return nil, fmt.Errorf("%s: every step needs a name", label)
	}
	timeout, err := durationField(what, "timeout", spec.Timeout, 0)
	if err != nil {
		return nil, err
	}
	lockTTL, err := durationField(what, "lock_ttl", spec.LockTTL, 30*time.Second)
	if err != nil {
		return nil, err
	}
	lockWait, err := durationField(what, "lock_wait", spec.LockWait, 0)
	if err != nil {
		return nil, err
	}
	window, err := durationField(what, "window", spec.Window, 0)
	if err != nil {
		return nil, err
	}
	step := &process.Step{
		Name:           spec.Name,
		Description:    spec.Description,
		Intent:         spec.Intent,
		Process:        spec.Process,
		Type:           spec.Family,
		Compensate:     spec.Compensate,
		Timeout:        timeout,
		Terminal:       spec.Terminal,
		SkipResult:     spec.SkipResult,
		Lock:           spec.Lock,
		LockTTL:        lockTTL,
		LockWait:       lockWait,
		RateLimit:      spec.RateLimit,
		Limit:          spec.Limit,
		Window:         window,
		CircuitBreaker: spec.CircuitBreaker,
	}
	if spec.Intent != "" && !intents[spec.Intent] {
		return nil, fmt.Errorf("%s: intent %q is not declared in this application", what, spec.Intent)
	}
	if spec.Compensate != "" && !intents[spec.Compensate] {
		return nil, fmt.Errorf("%s: compensate intent %q is not declared in this application", what, spec.Compensate)
	}
	if spec.Process != "" && !processes[spec.Process] {
		return nil, fmt.Errorf("%s: child process %q is not declared in this application", what, spec.Process)
	}
	for _, pair := range []struct {
		name  string
		value string
	}{{"lock", spec.Lock}, {"rate_limit", spec.RateLimit}, {"circuit_breaker", spec.CircuitBreaker}} {
		if pair.value == "" {
			continue
		}
		if _, ok := p.resources[pair.value]; !ok {
			return nil, fmt.Errorf("%s: %s names undeclared resource %q", what, pair.name, pair.value)
		}
	}

	if step.SkipWhen, err = compileGuard(what+" skip_when", spec.SkipWhen); err != nil {
		return nil, err
	}
	if step.Retry, err = p.compileProcessRetry(what, spec.Retry); err != nil {
		return nil, err
	}
	if step.InputShaper, err = compileShaper(what+" input_data", spec.InputData, p.schemas); err != nil {
		return nil, err
	}
	if step.OutputShaper, err = compileShaper(what+" output_data", spec.OutputData, p.schemas); err != nil {
		return nil, err
	}
	if step.LockKey, err = compileValuer(what+" lock_key", spec.LockKey); err != nil {
		return nil, err
	}
	if step.RateLimitKey, err = compileValuer(what+" rate_limit_key", spec.RateLimitKey); err != nil {
		return nil, err
	}
	if spec.Authz != nil {
		// A step's authz gate is compiled here so a broken one fails the deployment;
		// it is enforced by the step runner, which has the principal.
		if _, err := compileAuthz(p.buildContext(), what, spec.Authz); err != nil {
			return nil, err
		}
		p.stepAuthz[label+"/"+spec.Name] = spec.Authz
	}

	if spec.Task != nil {
		task, err := p.compileTask(what, spec.Task)
		if err != nil {
			return nil, err
		}
		step.Task = task
	}
	return step, nil
}

func (p *Platform) compileTask(what string, spec *TaskSpec) (*process.TaskDefinition, error) {
	due, err := durationField(what, "task due", spec.Due, 0)
	if err != nil {
		return nil, err
	}
	reminder, err := durationField(what, "task reminder", spec.Reminder, 0)
	if err != nil {
		return nil, err
	}
	definition := &process.TaskDefinition{
		Role:          spec.Role,
		Queue:         spec.Queue,
		Skills:        spec.Skills,
		Strategy:      spec.Strategy,
		FormSchema:    spec.FormSchema,
		Actions:       spec.Actions,
		Priority:      spec.Priority,
		Due:           due,
		Reminder:      reminder,
		Escalate:      spec.Escalate,
		AllowReassign: spec.AllowReassign,
		// Self-assignment defaults to allowed: the common case is a queue anybody
		// addressed may claim. A segregation-of-duties control turns it off
		// explicitly.
		AllowSelfAssign: spec.AllowSelfAssign == nil || *spec.AllowSelfAssign,
	}
	if spec.FormSchema != "" {
		if _, ok := p.schemas[spec.FormSchema]; !ok {
			return nil, fmt.Errorf("%s: task form_schema %q is not declared", what, spec.FormSchema)
		}
	}
	if definition.Title, err = compileRenderer(what+" task title", spec.Title); err != nil {
		return nil, err
	}
	if definition.Instructions, err = compileRenderer(what+" task instructions", spec.Instructions); err != nil {
		return nil, err
	}
	if definition.Assignee, err = compileRenderer(what+" task assignee", spec.Assignee); err != nil {
		return nil, err
	}
	for i, path := range spec.ForbidPrincipals {
		valuer, err := compileValuer(fmt.Sprintf("%s task forbid_principals[%d]", what, i), path)
		if err != nil {
			return nil, err
		}
		if valuer != nil {
			definition.ForbidPrincipals = append(definition.ForbidPrincipals, valuer)
		}
	}
	return definition, nil
}

func (p *Platform) compileEdge(label string, spec EdgeSpec) (*process.Edge, error) {
	what := fmt.Sprintf("%s edge %q", label, spec.Name)
	timeout, err := durationField(what, "timeout", spec.Timeout, 0)
	if err != nil {
		return nil, err
	}
	window, err := durationField(what, "window", spec.Window, 0)
	if err != nil {
		return nil, err
	}
	edge := &process.Edge{
		Name:            spec.Name,
		Type:            process.EdgeType(strings.ToLower(strings.TrimSpace(spec.Kind))),
		From:            spec.From,
		To:              spec.To,
		Sources:         spec.Sources,
		Targets:         spec.Targets,
		Strategy:        spec.Strategy,
		Quorum:          spec.Quorum,
		MaxConcurrency:  spec.MaxConcurrency,
		FailFast:        spec.FailFast,
		ContinueOnError: spec.ContinueOnError,
		CancelLosers:    spec.CancelLosers,
		Timeout:         timeout,
		Attempts:        spec.Attempts,
		OnTimeout:       spec.OnTimeout,
		Event:           spec.Event,
		Weight:          spec.Weight,
		Priority:        spec.Priority,
		RateLimit:       spec.RateLimit,
		Limit:           spec.Limit,
		Window:          window,
		ItemsPath:       spec.ItemsPath,
		BatchSize:       spec.BatchSize,
		TargetsPath:     spec.TargetsPath,
		Escalate:        spec.Escalate,
		Notify:          spec.Notify,
	}
	if edge.Type == "" {
		edge.Type = process.EdgeSimple
	}
	if !process.KnownEdgeType(string(edge.Type)) {
		return nil, fmt.Errorf("%s: unknown edge kind %q", what, spec.Kind)
	}
	if edge.Guard, err = compileGuard(what+" condition", spec.Condition); err != nil {
		return nil, err
	}
	if edge.Correlation, err = compileValuer(what+" correlation", spec.Correlation); err != nil {
		return nil, err
	}

	// The `extract` shorthand is folded into the data block's own extract stage, so
	// both spellings go through one pipeline.
	dataSpec := spec.Data
	if len(spec.Extract) > 0 {
		if dataSpec == nil {
			dataSpec = &DataSpec{}
		}
		if dataSpec.Extract == nil {
			dataSpec.Extract = map[string]string{}
		}
		for target, source := range spec.Extract {
			if _, exists := dataSpec.Extract[target]; !exists {
				dataSpec.Extract[target] = source
			}
		}
	}
	if edge.Shaper, err = compileShaper(what+" data", dataSpec, p.schemas); err != nil {
		return nil, err
	}

	for i, band := range spec.Thresholds {
		value, err := compileValuer(fmt.Sprintf("%s threshold[%d] value", what, i), band.Value)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, fmt.Errorf("%s threshold[%d]: a value expression is required", what, i)
		}
		edge.Thresholds = append(edge.Thresholds, process.Threshold{
			Name:   band.Name,
			Min:    band.Min,
			Max:    band.Max,
			Value:  value,
			Target: band.Target,
			Reason: band.Reason,
			Data:   band.Data,
		})
	}
	if spec.Notify != "" {
		if _, ok := p.resources[spec.Notify]; !ok {
			return nil, fmt.Errorf("%s: notify names undeclared resource %q", what, spec.Notify)
		}
	}
	if spec.RateLimit != "" {
		if _, ok := p.resources[spec.RateLimit]; !ok {
			return nil, fmt.Errorf("%s: rate_limit names undeclared resource %q", what, spec.RateLimit)
		}
	}
	return edge, nil
}

// compileProcessRetry builds a persisted retry policy, reusing the request tier's
// failure classification so a retry means the same thing in both places.
func (p *Platform) compileProcessRetry(what string, spec *RetrySpec) (*process.RetryPolicy, error) {
	if spec == nil {
		return nil, nil
	}
	policy, err := compileRetrySpec(spec)
	if err != nil {
		return nil, fmt.Errorf("%s retry: %w", what, err)
	}
	return &process.RetryPolicy{
		MaxAttempts:  policy.attempts,
		Strategy:     policy.strategy,
		InitialDelay: policy.initialDelay,
		MaxDelay:     policy.maxDelay,
		Jitter:       policy.jitter,
		Retriable:    policy.shouldRetry,
	}, nil
}

// ---------------------------------------------------------------------------
// Step runner
// ---------------------------------------------------------------------------

// intentStepRunner executes a step by dispatching its intent.
//
// This is the single adapter that makes a process step as capable as a request: the
// step's body is an ordinary intent, compiled and proven like any other, with the
// whole action catalog available to it.
type intentStepRunner struct{ platform *Platform }

// RunStep implements process.StepRunner.
func (r *intentStepRunner) RunStep(ctx context.Context, call process.StepCall) (any, error) {
	if call.Intent == "" {
		return nil, fmt.Errorf("step %q has no intent to run", call.Step.Name)
	}
	// The run's identity travels with the dispatch, so a tenant-scoped query or an
	// authorization gate inside the step sees the caller who started the run rather
	// than nothing.
	inv := &invocation.Invocation{
		ID:     invocation.ID(newPrefixedID("step")),
		Intent: invocation.IntentID(call.Intent),
		Metadata: invocation.NewQueueMeta("process."+call.Run.Process, 0, call.Run.ID, map[string]string{
			"run_id":       call.Run.ID,
			"step":         call.Step.Name,
			"principal_id": call.Run.PrincipalID,
			"tenant_id":    call.Run.TenantID,
			"attempt":      fmt.Sprint(max(call.Frame.Attempt, 1)),
		}),
		Transport: invocation.Transport{Protocol: "process"},
		Received:  time.Now(),
	}
	encoded, err := json.Marshal(call.Input)
	if err != nil {
		return nil, fmt.Errorf("step %q input cannot be serialised: %w", call.Step.Name, err)
	}
	inv.Input = invocation.NewInput(encoded, "application/json")

	ctx = withProcessIdentity(ctx, call.Run.PrincipalID, call.Run.TenantID)
	result, err := r.platform.Engine.Dispatch(ctx, inv)
	if err != nil {
		return nil, err
	}
	return result.Value, nil
}

// processIdentity carries a run's identity into the intent dispatch, where the
// route layer would normally have supplied it.
type processIdentityKey struct{}

type processIdentity struct {
	principal string
	tenant    string
}

func withProcessIdentity(ctx context.Context, principal, tenant string) context.Context {
	if principal == "" && tenant == "" {
		return ctx
	}
	return context.WithValue(ctx, processIdentityKey{}, processIdentity{principal: principal, tenant: tenant})
}

func processIdentityFrom(ctx context.Context) (processIdentity, bool) {
	identity, ok := ctx.Value(processIdentityKey{}).(processIdentity)
	return identity, ok
}

// ---------------------------------------------------------------------------
// Enqueuer
// ---------------------------------------------------------------------------

// queueEnqueuer schedules process advances on a durable queue, which is what makes
// the engine multi-replica: any replica consuming the queue can advance any run.
type queueEnqueuer struct {
	queue   spi.JobQueue
	delayed spi.QueueDelay
	jobType string
}

func newQueueEnqueuer(store string, resource Resource) (process.Enqueuer, error) {
	queue, ok := resource.(spi.JobQueue)
	if !ok {
		return nil, fmt.Errorf("resource %q is not a queue", store)
	}
	enqueuer := &queueEnqueuer{queue: queue, jobType: processAdvanceJobType}
	enqueuer.delayed, _ = queue.(spi.QueueDelay)
	if enqueuer.delayed == nil {
		// Without delayed enqueue a timer cannot become a wake-up, so every parked
		// run would need the ticker to notice it. That works, but it is worth saying
		// plainly rather than discovering it as latency.
		return nil, fmt.Errorf("queue resource %q cannot schedule delayed jobs, which a durable process needs for timers. Use queue.sql or queue.file", store)
	}
	return enqueuer, nil
}

// processAdvanceJobType is the reserved job type carrying advance work.
const processAdvanceJobType = "__process_advance__"

// EnqueueAdvance implements process.Enqueuer.
func (q *queueEnqueuer) EnqueueAdvance(_ context.Context, runID string) error {
	// The run id doubles as the concurrency key where the queue supports one, so two
	// advances of the same run never run at once — belt and braces alongside the
	// lease.
	if native, ok := q.queue.(*fh.DurableQueue); ok {
		_, err := native.EnqueueWithKey(q.jobType, map[string]string{"run_id": runID}, runID)
		return err
	}
	_, err := q.queue.Enqueue(q.jobType, map[string]string{"run_id": runID})
	return err
}

// EnqueueAdvanceAt implements process.Enqueuer.
func (q *queueEnqueuer) EnqueueAdvanceAt(_ context.Context, runID string, at time.Time) error {
	_, err := q.delayed.EnqueueDelayed(q.jobType, map[string]string{"run_id": runID}, at)
	return err
}

// ---------------------------------------------------------------------------
// Notifier
// ---------------------------------------------------------------------------

// processNotifier routes the engine's operational events to a channel resource.
type processNotifier struct{ platform *Platform }

// NotifyProcess implements process.Notifier.
func (n *processNotifier) NotifyProcess(ctx context.Context, event process.NotifyEvent) error {
	if event.Channel == "" {
		// Nothing configured: the event is still worth a log line, which the
		// platform's observer already emits. Silence here is deliberate rather than
		// a swallowed error.
		return nil
	}
	resource, ok := n.platform.resources[event.Channel]
	if !ok {
		return fmt.Errorf("ref/platform: notification channel %q is not declared", event.Channel)
	}
	subject, body := describeNotifyEvent(event)

	switch channel := resource.(type) {
	case spi.Notifier:
		_, err := channel.Notify(ctx, spi.Notification{
			Target:  event.Kind,
			Subject: subject,
			Body:    body,
			Data:    notifyPayload(event),
		})
		return err
	case spi.JobQueue:
		_, err := channel.Enqueue("process.notification", notifyPayload(event))
		return err
	default:
		return fmt.Errorf("ref/platform: resource %q cannot deliver process notifications", event.Channel)
	}
}

func describeNotifyEvent(event process.NotifyEvent) (string, string) {
	subject := strings.ReplaceAll(event.Kind, "_", " ")
	if event.Run != nil {
		subject = fmt.Sprintf("%s: %s", event.Run.Process, subject)
	}
	body := event.Detail
	if body == "" {
		body = subject
	}
	return subject, body
}

func notifyPayload(event process.NotifyEvent) map[string]any {
	payload := map[string]any{"kind": event.Kind, "detail": event.Detail}
	if event.Step != "" {
		payload["step"] = event.Step
	}
	if event.Run != nil {
		payload["run"] = map[string]any{
			"id":        event.Run.ID,
			"process":   event.Run.Process,
			"status":    string(event.Run.Status),
			"tenant_id": event.Run.TenantID,
		}
	}
	if event.Task != nil {
		payload["task"] = map[string]any{
			"id":       event.Task.ID,
			"title":    event.Task.Title,
			"step":     event.Task.Step,
			"assignee": event.Task.Assignee,
			"role":     event.Task.Role,
			"due_at":   event.Task.DueAt,
		}
	}
	return payload
}

// ---------------------------------------------------------------------------
// Store providers
// ---------------------------------------------------------------------------

func registerProcessStoreResources(r *Registry) {
	mustResource(r, "store.sql", ResourceFactoryFunc(openProcessSQLStore), ResourceKindInfo{
		Family:   "store",
		Summary:  "Durable process state in SQL tables. Correct across replicas; the right choice for production.",
		Provides: []string{"ProcessStore"},
		Config: []ConfigField{
			{Name: "database", Type: "resource", Required: true},
			{Name: "table_prefix", Type: "string", Default: "process"},
		},
	})

	mustResource(r, "store.memory", ResourceFactoryFunc(openProcessMemoryStore), ResourceKindInfo{
		Family:   "store",
		Summary:  "Durable process state in memory. Lost on restart and invisible to other replicas — tests and development only.",
		Provides: []string{"ProcessStore"},
	})
}

func openProcessSQLStore(ctx context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("store.sql", spec.Config, "database", "table_prefix"); err != nil {
		return nil, nil, err
	}
	handle, err := requireSQLHandle(spec, "database")
	if err != nil {
		return nil, nil, err
	}
	store, err := process.NewSQLStore(process.SQLStoreConfig{
		DB:          handle.DB,
		Dialect:     handle.Dialect,
		TablePrefix: configString(spec.Config, "table_prefix", "process"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("store.sql %q: %w", spec.Name, err)
	}
	if err := store.Migrate(ctx); err != nil {
		return nil, nil, fmt.Errorf("store.sql %q: migrate: %w", spec.Name, err)
	}
	return store, store, nil
}

func openProcessMemoryStore(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("store.memory", spec.Config); err != nil {
		return nil, nil, err
	}
	store := process.NewMemoryStore()
	return store, store, nil
}

// interface assertions keep the bridge honest: a contract drift becomes a compile
// error here rather than a type assertion failure in a deployment.
var (
	_ process.Guard      = processGuard{}
	_ process.Valuer     = processValuer{}
	_ process.Renderer   = processRenderer{}
	_ process.Shaper     = processShaper{}
	_ process.StepRunner = (*intentStepRunner)(nil)
	_ process.Enqueuer   = (*queueEnqueuer)(nil)
	_ process.Notifier   = (*processNotifier)(nil)
)

// processFailure maps a step error onto a platform failure so a caller inspecting
// the run's error sees the same categories a request would.
func processFailure(err error) error {
	if err == nil {
		return nil
	}
	var failure intent.Failure
	if errors.As(err, &failure) {
		return failure
	}
	if errors.Is(err, process.ErrRunNotFound) {
		return notFound("run", "")
	}
	if errors.Is(err, process.ErrTaskNotFound) {
		return notFound("task", "")
	}
	if errors.Is(err, process.ErrRevisionConflict) {
		return conflict("that changed while you were working on it; reload and try again")
	}
	return err
}

// knownProcessStatuses is the set an admin route accepts as a filter, so a typo is
// rejected rather than silently matching nothing.
var knownProcessStatuses = []string{
	string(process.StatusPending), string(process.StatusRunning), string(process.StatusWaiting),
	string(process.StatusCompensating), string(process.StatusCompleted),
	string(process.StatusFailed), string(process.StatusCancelled),
}

func validProcessStatus(value string) bool {
	return value == "" || slices.Contains(knownProcessStatuses, value)
}
