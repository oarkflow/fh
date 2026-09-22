package platform

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/oarkflow/fh/ref/process"
)

// Durable process actions.
//
// These are the bridge from the request tier into the durable one: a login intent
// can start a fulfilment process, an admin intent can list what is waiting, and a
// task-completion intent can record somebody's approval. What they deliberately do
// not do is wait: an action that blocked a request until a durable run finished
// would defeat the reason the run is durable.
//
// Every one of them resolves its engine at build time, so a route pointing at a
// process with no store fails the deployment rather than the request.

// registerProcessActions installs both halves of the durable-process family.
func registerProcessActions(r *Registry) {
	registerProcessRunActions(r)
	registerTaskActions(r)
}

func registerProcessRunActions(r *Registry) {
	mustAction(r, "process.start", processStartAction, ActionInfo{
		Family:   "process",
		Summary:  "Start a durable process run and publish its id",
		Provides: "An object with run_id and status",
		Kind:     "effect",
		Config: []ConfigField{
			{Name: "process", Type: "process", Required: true},
			{Name: "input_fact", Type: "fact", Default: "input"},
			{Name: "idempotency_fact", Type: "fact", Summary: "Deduplicates run creation; a repeat returns the original run"},
			{Name: "correlation_fact", Type: "fact"},
			{Name: "wait", Type: "bool", Default: "false", Summary: "Advance the run before returning. It still does not wait for a parked run."},
		},
	})

	mustAction(r, "process.signal", processSignalAction, ActionInfo{
		Family:   "process",
		Summary:  "Deliver an event to whichever runs are waiting for it",
		Provides: "An object with the woken run ids",
		Kind:     "effect",
		Config: []ConfigField{
			{Name: "event", Type: "string", Required: true},
			{Name: "correlation", Type: "expression", Required: true, Summary: "Which run the event belongs to"},
			{Name: "payload_fact", Type: "fact", Default: "input"},
		},
	})

	mustAction(r, "process.status", processStatusAction, ActionInfo{
		Family:   "process",
		Summary:  "Read a run's state, and optionally its step history",
		Provides: "The run, and its steps when include_steps is set",
		Kind:     "read",
		Config: []ConfigField{
			{Name: "run_id_fact", Type: "fact", Required: true},
			{Name: "process", Type: "process", Summary: "Which engine to ask. Omit when the application has one."},
			{Name: "include_steps", Type: "bool", Default: "false"},
		},
	})

	mustAction(r, "process.cancel", processCancelAction, ActionInfo{
		Family:   "process",
		Summary:  "Cancel a run",
		Provides: "The cancelled run",
		Kind:     "effect",
		Config: []ConfigField{
			{Name: "run_id_fact", Type: "fact", Required: true},
			{Name: "process", Type: "process"},
			{Name: "reason", Type: "template"},
		},
	})

	mustAction(r, "process.list", processListAction, ActionInfo{
		Family:   "process",
		Summary:  "List runs for an operations view",
		Provides: "A list of runs",
		Kind:     "read",
		Config: []ConfigField{
			{Name: "process", Type: "process"},
			{Name: "status", Type: "string", Summary: "pending, running, waiting, compensating, completed, failed or cancelled"},
			{Name: "waiting", Type: "bool", Summary: "Only parked runs"},
			{Name: "overdue", Type: "bool", Summary: "Only runs past their SLA breach"},
			{Name: "limit", Type: "int", Default: "50"},
		},
	})

	mustAction(r, "process.advance_manual", processAdvanceManualAction, ActionInfo{
		Family:  "process",
		Summary: "Release an operator gate on a parked run",
		Kind:    "effect",
		Config: []ConfigField{
			{Name: "run_id_fact", Type: "fact", Required: true},
			{Name: "process", Type: "process"},
			{Name: "edge", Type: "string", Summary: "Which gate, when a run has more than one"},
		},
	})
}

func registerTaskActions(r *Registry) {
	mustAction(r, "task.list", taskListAction, ActionInfo{
		Family:   "process",
		Summary:  "List the tasks a caller may act on: their own, plus their roles' queues",
		Provides: "A list of tasks",
		Kind:     "read",
		Config: []ConfigField{
			{Name: "process", Type: "process"},
			{Name: "scope", Type: "string", Default: "mine", Summary: `"mine" uses the caller's identity and roles; "all" lists everything (guard it with authz)`},
			{Name: "status", Type: "string"},
			{Name: "queue", Type: "string"},
			{Name: "overdue", Type: "bool"},
			{Name: "limit", Type: "int", Default: "50"},
		},
	})

	mustAction(r, "task.get", taskGetAction, ActionInfo{
		Family:   "process",
		Summary:  "Read one task",
		Provides: "The task",
		Kind:     "read",
		Config: []ConfigField{
			{Name: "task_id_fact", Type: "fact", Required: true},
			{Name: "process", Type: "process"},
		},
	})

	mustAction(r, "task.claim", taskClaimAction, ActionInfo{
		Family:   "process",
		Summary:  "Claim an open task for the caller",
		Provides: "The claimed task",
		Kind:     "effect",
		Config: []ConfigField{
			{Name: "task_id_fact", Type: "fact", Required: true},
			{Name: "process", Type: "process"},
		},
	})

	mustAction(r, "task.release", taskReleaseAction, ActionInfo{
		Family:   "process",
		Summary:  "Return a claimed task to its queue",
		Provides: "The released task",
		Kind:     "effect",
		Config: []ConfigField{
			{Name: "task_id_fact", Type: "fact", Required: true},
			{Name: "process", Type: "process"},
		},
	})

	mustAction(r, "task.complete", taskCompleteAction, ActionInfo{
		Family:   "process",
		Summary:  "Record the caller's decision and resume the run",
		Provides: "The completed task",
		Kind:     "effect",
		Config: []ConfigField{
			{Name: "task_id_fact", Type: "fact", Required: true},
			{Name: "action_fact", Type: "fact", Default: "input.action"},
			{Name: "payload_fact", Type: "fact", Default: "input"},
			{Name: "process", Type: "process"},
		},
	})

	mustAction(r, "task.reassign", taskReassignAction, ActionInfo{
		Family:   "process",
		Summary:  "Hand a task to somebody else, when the step allows it",
		Provides: "The reassigned task",
		Kind:     "effect",
		Config: []ConfigField{
			{Name: "task_id_fact", Type: "fact", Required: true},
			{Name: "assignee_fact", Type: "fact", Required: true},
			{Name: "process", Type: "process"},
		},
	})
}

// resolveEngine finds the engine an action should use: the one running the named
// process, or the application's only one.
//
// Resolving at build time means a route that points at a process with no store is a
// deployment failure. Resolving lazily would make it a 500 on the first request,
// which is exactly the class of surprise this platform exists to remove.
func resolveEngine(build BuildContext, spec NodeSpec) (*process.Engine, error) {
	platform := build.Platform
	if platform == nil {
		return nil, fmt.Errorf("node %q: no platform is available", spec.Name)
	}
	if name := configString(spec.Config, "process", ""); name != "" {
		engine, ok := platform.processes[name]
		if !ok {
			return nil, fmt.Errorf("node %q: process %q is not declared, or has no store", spec.Name, name)
		}
		return engine, nil
	}
	switch len(platform.engines) {
	case 0:
		return nil, fmt.Errorf("node %q: this application declares no process store, so there is no durable engine to use", spec.Name)
	case 1:
		for _, engine := range platform.engines {
			return engine, nil
		}
	}
	return nil, fmt.Errorf("node %q: this application has several process stores, so config.process must name which process to use", spec.Name)
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

var processStartAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	name, err := requiredString(spec.Config, "process")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	engine, err := resolveEngine(build, spec)
	if err != nil {
		return nil, err
	}
	if _, ok := engine.Definition(name); !ok {
		return nil, fmt.Errorf("node %q: process %q is not registered", spec.Name, name)
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	inputFact := configString(spec.Config, "input_fact", "input")
	idempotencyFact := configString(spec.Config, "idempotency_fact", "")
	correlationFact := configString(spec.Config, "correlation_fact", "")
	// Detached is the default: a request that starts a fulfilment should return as
	// soon as the run exists, not wait for its first step.
	detached := !configBool(spec.Config, "wait", false)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		input, _ := resolvePath(ctx.Inputs, inputFact)
		options := process.StartOptions{
			TenantID:    ctx.TenantID,
			PrincipalID: ctx.Principal.ID,
			Detached:    detached,
		}
		if idempotencyFact != "" {
			if value, found := resolvePath(ctx.Inputs, idempotencyFact); found {
				options.IdempotencyKey = Stringify(value)
			}
		}
		if correlationFact != "" {
			if value, found := resolvePath(ctx.Inputs, correlationFact); found {
				options.CorrelationID = Stringify(value)
			}
		}
		run, err := engine.Start(ctx.Context, name, input, options)
		if err != nil {
			return ActionResult{}, processFailure(err)
		}
		return singleOutput(spec, runView(run)), nil
	}), nil
})

var processSignalAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	engine, err := resolveEngine(build, spec)
	if err != nil {
		return nil, err
	}
	event, err := requiredString(spec.Config, "event")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	correlation, err := requiredExpr(spec.Config, "correlation")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	payloadFact := configString(spec.Config, "payload_fact", "input")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		key, err := correlation.String(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		if key == "" {
			// An empty correlation would wake every run waiting for this event name,
			// which is almost never the intent and is destructive when it is not.
			return ActionResult{}, invalidInput("the event correlation evaluated to empty")
		}
		payload, _ := resolvePath(ctx.Inputs, payloadFact)
		woken, err := engine.Signal(ctx.Context, event, key, payload)
		if err != nil {
			return ActionResult{}, processFailure(err)
		}
		return acknowledgement(spec, map[string]any{"event": event, "correlation": key, "woken": woken}), nil
	}), nil
})

var processStatusAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	engine, err := resolveEngine(build, spec)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	runFact, err := requiredString(spec.Config, "run_id_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	includeSteps := configBool(spec.Config, "include_steps", false)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		runID, err := factString(ctx.Inputs, runFact)
		if err != nil {
			return ActionResult{}, invalidInput("%v", err)
		}
		if !includeSteps {
			run, err := engine.Store().GetRun(ctx.Context, runID)
			if err != nil {
				return ActionResult{}, processFailure(err)
			}
			if err := assertRunVisible(ctx, run); err != nil {
				return ActionResult{}, err
			}
			return singleOutput(spec, runView(run)), nil
		}
		snapshot, err := engine.Snapshot(ctx.Context, runID)
		if err != nil {
			return ActionResult{}, processFailure(err)
		}
		if err := assertRunVisible(ctx, snapshot.Run); err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, snapshotView(snapshot)), nil
	}), nil
})

// assertRunVisible keeps one tenant from reading another's run.
//
// Reported as not-found rather than forbidden: confirming that a run exists in
// another tenant is itself a cross-tenant disclosure.
func assertRunVisible(ctx *ActionContext, run *process.Run) error {
	if ctx.TenantID == "" || run.TenantID == "" || run.TenantID == ctx.TenantID {
		return nil
	}
	return notFound("run", run.ID)
}

var processCancelAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	engine, err := resolveEngine(build, spec)
	if err != nil {
		return nil, err
	}
	runFact, err := requiredString(spec.Config, "run_id_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	reason, err := configTemplate(spec.Config, "reason", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		runID, err := factString(ctx.Inputs, runFact)
		if err != nil {
			return ActionResult{}, invalidInput("%v", err)
		}
		existing, err := engine.Store().GetRun(ctx.Context, runID)
		if err != nil {
			return ActionResult{}, processFailure(err)
		}
		if err := assertRunVisible(ctx, existing); err != nil {
			return ActionResult{}, err
		}
		message := ""
		if reason != nil {
			if message, err = reason.Render(actionEnv(ctx)); err != nil {
				return ActionResult{}, err
			}
		}
		if message == "" {
			message = "cancelled by " + orDefault(ctx.Principal.ID, "an operator")
		}
		run, err := engine.Cancel(ctx.Context, runID, message)
		if err != nil {
			return ActionResult{}, processFailure(err)
		}
		return acknowledgement(spec, runView(run)), nil
	}), nil
})

var processListAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	engine, err := resolveEngine(build, spec)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	status := configString(spec.Config, "status", "")
	if !validProcessStatus(status) {
		return nil, fmt.Errorf("node %q: %q is not a run status (use %s)", spec.Name, status, strings.Join(knownProcessStatuses, ", "))
	}
	limit, err := configInt(spec.Config, "limit", 50)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	filter := process.RunFilter{
		Process: configString(spec.Config, "process", ""),
		Status:  process.Status(status),
		Waiting: configBool(spec.Config, "waiting", false),
		Overdue: configBool(spec.Config, "overdue", false),
		Limit:   limit,
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		scoped := filter
		// A tenant-scoped caller only ever sees its own runs, whatever the node
		// configured.
		scoped.TenantID = ctx.TenantID
		if value, found := resolvePath(ctx.Inputs, "input.offset"); found {
			if offset, ok := ToFloat(value); ok && offset > 0 {
				scoped.Offset = int(offset)
			}
		}
		runs, err := engine.ListRuns(ctx.Context, scoped)
		if err != nil {
			return ActionResult{}, processFailure(err)
		}
		views := make([]any, 0, len(runs))
		for _, run := range runs {
			views = append(views, runView(run))
		}
		return singleOutput(spec, views), nil
	}), nil
})

var processAdvanceManualAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	engine, err := resolveEngine(build, spec)
	if err != nil {
		return nil, err
	}
	runFact, err := requiredString(spec.Config, "run_id_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	edge := configString(spec.Config, "edge", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		runID, err := factString(ctx.Inputs, runFact)
		if err != nil {
			return ActionResult{}, invalidInput("%v", err)
		}
		existing, err := engine.Store().GetRun(ctx.Context, runID)
		if err != nil {
			return ActionResult{}, processFailure(err)
		}
		if err := assertRunVisible(ctx, existing); err != nil {
			return ActionResult{}, err
		}
		if err := engine.AdvanceManual(ctx.Context, runID, edge); err != nil {
			return ActionResult{}, processFailure(err)
		}
		return acknowledgement(spec, map[string]any{"run_id": runID, "released": true}), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

var taskListAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	engine, err := resolveEngine(build, spec)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	scope := strings.ToLower(configString(spec.Config, "scope", "mine"))
	if !slices.Contains([]string{"mine", "all"}, scope) {
		return nil, fmt.Errorf("node %q: scope must be \"mine\" or \"all\"", spec.Name)
	}
	limit, err := configInt(spec.Config, "limit", 50)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	base := process.TaskFilter{
		Process: configString(spec.Config, "process", ""),
		Status:  process.TaskStatus(configString(spec.Config, "status", "")),
		Queue:   configString(spec.Config, "queue", ""),
		Overdue: configBool(spec.Config, "overdue", false),
		Limit:   limit,
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		filter := base
		filter.TenantID = ctx.TenantID
		if scope == "mine" {
			if ctx.Principal.ID == "" {
				// "My tasks" for nobody is nobody's tasks, not everybody's.
				return ActionResult{}, errUnauthenticated
			}
			filter.Assignee = ctx.Principal.ID
			filter.Roles = ctx.Principal.Roles
		}
		if value, found := resolvePath(ctx.Inputs, "input.offset"); found {
			if offset, ok := ToFloat(value); ok && offset > 0 {
				filter.Offset = int(offset)
			}
		}
		tasks, err := engine.ListTasks(ctx.Context, filter)
		if err != nil {
			return ActionResult{}, processFailure(err)
		}
		views := make([]any, 0, len(tasks))
		for _, task := range tasks {
			views = append(views, taskView(task))
		}
		return singleOutput(spec, views), nil
	}), nil
})

// taskCall is the shape every single-task action shares.
type taskCall struct {
	engine   *process.Engine
	taskFact string
}

func compileTaskCall(build BuildContext, spec NodeSpec) (*taskCall, error) {
	engine, err := resolveEngine(build, spec)
	if err != nil {
		return nil, err
	}
	taskFact, err := requiredString(spec.Config, "task_id_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	return &taskCall{engine: engine, taskFact: taskFact}, nil
}

// load resolves and tenant-checks the task.
func (c *taskCall) load(ctx *ActionContext) (*process.Task, error) {
	id, err := factString(ctx.Inputs, c.taskFact)
	if err != nil {
		return nil, invalidInput("%v", err)
	}
	task, err := c.engine.Task(ctx.Context, id)
	if err != nil {
		return nil, processFailure(err)
	}
	if ctx.TenantID != "" && task.TenantID != "" && task.TenantID != ctx.TenantID {
		return nil, notFound("task", id)
	}
	return task, nil
}

var taskGetAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileTaskCall(build, spec)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		task, err := call.load(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, taskView(task)), nil
	}), nil
})

var taskClaimAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileTaskCall(build, spec)
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		task, err := call.load(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		if ctx.Principal.ID == "" {
			return ActionResult{}, errUnauthenticated
		}
		claimed, err := call.engine.ClaimTask(ctx.Context, task.ID, ctx.Principal.ID)
		if err != nil {
			return ActionResult{}, conflict("%v", err)
		}
		return acknowledgement(spec, taskView(claimed)), nil
	}), nil
})

var taskReleaseAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileTaskCall(build, spec)
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		task, err := call.load(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		released, err := call.engine.ReleaseTask(ctx.Context, task.ID, ctx.Principal.ID)
		if err != nil {
			return ActionResult{}, permissionDenied(err.Error())
		}
		return acknowledgement(spec, taskView(released)), nil
	}), nil
})

var taskCompleteAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileTaskCall(build, spec)
	if err != nil {
		return nil, err
	}
	actionFact := configString(spec.Config, "action_fact", "input.action")
	payloadFact := configString(spec.Config, "payload_fact", "input")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		task, err := call.load(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		if ctx.Principal.ID == "" {
			return ActionResult{}, errUnauthenticated
		}
		chosen := ""
		if value, found := resolvePath(ctx.Inputs, actionFact); found {
			chosen = Stringify(value)
		}
		payload, _ := resolvePath(ctx.Inputs, payloadFact)

		// The completion payload is validated against the task's declared form schema
		// here, before the run resumes, so a downstream step never receives a shape
		// its own validation would have rejected.
		if task.FormSchema != "" {
			schema, ok := build.Schemas[task.FormSchema]
			if !ok {
				return ActionResult{}, unavailable("task form schema %q is not declared", task.FormSchema)
			}
			validated, err := schema.Validate(payload)
			if err != nil {
				return ActionResult{}, err
			}
			payload = validated
		}

		completed, err := call.engine.CompleteTask(ctx.Context, task.ID, ctx.Principal.ID, chosen, payload)
		if err != nil {
			return ActionResult{}, taskFailure(err)
		}
		return acknowledgement(spec, taskView(completed)), nil
	}), nil
})

var taskReassignAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileTaskCall(build, spec)
	if err != nil {
		return nil, err
	}
	assigneeFact, err := requiredString(spec.Config, "assignee_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		task, err := call.load(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		assignee, err := factString(ctx.Inputs, assigneeFact)
		if err != nil {
			return ActionResult{}, invalidInput("%v", err)
		}
		reassigned, err := call.engine.ReassignTask(ctx.Context, task.ID, ctx.Principal.ID, assignee)
		if err != nil {
			return ActionResult{}, permissionDenied(err.Error())
		}
		return acknowledgement(spec, taskView(reassigned)), nil
	}), nil
})

// taskFailure maps a task error onto a category. A validation problem and a lost
// race are different things to a caller, and reporting both as 500 would hide which.
func taskFailure(err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	switch {
	case strings.Contains(text, "cannot act on"), strings.Contains(text, "assigned to somebody else"),
		strings.Contains(text, "claimed by somebody else"), strings.Contains(text, "not yours"):
		return permissionDenied(strings.TrimPrefix(text, "ref/process: "))
	case strings.Contains(text, "already"), strings.Contains(text, "changed while"):
		return conflict("%s", strings.TrimPrefix(text, "ref/process: "))
	case strings.Contains(text, "is not one of"), strings.Contains(text, "needs one of"):
		return invalidInput("%s", strings.TrimPrefix(text, "ref/process: "))
	default:
		return processFailure(err)
	}
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

// taskView is the client-facing shape of a task. It excludes the forbid list and
// the internal revision: a work list does not need them, and the forbid list names
// other people.
func taskView(task *process.Task) map[string]any {
	view := map[string]any{
		"task_id":      task.ID,
		"run_id":       task.RunID,
		"process":      task.Process,
		"step":         task.Step,
		"status":       string(task.Status),
		"title":        task.Title,
		"instructions": task.Instructions,
		"actions":      task.Actions,
		"priority":     task.Priority,
		"created_at":   task.CreatedAt,
	}
	for key, value := range map[string]string{
		"assignee": task.Assignee, "role": task.Role, "queue": task.Queue,
		"claimed_by": task.ClaimedBy, "completed_by": task.CompletedBy, "action": task.Action,
		"form_schema": task.FormSchema,
	} {
		if value != "" {
			view[key] = value
		}
	}
	for key, value := range map[string]*time.Time{
		"due_at": task.DueAt, "claimed_at": task.ClaimedAt, "completed_at": task.CompletedAt,
	} {
		if value != nil {
			view[key] = *value
		}
	}
	if len(task.Data) > 0 {
		var data any
		if json.Unmarshal(task.Data, &data) == nil {
			view["data"] = data
		}
	}
	return view
}

// snapshotView is the operator-facing shape of a run and its history.
func snapshotView(snapshot *process.Snapshot) map[string]any {
	view := runView(snapshot.Run)
	steps := make([]any, 0, len(snapshot.Steps))
	for _, state := range snapshot.Steps {
		entry := map[string]any{
			"step":     state.Step,
			"key":      state.Key,
			"status":   string(state.Status),
			"attempt":  state.Attempt,
			"sequence": state.Sequence,
			"started":  state.StartedAt,
		}
		if state.FinishedAt != nil {
			entry["finished"] = *state.FinishedAt
		}
		if state.Error != "" {
			entry["error"] = state.Error
		}
		if state.Compensated {
			entry["compensated"] = true
		}
		if len(state.Result) > 0 {
			var result any
			if json.Unmarshal(state.Result, &result) == nil {
				entry["result"] = result
			}
		}
		steps = append(steps, entry)
	}
	view["steps"] = steps

	if len(snapshot.Tasks) > 0 {
		tasks := make([]any, 0, len(snapshot.Tasks))
		for _, task := range snapshot.Tasks {
			tasks = append(tasks, taskView(task))
		}
		view["tasks"] = tasks
	}
	if len(snapshot.Timers) > 0 {
		timers := make([]any, 0, len(snapshot.Timers))
		for _, timer := range snapshot.Timers {
			timers = append(timers, map[string]any{
				"kind": timer.Kind, "step": timer.Step, "edge": timer.Edge, "fire_at": timer.Fire,
			})
		}
		view["timers"] = timers
	}
	if len(snapshot.Subscriptions) > 0 {
		subscriptions := make([]any, 0, len(snapshot.Subscriptions))
		for _, subscription := range snapshot.Subscriptions {
			subscriptions = append(subscriptions, map[string]any{
				"event": subscription.Event, "correlation": subscription.Correlation, "step": subscription.Step,
			})
		}
		view["waiting_for"] = subscriptions
	}
	return view
}
