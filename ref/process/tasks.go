package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Human tasks.
//
// A task step does not execute a body. It creates a row, parks the run, and waits
// for a person — which is the only honest way to model an approval. A request
// cannot wait two days, and an in-memory promise cannot survive the deploy that
// happens on day one.
//
// The controls that make a task list usable in a regulated process are here rather
// than left to the application:
//
//   - Claiming is a compare-and-set on the task's revision, so two people clicking
//     "claim" at the same moment produce one winner and one clear message.
//   - ForbidPrincipals expresses separation of duties: the person who raised a
//     request cannot approve it, checked against the run's own data rather than
//     trusted to the UI.
//   - Completion validates the chosen action against the declared set, so the
//     outgoing branch edges can rely on the value they test.

// openTask creates the work item for a task step and parks the run.
func (e *Engine) openTask(ctx context.Context, run *Run, step *Step, frame Frame, scope Scope, input any) (*WaitState, *StepState, error) {
	// An existing open task for this step means the run was already parked here and
	// something re-advanced it. Creating a second task would show the same approval
	// twice.
	existing, err := e.store.ListTasks(ctx, TaskFilter{RunID: run.ID, Limit: 100})
	if err != nil {
		return nil, nil, err
	}
	for _, task := range existing {
		if task.Step == step.Name && task.Open() {
			return &WaitState{
				Reason: "task", Step: step.Name, TaskID: task.ID, Until: task.DueAt,
				Detail: "waiting for somebody to complete this task",
			}, nil, nil
		}
	}

	definition := step.Task
	now := e.now()
	task := &Task{
		ID:         randomID(),
		RunID:      run.ID,
		Process:    run.Process,
		Step:       step.Name,
		Status:     TaskOpen,
		Role:       definition.Role,
		Queue:      definition.Queue,
		Skills:     slices.Clone(definition.Skills),
		Actions:    slices.Clone(definition.Actions),
		FormSchema: definition.FormSchema,
		Priority:   definition.Priority,
		TenantID:   run.TenantID,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if encoded, err := json.Marshal(input); err == nil {
		task.Data = encoded
	}
	if definition.Title != nil {
		if title, err := definition.Title.Render(scope); err == nil {
			task.Title = title
		}
	}
	if task.Title == "" {
		task.Title = step.Name
	}
	if definition.Instructions != nil {
		if instructions, err := definition.Instructions.Render(scope); err == nil {
			task.Instructions = instructions
		}
	}
	if definition.Assignee != nil {
		if assignee, err := definition.Assignee.Render(scope); err == nil {
			task.Assignee = assignee
		}
	}
	for _, forbidden := range definition.ForbidPrincipals {
		value, err := forbidden.Value(scope)
		if err != nil {
			continue
		}
		if text := fmt.Sprint(value); text != "" {
			task.ForbidPrincipals = append(task.ForbidPrincipals, text)
		}
	}
	if definition.Due > 0 {
		due := now.Add(definition.Due)
		task.DueAt = &due
		if definition.Reminder > 0 && definition.Reminder < definition.Due {
			reminder := due.Add(-definition.Reminder)
			task.ReminderAt = &reminder
		}
	}

	// Routing picks an assignee when the strategy asks for one. A queue strategy
	// deliberately leaves the task unassigned: whoever is addressed claims it.
	if task.Assignee == "" {
		assignee, err := e.route(ctx, definition)
		if err == nil && assignee != "" {
			task.Assignee = assignee
		}
	}
	// A forbidden assignee is dropped rather than honoured. Leaving the task in the
	// role queue is the safe outcome; assigning it to somebody who must not act is
	// not.
	if task.Assignee != "" && slices.Contains(task.ForbidPrincipals, task.Assignee) {
		task.Assignee = ""
	}

	if err := e.store.SaveTask(ctx, task); err != nil {
		return nil, nil, err
	}

	state := &StepState{
		RunID: run.ID, Step: step.Name, Key: frame.StateKey(),
		Status: StepWaiting, Attempt: max(frame.Attempt, 1), StartedAt: now,
	}
	if encoded, err := json.Marshal(input); err == nil {
		state.Input = encoded
	}
	_ = e.store.SaveStep(ctx, state)

	if task.DueAt != nil && e.enqueuer != nil {
		_ = e.enqueuer.EnqueueAdvanceAt(ctx, run.ID, *task.DueAt)
	}
	if e.notifier != nil {
		_ = e.notifier.NotifyProcess(ctx, NotifyEvent{Kind: "task_assigned", Run: run, Task: task, Step: step.Name})
	}

	return &WaitState{
		Reason: "task", Step: step.Name, TaskID: task.ID, Until: task.DueAt,
		Detail: "waiting for somebody to complete this task",
	}, state, nil
}

// route picks an assignee for the strategies that need one.
//
// Round-robin and least-loaded need a roster of candidates, which this package does
// not have — the host owns user directories. Rather than inventing one, both
// strategies fall back to leaving the task in its queue, which is correct and
// visible. A host that wants real routing supplies a Router.
func (e *Engine) route(ctx context.Context, definition *TaskDefinition) (string, error) {
	if e.router == nil {
		return "", nil
	}
	switch definition.Strategy {
	case "round_robin", "least_loaded":
		return e.router.Route(ctx, RouteRequest{
			Role:     definition.Role,
			Queue:    definition.Queue,
			Skills:   definition.Skills,
			Strategy: definition.Strategy,
		})
	default:
		return "", nil
	}
}

// Router assigns a task to a person. A host implements it over whatever user
// directory it has; without one, tasks stay in their role queue.
type Router interface {
	Route(ctx context.Context, request RouteRequest) (string, error)
}

// RouteRequest describes the task being assigned.
type RouteRequest struct {
	Role     string
	Queue    string
	Skills   []string
	Strategy string
}

// SetRouter installs a task router.
func (e *Engine) SetRouter(router Router) { e.router = router }

// ClaimTask assigns an open task to one person.
//
// The revision compare-and-set is what makes this safe: two people claiming the
// same task produce one success and one "somebody else got there first", rather
// than two people both believing they own it.
func (e *Engine) ClaimTask(ctx context.Context, taskID, principal string) (*Task, error) {
	if principal == "" {
		return nil, errors.New("ref/process: claiming a task needs a principal")
	}
	task, err := e.store.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !task.Open() {
		return nil, fmt.Errorf("ref/process: task %s is %s and cannot be claimed", taskID, task.Status)
	}
	if slices.Contains(task.ForbidPrincipals, principal) {
		return nil, fmt.Errorf("ref/process: you cannot act on this task")
	}
	if task.Assignee != "" && task.Assignee != principal {
		return nil, fmt.Errorf("ref/process: task %s is assigned to somebody else", taskID)
	}
	if task.ClaimedBy != "" && task.ClaimedBy != principal {
		return nil, fmt.Errorf("ref/process: task %s was already claimed", taskID)
	}

	now := e.now()
	task.Status = TaskClaimed
	task.ClaimedBy = principal
	task.ClaimedAt = &now
	if err := e.store.SaveTask(ctx, task); err != nil {
		if errors.Is(err, ErrRevisionConflict) {
			return nil, fmt.Errorf("ref/process: somebody else claimed task %s first", taskID)
		}
		return nil, err
	}
	return task, nil
}

// ReleaseTask returns a claimed task to its queue.
func (e *Engine) ReleaseTask(ctx context.Context, taskID, principal string) (*Task, error) {
	task, err := e.store.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if task.ClaimedBy != principal {
		return nil, fmt.Errorf("ref/process: task %s is not yours to release", taskID)
	}
	task.Status = TaskOpen
	task.ClaimedBy = ""
	task.ClaimedAt = nil
	if err := e.store.SaveTask(ctx, task); err != nil {
		return nil, err
	}
	return task, nil
}

// ReassignTask moves a task to somebody else, when the step allows it.
func (e *Engine) ReassignTask(ctx context.Context, taskID, principal, assignee string) (*Task, error) {
	task, err := e.store.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !task.Open() {
		return nil, fmt.Errorf("ref/process: task %s is %s and cannot be reassigned", taskID, task.Status)
	}
	definition, taskDefinition, err := e.taskStep(task)
	if err != nil {
		return nil, err
	}
	_ = definition
	if !taskDefinition.AllowReassign {
		return nil, fmt.Errorf("ref/process: task %s does not allow reassignment", taskID)
	}
	if slices.Contains(task.ForbidPrincipals, assignee) {
		return nil, fmt.Errorf("ref/process: %s cannot act on this task", assignee)
	}
	task.Assignee = assignee
	task.ClaimedBy = ""
	task.ClaimedAt = nil
	task.Status = TaskOpen
	if err := e.store.SaveTask(ctx, task); err != nil {
		return nil, err
	}
	if e.notifier != nil {
		run, _ := e.store.GetRun(ctx, task.RunID)
		_ = e.notifier.NotifyProcess(ctx, NotifyEvent{Kind: "task_assigned", Run: run, Task: task, Step: task.Step,
			Detail: "reassigned by " + principal})
	}
	return task, nil
}

// CompleteTask records a person's decision and resumes the run.
//
// The action is validated against the declared set, so the outgoing branch edges
// can test `result.action` and rely on it being one of the values the author
// planned for rather than whatever a client sent.
func (e *Engine) CompleteTask(ctx context.Context, taskID, principal, action string, payload any) (*Task, error) {
	task, err := e.store.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !task.Open() {
		return nil, fmt.Errorf("ref/process: task %s is already %s", taskID, task.Status)
	}
	if slices.Contains(task.ForbidPrincipals, principal) {
		return nil, fmt.Errorf("ref/process: you cannot act on this task")
	}
	if task.Assignee != "" && task.Assignee != principal && task.ClaimedBy != principal {
		return nil, fmt.Errorf("ref/process: task %s is assigned to somebody else", taskID)
	}
	if task.ClaimedBy != "" && task.ClaimedBy != principal {
		return nil, fmt.Errorf("ref/process: task %s is claimed by somebody else", taskID)
	}
	if len(task.Actions) > 0 {
		if action == "" {
			return nil, fmt.Errorf("ref/process: this task needs one of these actions: %v", task.Actions)
		}
		if !slices.Contains(task.Actions, action) {
			return nil, fmt.Errorf("ref/process: %q is not one of this task's actions: %v", action, task.Actions)
		}
	}

	// The result the step publishes: the chosen action, who chose it and when, plus
	// whatever the form submitted. Downstream edges test `action`; an audit trail
	// needs the rest.
	result := map[string]any{
		"action":       action,
		"completed_by": principal,
		"completed_at": e.now().Format(time.RFC3339),
		"task_id":      task.ID,
	}
	if object, ok := payload.(map[string]any); ok {
		for key, value := range object {
			if _, reserved := result[key]; reserved {
				continue
			}
			result[key] = value
		}
	} else if payload != nil {
		result["payload"] = payload
	}

	now := e.now()
	task.Status = TaskCompleted
	task.CompletedBy = principal
	task.CompletedAt = &now
	task.Action = action
	if encoded, err := json.Marshal(result); err == nil {
		task.Result = encoded
	}
	if err := e.store.SaveTask(ctx, task); err != nil {
		if errors.Is(err, ErrRevisionConflict) {
			return nil, fmt.Errorf("ref/process: task %s changed while you were completing it; reload and try again", taskID)
		}
		return nil, err
	}

	// Record the completion as the step's own result, so a later edge reading
	// results.<step> sees the decision.
	if state, err := e.stepStateByKey(ctx, task.RunID, task.Step); err == nil && state != nil {
		sequence, _ := e.store.NextStepSequence(ctx, task.RunID)
		state.Status = StepCompleted
		state.Sequence = sequence
		state.FinishedAt = &now
		state.Result = task.Result
		_ = e.store.SaveStep(ctx, state)
	}

	// The run continues from the task step's own outgoing edges. That is the one
	// place a resumed frame is the *same* step rather than a target: the task step
	// has now produced its result, and its edges resolve off it.
	if err := e.resumeAfterTask(ctx, task, result); err != nil {
		return task, err
	}
	return task, nil
}

// resumeAfterTask re-enters the run at the task step so its edges resolve.
//
// It shares continueFromStep with the child-process path: both are steps whose work
// finished outside an ordinary execution, and both must resolve edges rather than
// re-run the step — re-running a task step would open a second task.
func (e *Engine) resumeAfterTask(ctx context.Context, task *Task, result map[string]any) error {
	return e.continueFromStep(ctx, task.RunID, task.Step, result, nil)
}

// taskStep resolves the definition and task configuration behind a task row.
func (e *Engine) taskStep(task *Task) (*Definition, *TaskDefinition, error) {
	definition, ok := e.Definition(task.Process)
	if !ok {
		return nil, nil, fmt.Errorf("ref/process: process %q is not registered", task.Process)
	}
	step, ok := definition.Step(task.Step)
	if !ok || step.Task == nil {
		return nil, nil, fmt.Errorf("ref/process: step %q is not a task step", task.Step)
	}
	return definition, step.Task, nil
}

// ListTasks exposes the work list.
func (e *Engine) ListTasks(ctx context.Context, filter TaskFilter) ([]*Task, error) {
	return e.store.ListTasks(ctx, filter)
}

// Task returns one task.
func (e *Engine) Task(ctx context.Context, id string) (*Task, error) {
	return e.store.GetTask(ctx, id)
}

// tickTasks handles reminders and overdue tasks. It runs inside Tick so one
// scheduled job covers timers and tasks together.
func (e *Engine) tickTasks(ctx context.Context, limit int) error {
	tasks, err := e.store.DueTasks(ctx, e.now(), limit)
	if err != nil {
		return err
	}
	now := e.now()
	for _, task := range tasks {
		definition, taskDefinition, err := e.taskStep(task)
		if err != nil {
			continue
		}
		_ = definition

		// A reminder fires once: clearing the field is what makes it once.
		if task.ReminderAt != nil && !task.ReminderAt.After(now) {
			task.ReminderAt = nil
			if e.notifier != nil {
				run, _ := e.store.GetRun(ctx, task.RunID)
				_ = e.notifier.NotifyProcess(ctx, NotifyEvent{Kind: "task_reminder", Run: run, Task: task, Step: task.Step})
			}
			_ = e.store.SaveTask(ctx, task)
			continue
		}

		if task.DueAt == nil || task.DueAt.After(now) {
			continue
		}
		// Overdue. Escalate if the step says where to, otherwise notify and leave it
		// — silently cancelling somebody's outstanding approval would be worse.
		if taskDefinition.Escalate != "" && task.Role != taskDefinition.Escalate {
			task.Role = taskDefinition.Escalate
			task.Assignee = ""
			task.ClaimedBy = ""
			task.ClaimedAt = nil
			task.Status = TaskEscalated
			// Give the new owner the same window again rather than leaving the task
			// permanently overdue and re-escalating on every tick.
			extended := now.Add(taskDefinition.Due)
			task.DueAt = &extended
			_ = e.store.SaveTask(ctx, task)
			if e.notifier != nil {
				run, _ := e.store.GetRun(ctx, task.RunID)
				_ = e.notifier.NotifyProcess(ctx, NotifyEvent{Kind: "escalation", Run: run, Task: task, Step: task.Step,
					Detail: "overdue, reassigned to " + taskDefinition.Escalate})
			}
			continue
		}
		if e.notifier != nil {
			run, _ := e.store.GetRun(ctx, task.RunID)
			_ = e.notifier.NotifyProcess(ctx, NotifyEvent{Kind: "task_overdue", Run: run, Task: task, Step: task.Step})
		}
		// Clear the due date so this does not notify on every tick from now on.
		task.DueAt = nil
		_ = e.store.SaveTask(ctx, task)
	}
	return nil
}
