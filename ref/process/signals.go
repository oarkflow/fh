package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Waking parked runs: events, timers and operator gates.
//
// Everything here shares one shape. A parked run left a durable marker — a
// subscription or a timer — recording which step to resume at and which edge
// parked it. Something external arrives, the marker is found, matched and
// consumed, a frame goes back on the run's cursor, and the run advances.
//
// Consuming the marker before advancing is what makes a signal at-most-once per
// subscription. If the advance then fails, the run keeps the frame on its cursor
// and a later advance picks it up: the work is not lost, and the event is not
// applied twice.

// Signal delivers an external event to whatever runs are waiting for it.
//
// It returns the run ids it woke. Delivering an event nobody is waiting for is not
// an error: a system emitting "payment.settled" should not have to know whether
// anything cares, and treating it as a failure would make every integration
// fragile.
func (e *Engine) Signal(ctx context.Context, name, correlation string, payload any) ([]string, error) {
	if name == "" {
		return nil, errors.New("ref/process: an event needs a name")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("ref/process: the event payload cannot be serialised: %w", err)
	}

	_ = e.store.RecordEvent(ctx, &Event{
		ID: randomID(), Name: name, Correlation: correlation,
		Payload: encoded, ReceivedAt: e.now(),
	})

	subscriptions, err := e.store.MatchSubscriptions(ctx, name, correlation)
	if err != nil {
		return nil, err
	}
	now := e.now()
	woken := make([]string, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		if subscription.ExpiresAt != nil && subscription.ExpiresAt.Before(now) {
			// The run has already given up on this event; its timeout timer will
			// clean the subscription up.
			continue
		}
		if err := e.deliver(ctx, subscription, encoded); err != nil {
			// One run failing to accept an event must not stop the others: they are
			// independent, and a partial delivery is better than none.
			continue
		}
		woken = append(woken, subscription.RunID)
	}
	return woken, nil
}

// deliver consumes one subscription and resumes its run.
func (e *Engine) deliver(ctx context.Context, subscription *Subscription, payload json.RawMessage) error {
	if err := e.store.DeleteSubscription(ctx, subscription.ID); err != nil {
		return err
	}
	// A child completion resumes *at* the calling step rather than at a target:
	// the step has now produced its result, so its own edges resolve off it. Pushing
	// a frame for the step instead would re-execute it and start a second child.
	if subscription.Event == childCompletedEvent {
		return e.resumeAfterChild(ctx, subscription, payload)
	}
	if err := e.resume(ctx, subscription.RunID, Frame{
		Step:    subscription.Step,
		Input:   payload,
		Edge:    subscription.Edge,
		Attempt: 1,
	}, subscription.Edge, "wait_event"); err != nil {
		return err
	}
	return e.advanceOrEnqueue(ctx, subscription.RunID)
}

// resume puts a frame back on a parked run's cursor.
//
// It takes the lease, so a resume cannot interleave with an advance, and it
// tolerates a revision conflict by retrying: the common cause is another signal
// arriving at the same instant, and both should land.
func (e *Engine) resume(ctx context.Context, runID string, frame Frame, edge, timerKind string) error {
	for attempt := range 3 {
		acquired, err := e.store.AcquireLease(ctx, runID, e.owner, e.leaseTTL)
		if err != nil {
			return err
		}
		if !acquired {
			// Somebody is advancing the run. Wait briefly and retry: the frame must
			// land, or the signal is lost.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
			}
			continue
		}

		err = func() error {
			defer func() { _ = e.store.ReleaseLease(context.WithoutCancel(ctx), runID, e.owner) }()

			run, err := e.store.GetRun(ctx, runID)
			if err != nil {
				return err
			}
			if run.Status.Terminal() {
				// The run finished or was cancelled while the signal was in flight.
				// Dropping it is correct; there is nothing left to tell.
				return nil
			}
			// The deadline the run was waiting on is no longer relevant.
			if edge != "" && timerKind != "" {
				_ = e.store.DeleteRunTimers(ctx, runID, timerKind)
			}
			run.Frames = append(run.Frames, frame)
			run.Status = StatusRunning
			run.Waiting = nil
			return e.store.SaveRun(ctx, run)
		}()

		if errors.Is(err, ErrRevisionConflict) {
			continue
		}
		return err
	}
	return fmt.Errorf("ref/process: could not resume run %s — it stayed busy across three attempts", runID)
}

// AdvanceManual releases an operator gate.
//
// It is the only thing that releases one: no timer, no event and no retry will.
// That is the point of a manual edge, and an engine that quietly released them
// after a while would be worse than not having them.
func (e *Engine) AdvanceManual(ctx context.Context, runID, edge string) error {
	subscriptions, err := e.store.ListSubscriptions(ctx, runID)
	if err != nil {
		return err
	}
	var target *Subscription
	for _, subscription := range subscriptions {
		if subscription.Event != manualEventName {
			continue
		}
		if edge == "" || subscription.Edge == edge {
			target = subscription
			break
		}
	}
	if target == nil {
		return fmt.Errorf("ref/process: run %s has no operator gate outstanding%s", runID, edgeSuffix(edge))
	}

	// The source step's result was parked alongside the gate so the resumed step
	// receives it rather than nothing.
	payload := json.RawMessage(nil)
	if timers, err := e.store.ListTimers(ctx, runID); err == nil {
		for _, timer := range timers {
			if timer.Kind == "manual_payload" && timer.Edge == target.Edge {
				payload = timer.Payload
				_ = e.store.DeleteTimer(ctx, timer.ID)
				break
			}
		}
	}
	if err := e.store.DeleteSubscription(ctx, target.ID); err != nil {
		return err
	}
	if err := e.resume(ctx, runID, Frame{Step: target.Step, Input: payload, Edge: target.Edge, Attempt: 1}, "", ""); err != nil {
		return err
	}
	return e.Advance(ctx, runID)
}

func edgeSuffix(edge string) string {
	if edge == "" {
		return ""
	}
	return fmt.Sprintf(" on edge %q", edge)
}

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

// Tick fires every timer that is due and returns how many it handled.
//
// Call it from a scheduled job. It is safe to call from every replica at once: the
// store claims timers atomically, so each one fires once.
func (e *Engine) Tick(ctx context.Context, limit int) (int, error) {
	timers, err := e.store.DueTimers(ctx, e.now(), limit)
	if err != nil {
		return 0, err
	}
	handled := 0
	for _, timer := range timers {
		if err := e.fireTimer(ctx, timer); err != nil {
			// One bad timer must not stop the rest. The run it belongs to keeps its
			// state, and an operator can see it in the run's own error.
			continue
		}
		handled++
	}
	// Overdue tasks are the other thing that needs a periodic look, and folding it
	// into the same tick means one scheduled job rather than two.
	if err := e.tickTasks(ctx, limit); err != nil {
		return handled, err
	}
	return handled, nil
}

// fireTimer dispatches one due timer.
func (e *Engine) fireTimer(ctx context.Context, timer *Timer) error {
	run, err := e.store.GetRun(ctx, timer.RunID)
	if err != nil {
		if errors.Is(err, ErrRunNotFound) {
			// The run was purged; its timer is just litter.
			return nil
		}
		return err
	}
	if run.Status.Terminal() {
		return nil
	}

	switch timer.Kind {
	case "run_timeout":
		definition, defErr := e.definitionFor(run)
		timeout := time.Duration(0)
		if defErr == nil {
			timeout = definition.Timeout
		}
		return e.failRun(ctx, run, "", fmt.Errorf("the run exceeded its %s timeout", timeout))

	case "sla":
		return e.handleSLABreach(ctx, run)

	case "delayed":
		// The park is over: put the continuation back and go.
		if err := e.resume(ctx, run.ID, Frame{Step: timer.Step, Input: timer.Payload, Edge: timer.Edge, Attempt: 1}, "", ""); err != nil {
			return err
		}
		return e.advanceOrEnqueue(ctx, run.ID)

	case "wait_event", "timeout":
		return e.handleDeadline(ctx, run, timer)

	case "escalation":
		return e.handleEscalation(ctx, run, timer)

	case "manual_payload":
		// Not a real timer: it carries a manual gate's payload and must never fire.
		// Reaching here means the far-future fire time was somehow met, so put it
		// back rather than acting on it.
		return e.store.AddTimer(ctx, timer)

	default:
		return fmt.Errorf("ref/process: run %s has a timer of unknown kind %q", run.ID, timer.Kind)
	}
}

// handleDeadline handles a wait-event or step timeout expiring.
func (e *Engine) handleDeadline(ctx context.Context, run *Run, timer *Timer) error {
	// A deadline that fires when the run is no longer waiting for it is stale — the
	// event already arrived, or the step already finished.
	if run.Status != StatusWaiting {
		return nil
	}
	subscriptions, err := e.store.ListSubscriptions(ctx, run.ID)
	if err != nil {
		return err
	}
	stillWaiting := slices.ContainsFunc(subscriptions, func(subscription *Subscription) bool {
		return subscription.Edge == timer.Edge
	})
	if timer.Kind == "wait_event" {
		if !stillWaiting {
			return nil
		}
		for _, subscription := range subscriptions {
			if subscription.Edge == timer.Edge {
				_ = e.store.DeleteSubscription(ctx, subscription.ID)
			}
		}
	}

	if timer.OnFire != "" {
		// A configured on_timeout target is the author's answer to "what if it never
		// comes", and taking it is not a failure.
		payload, _ := json.Marshal(map[string]any{
			"timed_out": true,
			"edge":      timer.Edge,
			"step":      timer.Step,
		})
		if err := e.resume(ctx, run.ID, Frame{Step: timer.OnFire, Input: payload, Edge: timer.Edge, Attempt: 1}, "", ""); err != nil {
			return err
		}
		return e.advanceOrEnqueue(ctx, run.ID)
	}

	latest, err := e.store.GetRun(ctx, run.ID)
	if err != nil {
		return err
	}
	definition, defErr := e.definitionFor(latest)
	cause := fmt.Errorf("the run waited for %q longer than allowed and the edge declares no on_timeout", timer.Edge)
	if defErr != nil {
		return e.failRun(ctx, latest, timer.Step, cause)
	}
	// An expiry with no configured alternative is a failure, and a failure runs
	// compensation like any other.
	return e.beginCompensation(ctx, definition, latest, timer.Step, cause)
}

// handleEscalation raises overdue work and continues at the escalation target.
func (e *Engine) handleEscalation(ctx context.Context, run *Run, timer *Timer) error {
	if e.notifier != nil {
		definition, _ := e.Definition(run.Process)
		channel := ""
		if definition != nil {
			if edge, ok := definition.Edge(timer.Edge); ok {
				channel = edge.Notify
			}
		}
		_ = e.notifier.NotifyProcess(ctx, NotifyEvent{
			Kind: "escalation", Channel: channel, Run: run, Step: timer.Step,
			Detail: fmt.Sprintf("escalating after %s", timer.Edge),
		})
	}
	if err := e.resume(ctx, run.ID, Frame{Step: timer.Step, Input: timer.Payload, Edge: timer.Edge, Attempt: 1}, "", ""); err != nil {
		return err
	}
	return e.advanceOrEnqueue(ctx, run.ID)
}

// handleSLABreach applies the definition's breach action, once.
func (e *Engine) handleSLABreach(ctx context.Context, run *Run) error {
	if run.SLABreached {
		return nil
	}
	definition, ok := e.Definition(run.Process)
	if !ok || definition.SLA == nil {
		return nil
	}
	run.SLABreached = true
	if err := e.store.SaveRun(ctx, run); err != nil && !errors.Is(err, ErrRevisionConflict) {
		return err
	}
	if e.notifier != nil {
		_ = e.notifier.NotifyProcess(ctx, NotifyEvent{
			Kind: "sla_breach", Channel: definition.SLA.Notify, Run: run,
			Detail: fmt.Sprintf("the run passed its %s SLA", definition.SLA.Breach),
		})
	}

	switch definition.SLA.OnBreach {
	case "cancel":
		_, err := e.Cancel(ctx, run.ID, "cancelled after breaching its SLA")
		return err
	case "fail":
		latest, err := e.store.GetRun(ctx, run.ID)
		if err != nil {
			return err
		}
		return e.beginCompensation(ctx, definition, latest, "", fmt.Errorf("the run breached its %s SLA", definition.SLA.Breach))
	case "escalate":
		// Reassign the run's open tasks to the escalation role so somebody with the
		// authority to unblock it sees them.
		return e.escalateRunTasks(ctx, run, definition.SLA.Escalate)
	default:
		// "notify": the notification above was the action.
		return nil
	}
}

func (e *Engine) escalateRunTasks(ctx context.Context, run *Run, role string) error {
	if role == "" {
		return nil
	}
	tasks, err := e.store.ListTasks(ctx, TaskFilter{RunID: run.ID, Limit: 100})
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if !task.Open() {
			continue
		}
		task.Role = role
		task.Assignee = ""
		task.ClaimedBy = ""
		task.ClaimedAt = nil
		task.Status = TaskEscalated
		if err := e.store.SaveTask(ctx, task); err != nil {
			continue
		}
		if e.notifier != nil {
			_ = e.notifier.NotifyProcess(ctx, NotifyEvent{Kind: "escalation", Run: run, Task: task, Step: task.Step,
				Detail: "reassigned to " + role})
		}
	}
	return nil
}

// advanceOrEnqueue prefers the queue when there is one, so a timer firing on a
// scheduler replica does not do the work on that replica.
func (e *Engine) advanceOrEnqueue(ctx context.Context, runID string) error {
	if e.enqueuer != nil {
		return e.enqueuer.EnqueueAdvance(ctx, runID)
	}
	return e.Advance(ctx, runID)
}

// ---------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------

// Purge removes terminal runs past their process's retention. Call it from a
// scheduled job.
//
// Retention is per process, and a process with none keeps its runs forever — which
// is a deliberate choice an author makes rather than a default that quietly grows
// a table until somebody notices.
func (e *Engine) Purge(ctx context.Context, limit int) (int, error) {
	total := 0
	for _, name := range e.Definitions() {
		definition, ok := e.Definition(name)
		if !ok || definition.Retention <= 0 {
			continue
		}
		purged, err := e.store.PurgeRuns(ctx, name, e.now().Add(-definition.Retention), limit)
		total += purged
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// RecoverStalled re-enqueues runs that should be advancing but are not.
//
// It is the backstop for a replica that died holding a lease: the run stays in
// "running" with nobody driving it until the lease lapses, and nothing would
// otherwise notice. Call it from a scheduled job on a longer interval than Tick.
func (e *Engine) RecoverStalled(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	if olderThan <= 0 {
		olderThan = 5 * time.Minute
	}
	runs, err := e.store.ListRuns(ctx, RunFilter{Status: StatusRunning, Limit: limit})
	if err != nil {
		return 0, err
	}
	cutoff := e.now().Add(-olderThan)
	recovered := 0
	for _, run := range runs {
		if run.UpdatedAt.After(cutoff) {
			continue
		}
		if err := e.advanceOrEnqueue(ctx, run.ID); err != nil {
			continue
		}
		recovered++
	}
	return recovered, nil
}
