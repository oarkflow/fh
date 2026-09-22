package process

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
)

// Compensation: undoing work that already committed.
//
// A process that charges a card and then fails to ship cannot be rolled back by a
// database transaction — the charge happened in somebody else's system. What it can
// do is run the inverse of each committed step, newest first, which is the saga
// pattern. Three properties make this trustworthy rather than decorative:
//
//   - Reverse completion order. Steps are compensated in the order they actually
//     completed, backwards. Not declaration order, which can differ when branches
//     ran concurrently, and not forwards, which would refund before un-reserving.
//   - Persisted progress. The list of steps still to compensate is a column on the
//     run, and each compensation marks its step. A replica that dies halfway
//     through a rollback resumes where it stopped rather than starting over and
//     double-refunding.
//   - Loud failure. A compensation that itself fails does not get swallowed. The
//     run ends failed, the surviving compensations are recorded, and the notifier
//     is told — because a half-completed rollback is exactly the state a human
//     needs to know about.

// beginCompensation switches a run into rollback.
//
// If nothing has committed that can be undone, the run simply fails: inventing a
// compensation phase with nothing in it would only obscure the original failure.
func (e *Engine) beginCompensation(ctx context.Context, definition *Definition, run *Run, failedStep string, cause error) error {
	states, err := e.store.ListSteps(ctx, run.ID)
	if err != nil {
		return err
	}

	// Walk completions backwards, collecting the steps that declare a compensation
	// and have not already been compensated.
	pending := make([]string, 0, len(states))
	for i := len(states) - 1; i >= 0; i-- {
		state := states[i]
		if state.Status != StepCompleted || state.Compensated {
			continue
		}
		step, ok := definition.Step(state.Step)
		if !ok || step.Compensate == "" {
			continue
		}
		// The failing step is not compensated: it did not commit. Its own retry and
		// error edges were the place to handle it.
		if state.Key == failedStep && state.Step == failedStep {
			continue
		}
		pending = append(pending, state.Key)
	}

	if len(pending) == 0 {
		return e.failRun(ctx, run, failedStep, cause)
	}

	run.Status = StatusCompensating
	run.Error = cause.Error()
	run.FailedStep = failedStep
	run.Compensating = pending
	run.Waiting = nil
	// Everything still queued is abandoned: the run is unwinding, and running more
	// forward work would create more to unwind.
	run.Frames = nil

	// Outstanding timers and subscriptions belong to the forward path. Leaving them
	// would wake a compensating run with forward work.
	_ = e.store.DeleteRunTimers(ctx, run.ID, "delayed", "timeout", "wait_event", "escalation", "manual_payload")
	_ = e.store.DeleteRunSubscriptions(ctx, run.ID)
	e.cancelRunTasks(ctx, run)

	return e.queueNextCompensation(ctx, definition, run)
}

// queueNextCompensation pushes the next compensation frame, one at a time.
//
// Sequential rather than concurrent, deliberately: compensations frequently touch
// the same external state the forward steps did, and running a refund and an
// inventory release against the same order concurrently is how a rollback creates
// its own conflict.
func (e *Engine) queueNextCompensation(ctx context.Context, definition *Definition, run *Run) error {
	for len(run.Compensating) > 0 {
		key := run.Compensating[0]
		run.Compensating = run.Compensating[1:]

		state, err := e.stepStateByKey(ctx, run.ID, key)
		if err != nil {
			return err
		}
		if state == nil || state.Compensated {
			continue
		}
		step, ok := definition.Step(state.Step)
		if !ok || step.Compensate == "" {
			continue
		}
		run.Frames = append(run.Frames, Frame{
			Step:       state.Step,
			Key:        key,
			Input:      state.Result,
			From:       run.FailedStep,
			Compensate: true,
			Attempt:    1,
		})
		return e.store.SaveRun(ctx, run)
	}
	// Nothing left to undo.
	return e.finishCompensation(ctx, run)
}

// executeCompensation runs one step's inverse.
func (e *Engine) executeCompensation(ctx context.Context, run *Run, step *Step, frame Frame) stepOutcome {
	outcome := stepOutcome{frame: frame, step: step}
	if step.Compensate == "" {
		// Nothing to do; treat it as already undone so the walk continues.
		outcome.skipped = true
		return outcome
	}

	var input any
	if len(frame.Input) > 0 {
		_ = json.Unmarshal(frame.Input, &input)
	}
	scope, err := e.scope(ctx, run, frame, input, input)
	if err != nil {
		outcome.err = err
		return outcome
	}

	runCtx := ctx
	if step.Timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, step.Timeout)
		defer cancel()
	}

	_, err = e.runner.RunStep(runCtx, StepCall{
		Run: run, Step: step, Frame: frame,
		Intent: step.Compensate, Input: input, Scope: scope, Compensating: true,
	})
	outcome.err = err
	return outcome
}

// afterCompensation records the outcome and moves to the next step, or finishes.
func (e *Engine) afterCompensation(ctx context.Context, run *Run, outcome stepOutcome) error {
	definition, err := e.definitionFor(run)
	if err != nil {
		return e.failRun(ctx, run, run.FailedStep, err)
	}

	key := outcome.frame.StateKey()
	state, stateErr := e.stepStateByKey(ctx, run.ID, key)
	if stateErr == nil && state != nil {
		if outcome.err == nil {
			state.Compensated = true
			state.Status = StepCompensated
		} else {
			// The step stays uncompensated in the record. That is the truth, and the
			// truth is what an operator needs to decide what to do by hand.
			state.Error = fmt.Sprintf("compensation failed: %v", outcome.err)
		}
		_ = e.store.SaveStep(ctx, state)
	}

	if outcome.err != nil {
		// Stop the rollback here rather than carrying on. Continuing past a failed
		// compensation would leave an inconsistent middle and make the eventual
		// manual repair harder, not easier.
		remaining := slices.Clone(run.Compensating)
		if e.notifier != nil {
			_ = e.notifier.NotifyProcess(ctx, NotifyEvent{
				Kind: "compensation_failed", Run: run, Step: outcome.frame.Step,
				Detail: fmt.Sprintf("compensation for %s failed: %v. %d steps remain uncompensated: %v",
					outcome.frame.Step, outcome.err, len(remaining), remaining),
			})
		}
		return e.failRun(ctx, run, run.FailedStep,
			fmt.Errorf("%s (rollback stopped: compensating %s failed: %v; still uncompensated: %v)",
				run.Error, outcome.frame.Step, outcome.err, remaining))
	}
	return e.queueNextCompensation(ctx, definition, run)
}

// finishCompensation ends a fully rolled-back run.
//
// The status is failed, not cancelled: the run did not achieve what it set out to,
// and reporting a clean cancellation would misrepresent what happened. What the
// successful rollback buys is that the world is consistent again, which the error
// message says.
func (e *Engine) finishCompensation(ctx context.Context, run *Run) error {
	cause := run.Error
	if cause == "" {
		cause = "the run failed"
	}
	return e.failRun(ctx, run, run.FailedStep,
		fmt.Errorf("%s (every committed step was rolled back)", cause))
}

// stepStateByKey finds one step state by its key.
func (e *Engine) stepStateByKey(ctx context.Context, runID, key string) (*StepState, error) {
	states, err := e.store.ListSteps(ctx, runID)
	if err != nil {
		return nil, err
	}
	for _, state := range states {
		if state.Key == key {
			return state, nil
		}
	}
	return nil, nil
}
