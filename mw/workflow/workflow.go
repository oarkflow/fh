package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

type StepType uint8

const (
	StepSync StepType = iota
	StepAsync
	StepBranch
	StepParallel
)

// Step is one unit of work inside a Workflow.
type Step struct {
	Name      string
	Type      StepType
	Handler   fh.HandlerFunc
	JobType   string
	Condition func(fh.Ctx) bool
	Branches  []*Workflow

	// Timeout bounds a StepSync handler's execution. If <= 0, no deadline is applied.
	// The step observes fh.Ctx.Context() the same way mw/timeout does; it is the
	// handler's responsibility to check ctx.Done() for long-running work.
	Timeout time.Duration

	// RetryAttempts is the number of additional attempts after the first failure
	// for a StepSync handler. 0 means no retry.
	RetryAttempts int

	// RetryBackoff is the delay between retry attempts. Ignored if RetryAttempts is 0.
	RetryBackoff time.Duration

	// ParallelFailFast controls StepParallel behavior. When true (default), the
	// first branch error cancels the workflow. When false, all branches run to
	// completion and their errors are joined.
	ParallelFailFast bool
}

// StepOption configures a Step registered through UseWithOptions.
type StepOption func(*Step)

// WithCondition only runs the step when fn returns true.
func WithCondition(fn func(fh.Ctx) bool) StepOption {
	return func(s *Step) { s.Condition = fn }
}

// WithTimeout bounds the step's execution time via a deadline context.
func WithTimeout(d time.Duration) StepOption {
	return func(s *Step) { s.Timeout = d }
}

// WithRetry retries a failed step up to attempts additional times, waiting
// backoff between attempts. Retries stop early if the context is done.
func WithRetry(attempts int, backoff time.Duration) StepOption {
	return func(s *Step) { s.RetryAttempts = attempts; s.RetryBackoff = backoff }
}

// Workflow is an ordered, composable sequence of steps executed as middleware.
type Workflow struct {
	Name      string
	condition func(fh.Ctx) bool
	Steps     []Step

	onStepStart    func(step string)
	onStepComplete func(step string, err error, dur time.Duration)
	onComplete     func(err error, dur time.Duration)
	onError        func(step string, err error) error
}

// New creates a named workflow.
func New(name string) *Workflow {
	return &Workflow{Name: name}
}

// Condition only runs the whole workflow when fn returns true.
func (w *Workflow) Condition(fn func(fh.Ctx) bool) *Workflow {
	w.condition = fn
	return w
}

// OnStepStart registers an observability hook called before each step runs.
func (w *Workflow) OnStepStart(fn func(step string)) *Workflow {
	w.onStepStart = fn
	return w
}

// OnStepComplete registers an observability hook called after each step finishes.
func (w *Workflow) OnStepComplete(fn func(step string, err error, dur time.Duration)) *Workflow {
	w.onStepComplete = fn
	return w
}

// OnComplete registers an observability hook called after the whole workflow finishes.
func (w *Workflow) OnComplete(fn func(err error, dur time.Duration)) *Workflow {
	w.onComplete = fn
	return w
}

// OnError registers a compensation hook invoked when a step returns an error.
// If the hook returns nil, the workflow continues to the next step instead of
// aborting. This is intended for compensating transactions (e.g. releasing a
// reservation made by an earlier step). Panics inside steps are converted to
// errors and also routed through this hook.
func (w *Workflow) OnError(fn func(step string, err error) error) *Workflow {
	w.onError = fn
	return w
}

// Use appends a synchronous step. conditions[0], if given, gates the step.
func (w *Workflow) Use(name string, handler fh.HandlerFunc, conditions ...func(fh.Ctx) bool) *Workflow {
	step := Step{Name: name, Type: StepSync, Handler: handler}
	if len(conditions) > 0 {
		step.Condition = conditions[0]
	}
	w.Steps = append(w.Steps, step)
	return w
}

// UseWithOptions appends a synchronous step configured with timeout/retry/condition options.
func (w *Workflow) UseWithOptions(name string, handler fh.HandlerFunc, opts ...StepOption) *Workflow {
	step := Step{Name: name, Type: StepSync, Handler: handler}
	for _, opt := range opts {
		opt(&step)
	}
	w.Steps = append(w.Steps, step)
	return w
}

// Job appends an async step that hands the request off to the durable queue
// via fh.AtomicHandoff instead of running inline.
func (w *Workflow) Job(name, jobType string, conditions ...func(fh.Ctx) bool) *Workflow {
	step := Step{Name: name, Type: StepAsync, JobType: jobType}
	if len(conditions) > 0 {
		step.Condition = conditions[0]
	}
	w.Steps = append(w.Steps, step)
	return w
}

// Branch runs the first branch whose Condition passes (or the first
// unconditional branch), then stops evaluating further branches.
func (w *Workflow) Branch(name string, branches ...*Workflow) *Workflow {
	w.Steps = append(w.Steps, Step{Name: name, Type: StepBranch, Branches: branches})
	return w
}

// Parallel runs all branches concurrently and fails fast on the first error.
func (w *Workflow) Parallel(name string, branches ...*Workflow) *Workflow {
	w.Steps = append(w.Steps, Step{Name: name, Type: StepParallel, Branches: branches, ParallelFailFast: true})
	return w
}

// ParallelJoin runs all branches concurrently, always waits for every branch
// to finish, and returns a joined error if any branch failed.
func (w *Workflow) ParallelJoin(name string, branches ...*Workflow) *Workflow {
	w.Steps = append(w.Steps, Step{Name: name, Type: StepParallel, Branches: branches, ParallelFailFast: false})
	return w
}

// Handler returns the fh.HandlerFunc that executes this workflow.
func (w *Workflow) Handler() fh.HandlerFunc {
	return func(c fh.Ctx) error {
		return w.execute(c)
	}
}

func (w *Workflow) execute(c fh.Ctx) error {
	if w.condition != nil && !w.condition(c) {
		return nil
	}
	start := time.Now()
	err := w.run(c)
	if w.onComplete != nil {
		w.onComplete(err, time.Since(start))
	}
	return err
}

func (w *Workflow) run(c fh.Ctx) error {
	for _, step := range w.Steps {
		if step.Condition != nil && !step.Condition(c) {
			continue
		}
		if w.onStepStart != nil {
			w.onStepStart(step.Name)
		}
		stepStart := time.Now()
		err := w.runStep(c, step)
		if w.onStepComplete != nil {
			w.onStepComplete(step.Name, err, time.Since(stepStart))
		}
		if err != nil {
			if w.onError != nil {
				if compErr := w.onError(step.Name, err); compErr == nil {
					continue
				} else {
					return compErr
				}
			}
			return err
		}
	}
	return nil
}

func (w *Workflow) runStep(c fh.Ctx, step Step) error {
	switch step.Type {
	case StepSync:
		return runSyncStep(c, step)
	case StepAsync:
		return runAsyncStep(c, w, step)
	case StepBranch:
		return executeBranch(c, step)
	case StepParallel:
		return executeParallel(c, step)
	default:
		return nil
	}
}

func runSyncStep(c fh.Ctx, step Step) (err error) {
	if step.Handler == nil {
		return nil
	}
	attempts := step.RetryAttempts + 1
	for i := range attempts {
		if i > 0 && step.RetryBackoff > 0 {
			select {
			case <-c.Context().Done():
				return c.Context().Err()
			case <-time.After(step.RetryBackoff):
			}
		}
		err = callWithTimeout(c, step)
		if err == nil {
			return nil
		}
		if c.Context().Err() != nil {
			return err
		}
	}
	return err
}

func callWithTimeout(c fh.Ctx, step Step) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("workflow step %s: panic: %v", step.Name, r)
		}
	}()
	if step.Timeout <= 0 {
		return step.Handler(c)
	}
	parent := c.Context()
	ctx, cancel := context.WithTimeout(parent, step.Timeout)
	defer cancel()
	c.SetContext(ctx)
	defer c.SetContext(parent)
	return step.Handler(c)
}

func runAsyncStep(c fh.Ctx, w *Workflow, step Step) error {
	if step.JobType == "" {
		return nil
	}
	id, err := fh.AtomicHandoff(c, step.JobType, fh.Map{
		"workflow":   w.Name,
		"step":       step.Name,
		"request_id": c.Locals("request_id"),
	})
	if err != nil {
		return err
	}
	c.Locals("job_id", id)
	return nil
}

func executeBranch(c fh.Ctx, step Step) error {
	for _, branch := range step.Branches {
		if branch.condition != nil && !branch.condition(c) {
			continue
		}
		return branch.run(c)
	}
	return nil
}

func executeParallel(c fh.Ctx, step Step) error {
	var wg sync.WaitGroup
	errs := make([]error, len(step.Branches))

	for i, branch := range step.Branches {
		wg.Add(1)
		go func(i int, b *Workflow) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errs[i] = fmt.Errorf("workflow %s: panic: %v", b.Name, r)
				}
			}()
			errs[i] = b.run(c)
		}(i, branch)
	}
	wg.Wait()

	if step.ParallelFailFast {
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		return nil
	}
	return errors.Join(errs...)
}
