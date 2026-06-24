package workflow

import (
	"sync"

	"github.com/oarkflow/fh"
)

type StepType uint8

const (
	StepSync StepType = iota
	StepAsync
	StepBranch
	StepParallel
)

type Step struct {
	Name      string
	Type      StepType
	Handler   fh.HandlerFunc
	JobType   string
	Condition func(fh.Ctx) bool
	Branches  []*Workflow
}

type Workflow struct {
	Name      string
	condition func(fh.Ctx) bool
	Steps     []Step
}

func New(name string) *Workflow {
	return &Workflow{Name: name}
}

func (w *Workflow) Condition(fn func(fh.Ctx) bool) *Workflow {
	w.condition = fn
	return w
}

func (w *Workflow) Use(name string, handler fh.HandlerFunc, conditions ...func(fh.Ctx) bool) *Workflow {
	step := Step{Name: name, Type: StepSync, Handler: handler}
	if len(conditions) > 0 {
		step.Condition = conditions[0]
	}
	w.Steps = append(w.Steps, step)
	return w
}

func (w *Workflow) Job(name, jobType string, conditions ...func(fh.Ctx) bool) *Workflow {
	step := Step{Name: name, Type: StepAsync, JobType: jobType}
	if len(conditions) > 0 {
		step.Condition = conditions[0]
	}
	w.Steps = append(w.Steps, step)
	return w
}

func (w *Workflow) Branch(name string, branches ...*Workflow) *Workflow {
	w.Steps = append(w.Steps, Step{Name: name, Type: StepBranch, Branches: branches})
	return w
}

func (w *Workflow) Parallel(name string, branches ...*Workflow) *Workflow {
	w.Steps = append(w.Steps, Step{Name: name, Type: StepParallel, Branches: branches})
	return w
}

func (w *Workflow) Handler() fh.HandlerFunc {
	return func(c fh.Ctx) error {
		return w.execute(c)
	}
}

func (w *Workflow) execute(c fh.Ctx) error {
	if w.condition != nil && !w.condition(c) {
		return nil
	}
	for _, step := range w.Steps {
		if step.Condition != nil && !step.Condition(c) {
			continue
		}
		switch step.Type {
		case StepSync:
			if step.Handler != nil {
				if err := step.Handler(c); err != nil {
					return err
				}
			}
		case StepAsync:
			if step.JobType != "" {
				id, err := fh.AtomicHandoff(c, step.JobType, fh.Map{
					"workflow":   w.Name,
					"step":       step.Name,
					"request_id": c.Locals("request_id"),
				})
				if err != nil {
					return err
				}
				c.Locals("job_id", id)
			}
		case StepBranch:
			if err := executeBranch(c, step); err != nil {
				return err
			}
		case StepParallel:
			if err := executeParallel(c, step); err != nil {
				return err
			}
		}
	}
	return nil
}

func executeBranch(c fh.Ctx, step Step) error {
	for _, branch := range step.Branches {
		if branch.condition != nil && !branch.condition(c) {
			continue
		}
		if err := branch.execute(c); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func executeParallel(c fh.Ctx, step Step) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(step.Branches))

	for _, branch := range step.Branches {
		wg.Add(1)
		go func(b *Workflow) {
			defer wg.Done()
			if err := b.execute(c); err != nil {
				errCh <- err
			}
		}(branch)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}
