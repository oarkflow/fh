package saga

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrSagaFailed indicates that the saga failed and compensation was executed.
var ErrSagaFailed = errors.New("saga transaction failed and rolled back")

// Step represents a single transaction boundary in a distributed workflow.
type Step struct {
	Name string
	// Action is the forward operation (e.g., Charge Credit Card, Insert Row).
	Action func(ctx context.Context) error
	// Compensate is the reverse operation (e.g., Refund Credit Card, Delete Row).
	// It is only called if the Action succeeded but a subsequent Step failed.
	Compensate func(ctx context.Context) error
}

// Orchestrator manages a distributed transaction using the Saga pattern.
type Orchestrator struct {
	steps []Step
	mu    sync.Mutex
}

// NewOrchestrator creates a new Saga workflow manager.
func NewOrchestrator() *Orchestrator {
	return &Orchestrator{
		steps: make([]Step, 0),
	}
}

// AddStep appends a transactional step to the workflow.
func (o *Orchestrator) AddStep(name string, action, compensate func(ctx context.Context) error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	
	o.steps = append(o.steps, Step{
		Name:       name,
		Action:     action,
		Compensate: compensate,
	})
}

// Execute runs the Saga. If any Step's Action fails, it will immediately halt
// and execute the Compensate functions of all previously successful steps in reverse order.
func (o *Orchestrator) Execute(ctx context.Context) error {
	o.mu.Lock()
	steps := o.steps // create a snapshot
	o.mu.Unlock()

	var completedSteps []Step

	for _, step := range steps {
		err := step.Action(ctx)
		if err != nil {
			// A step failed. We must roll back all previously completed steps.
			fmt.Printf("[SAGA] Step '%s' failed: %v. Initiating compensation (rollback)...\n", step.Name, err)
			
			// Execute compensation in reverse order
			for i := len(completedSteps) - 1; i >= 0; i-- {
				compStep := completedSteps[i]
				if compStep.Compensate != nil {
					compErr := compStep.Compensate(ctx)
					if compErr != nil {
						// In a true enterprise system, this requires human intervention or dead-letter queues.
						fmt.Printf("[SAGA CRITICAL] Compensation for step '%s' failed: %v\n", compStep.Name, compErr)
					} else {
						fmt.Printf("[SAGA] Successfully compensated step '%s'\n", compStep.Name)
					}
				}
			}
			// Return wrapped error
			return fmt.Errorf("%w: failed at step '%s' (%v)", ErrSagaFailed, step.Name, err)
		}
		
		// Mark step as completed so it can be compensated if a future step fails.
		fmt.Printf("[SAGA] Step '%s' completed successfully.\n", step.Name)
		completedSteps = append(completedSteps, step)
	}

	return nil
}
