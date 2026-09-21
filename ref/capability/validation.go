package capability

import (
	"errors"
	"fmt"

	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/invocation"
)

var (
	ErrValidationFailed = errors.New("ref: payload validation failed")
)

// ValidatorFunc validates raw invocation input.
type ValidatorFunc func(input invocation.Input) error

// NewValidationCapability creates an input validation capability (PureNode, PreAuthSafe).
func NewValidationCapability(name string, validator ValidatorFunc, opts ...Option) Registration {
	if name == "" {
		name = "capability.validation"
	}
	reg := NewRegistration(name, graph.PureNode, opts...)
	if reg.Speculation == graph.NoSpeculation {
		reg.Speculation = graph.PreAuthSafe
	}

	reg.Run = func(nc *execution.NodeContext) error {
		if validator == nil {
			return nil
		}

		if err := validator(nc.Invocation().Input); err != nil {
			return fmt.Errorf("%w: %v", ErrValidationFailed, err)
		}
		return nil
	}

	return reg
}
