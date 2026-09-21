package capability

import (
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/graph"
)

// CacheLookupFunc checks if a cached response exists for an execution.
// Returns (value, hit, error).
type CacheLookupFunc func(nc *execution.NodeContext) (any, bool, error)

// NewCacheCapability creates a speculative or standard cache lookup capability.
func NewCacheCapability(name string, lookup CacheLookupFunc, opts ...Option) Registration {
	if name == "" {
		name = "capability.cache"
	}
	reg := NewRegistration(name, graph.PureNode, opts...)
	if reg.Speculation == graph.NoSpeculation {
		reg.Speculation = graph.PreAuthSafe
	}

	reg.Run = func(nc *execution.NodeContext) error {
		if lookup == nil {
			return nil
		}

		val, hit, err := lookup(nc)
		if err != nil {
			// Cache lookup errors should fail open rather than abort execution
			return nil
		}

		if hit {
			nc.SetShortCircuit(val)
		}

		return nil
	}

	return reg
}
