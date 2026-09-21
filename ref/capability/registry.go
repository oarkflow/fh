package capability

import (
	"fmt"
	"sync"

	"github.com/oarkflow/fh/ref/fact"
)

// Registry holds all registered capabilities.
type Registry struct {
	mu     sync.RWMutex
	byName map[string]*Registration
	byFact map[fact.DefinitionID]*Registration
}

// NewRegistry initializes an empty capability registry.
func NewRegistry() *Registry {
	return &Registry{
		byName: make(map[string]*Registration),
		byFact: make(map[fact.DefinitionID]*Registration),
	}
}

// Register registers a capability.
func (r *Registry) Register(reg Registration) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.byName[reg.Name]; exists {
		return fmt.Errorf("ref: duplicate capability %q", reg.Name)
	}

	copied := reg
	r.byName[reg.Name] = &copied

	for _, key := range reg.Provides {
		if existing, conflict := r.byFact[key.DefID]; conflict {
			return fmt.Errorf("ref: fact %q (def %d) already provided by capability %q",
				key.Name, key.DefID, existing.Name)
		}
		r.byFact[key.DefID] = &copied
	}

	return nil
}

// Lookup finds a capability by name.
func (r *Registry) Lookup(name string) (*Registration, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reg, ok := r.byName[name]
	return reg, ok
}

// ProducerOf finds the capability that provides a given fact DefinitionID.
func (r *Registry) ProducerOf(id fact.DefinitionID) (*Registration, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reg, ok := r.byFact[id]
	return reg, ok
}

// All returns a snapshot of all registered capabilities.
func (r *Registry) All() map[string]*Registration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	res := make(map[string]*Registration, len(r.byName))
	for k, v := range r.byName {
		res[k] = v
	}
	return res
}
