package fact

import (
	"sync"
	"sync/atomic"
)

// Store holds facts for one execution. Indexed by PlanSlot (0..N-1).
type Store struct {
	slots []any
	flags []atomic.Uint32
	count atomic.Int32
}

var storePool = sync.Pool{
	New: func() any {
		s := &Store{
			slots: make([]any, 32),
			flags: make([]atomic.Uint32, 32),
		}
		return s
	},
}

// AcquireStore obtains a Store with at least n slots from the pool.
func AcquireStore(n int) *Store {
	if n < 0 {
		n = 0
	}
	if n > 32 {
		return NewStore(n)
	}
	s := storePool.Get().(*Store)
	s.count.Store(0)
	for i := 0; i < n; i++ {
		s.flags[i].Store(0)
	}
	return s
}

// ReleaseStore returns a Store to the pool if capacity fits.
func ReleaseStore(s *Store) {
	if s == nil || len(s.slots) != 32 {
		return
	}
	storePool.Put(s)
}

// NewStore creates a store with exactly n slots (one per fact in the plan).
func NewStore(n int) *Store {
	if n < 0 {
		n = 0
	}
	return &Store{
		slots: make([]any, n),
		flags: make([]atomic.Uint32, n),
	}
}

// Put stores a typed fact value at its plan slot with zero-alloc box reuse.
func Put[T any](s *Store, slot PlanSlot, value T) {
	if s == nil || int(slot) >= len(s.slots) {
		return
	}
	// Fast path: reuse existing allocated *T box
	if box, ok := s.slots[slot].(*T); ok && box != nil {
		*box = value
		if s.flags[slot].Swap(1) == 0 {
			s.count.Add(1)
		}
		return
	}
	// First use: allocate box for future reuse
	box := new(T)
	*box = value
	s.slots[slot] = box
	if s.flags[slot].Swap(1) == 0 {
		s.count.Add(1)
	}
}

// Get retrieves a typed fact value from its plan slot.
func Get[T any](s *Store, slot PlanSlot) (T, bool) {
	var zero T
	if s == nil || int(slot) >= len(s.slots) {
		return zero, false
	}
	if s.flags[slot].Load() == 0 {
		return zero, false
	}
	if box, ok := s.slots[slot].(*T); ok && box != nil {
		return *box, true
	}
	if val, ok := s.slots[slot].(T); ok {
		return val, true
	}
	if box, ok := s.slots[slot].(*any); ok && box != nil {
		if val, ok := (*box).(T); ok {
			return val, true
		}
	}
	return zero, false
}

// Has reports whether a fact has been published at the given slot.
func Has(s *Store, slot PlanSlot) bool {
	if s == nil || int(slot) >= len(s.slots) {
		return false
	}
	return s.flags[slot].Load() != 0
}

// Has reports whether a fact has been published at the given slot.
func (s *Store) Has(slot PlanSlot) bool {
	return Has(s, slot)
}

// Count returns the number of published facts.
func (s *Store) Count() int {
	if s == nil {
		return 0
	}
	return int(s.count.Load())
}

// SlotCount returns the total number of allocated slots.
func (s *Store) SlotCount() int {
	if s == nil {
		return 0
	}
	return len(s.slots)
}
