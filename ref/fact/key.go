package fact

import "sync/atomic"

// definitionCounter assigns globally unique definition IDs.
var definitionCounter atomic.Uint32

// DefinitionID is the stable, globally unique identity of a fact.
// Assigned once at package init / NewKey time. Never changes.
type DefinitionID uint32

// PlanSlot is the dense 0..N-1 index assigned during graph compilation.
// Engine-local and plan-local — different plans may assign different slots
// to the same DefinitionID.
type PlanSlot uint32

// Key is a typed fact key. T is the value type stored.
// Keys are created at package-init time.
type Key[T any] struct {
	Name  string
	defID DefinitionID
}

// NewKey creates a fact key with a stable definition ID.
func NewKey[T any](name string) Key[T] {
	return Key[T]{
		Name:  name,
		defID: DefinitionID(definitionCounter.Add(1)),
	}
}

// DefinitionID returns the stable global identity.
func (k Key[T]) DefinitionID() DefinitionID { return k.defID }

// AnyKey is a type-erased fact key for non-generic contexts.
type AnyKey struct {
	Name  string
	DefID DefinitionID
}

// Any returns the type-erased version of this Key.
func (k Key[T]) Any() AnyKey {
	return AnyKey{Name: k.Name, DefID: k.defID}
}
