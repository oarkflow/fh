package execution

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Verdict is the composite authorization outcome.
type Verdict uint8

const (
	VerdictPending Verdict = iota // not yet decided
	VerdictAllow                  // all evaluated policies passed
	VerdictDeny                   // at least one policy denied
)

func (v Verdict) String() string {
	switch v {
	case VerdictPending:
		return "pending"
	case VerdictAllow:
		return "allow"
	case VerdictDeny:
		return "deny"
	default:
		return "unknown"
	}
}

// Reason records why a decision was made.
type Reason struct {
	Policy  string
	Verdict Verdict
	Message string
}

// Obligation is something the application must execute as a condition of approval.
type Obligation struct {
	Name   string
	Action string
	Data   any
}

// Constraint restricts downstream data access.
type Constraint struct {
	Field    string
	Operator string // "eq", "in", "not_in", "lt", "gt"
	Values   []string
}

// ConstraintSet is the intersection of all policy constraints.
type ConstraintSet struct {
	TenantID string
	Regions  []string
	Fields   []string // allowed projection fields
	Filters  []Constraint
}

// DecisionSet accumulates decisions from multiple policy nodes.
// Composition rules:
//   - DENY dominates (any deny -> final deny)
//   - VerdictAllow requires all required policy nodes to have completed Allow
//   - Constraints intersect (narrowest scope wins)
//   - Contradictory constraints (e.g. conflicting tenant IDs, disjoint regions) yield Deny
//   - Obligations accumulate
type DecisionSet struct {
	mu              sync.Mutex
	required        int32
	completed       int32
	atomicCompleted atomic.Int32
	denied          bool
	atomicDenied    atomic.Bool
	constraints     ConstraintSet
	obligations     []Obligation
	reasons         []Reason
}

var decisionSetPool = sync.Pool{
	New: func() any {
		return &DecisionSet{}
	},
}

// AcquireDecisionSet obtains a DecisionSet from the pool with the given required policy count.
func AcquireDecisionSet(required int32) *DecisionSet {
	ds := decisionSetPool.Get().(*DecisionSet)
	ds.required = required
	ds.completed = 0
	ds.atomicCompleted.Store(0)
	ds.denied = false
	ds.atomicDenied.Store(false)
	ds.constraints = ConstraintSet{}
	ds.obligations = ds.obligations[:0]
	ds.reasons = ds.reasons[:0]
	return ds
}

// ReleaseDecisionSet returns a DecisionSet to the pool.
func ReleaseDecisionSet(ds *DecisionSet) {
	if ds == nil {
		return
	}
	decisionSetPool.Put(ds)
}

// NewDecisionSet creates a new decision set with an expected number of required policy nodes.
func NewDecisionSet(required ...int32) *DecisionSet {
	var req int32
	if len(required) > 0 {
		req = required[0]
	}
	return &DecisionSet{required: req}
}

// RecordAllow records an allow decision with optional constraints and obligations.
func (ds *DecisionSet) RecordAllow(policy string, constraints []Constraint, obligations ...Obligation) {
	if ds == nil {
		return
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.completed++
	ds.atomicCompleted.Add(1)
	ds.reasons = append(ds.reasons, Reason{Policy: policy, Verdict: VerdictAllow})
	ds.obligations = append(ds.obligations, obligations...)

	// Intersect constraints with contradiction detection
	for _, c := range constraints {
		ds.applyConstraint(c)
	}
}

// RecordDeny records a deny decision. DENY dominates — once denied, always denied.
func (ds *DecisionSet) RecordDeny(policy, message string) {
	if ds == nil {
		return
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.denied = true
	ds.atomicDenied.Store(true)
	ds.reasons = append(ds.reasons, Reason{Policy: policy, Verdict: VerdictDeny, Message: message})
}

// Verdict returns the current composite verdict.
// VerdictAllow is ONLY returned when all required policy nodes have completed and none denied.
func (ds *DecisionSet) Verdict() Verdict {
	if ds == nil {
		return VerdictPending
	}
	if ds.atomicDenied.Load() {
		return VerdictDeny
	}
	if ds.required == 0 || ds.atomicCompleted.Load() >= ds.required {
		return VerdictAllow
	}
	return VerdictPending
}

// Constraints returns a copy of the accumulated constraints.
func (ds *DecisionSet) Constraints() ConstraintSet {
	if ds == nil {
		return ConstraintSet{}
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

	res := ds.constraints
	res.Regions = append([]string(nil), ds.constraints.Regions...)
	res.Fields = append([]string(nil), ds.constraints.Fields...)
	res.Filters = append([]Constraint(nil), ds.constraints.Filters...)
	return res
}

// Obligations returns a copy of the accumulated obligations.
func (ds *DecisionSet) Obligations() []Obligation {
	if ds == nil {
		return nil
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return append([]Obligation(nil), ds.obligations...)
}

// Reasons returns a copy of the decision audit log.
func (ds *DecisionSet) Reasons() []Reason {
	if ds == nil {
		return nil
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return append([]Reason(nil), ds.reasons...)
}

func (ds *DecisionSet) applyConstraint(c Constraint) {
	switch c.Field {
	case "tenant_id":
		if len(c.Values) > 0 {
			if ds.constraints.TenantID == "" {
				ds.constraints.TenantID = c.Values[0]
			} else if ds.constraints.TenantID != c.Values[0] {
				// Contradiction detection: contradictory tenant_id constraints yield DENY
				ds.denied = true
				ds.atomicDenied.Store(true)
				ds.reasons = append(ds.reasons, Reason{
					Policy:  "constraint.algebra",
					Verdict: VerdictDeny,
					Message: fmt.Sprintf("contradictory tenant_id constraint: %q vs %q", ds.constraints.TenantID, c.Values[0]),
				})
			}
		}
	case "region":
		if ds.constraints.Regions == nil {
			ds.constraints.Regions = append([]string(nil), c.Values...)
		} else {
			intersected := intersectStrings(ds.constraints.Regions, c.Values)
			if len(intersected) == 0 {
				ds.denied = true
				ds.atomicDenied.Store(true)
				ds.reasons = append(ds.reasons, Reason{
					Policy:  "constraint.algebra",
					Verdict: VerdictDeny,
					Message: "contradictory region constraint: empty intersection",
				})
			}
			ds.constraints.Regions = intersected
		}
	case "field":
		if ds.constraints.Fields == nil {
			ds.constraints.Fields = append([]string(nil), c.Values...)
		} else {
			intersected := intersectStrings(ds.constraints.Fields, c.Values)
			if len(intersected) == 0 {
				ds.denied = true
				ds.atomicDenied.Store(true)
				ds.reasons = append(ds.reasons, Reason{
					Policy:  "constraint.algebra",
					Verdict: VerdictDeny,
					Message: "contradictory field constraint: empty intersection",
				})
			}
			ds.constraints.Fields = intersected
		}
	default:
		ds.constraints.Filters = append(ds.constraints.Filters, c)
	}
}

func intersectStrings(a, b []string) []string {
	set := make(map[string]bool, len(b))
	for _, s := range b {
		set[s] = true
	}
	var out []string
	for _, s := range a {
		if set[s] {
			out = append(out, s)
		}
	}
	return out
}
