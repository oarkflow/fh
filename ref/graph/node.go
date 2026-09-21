package graph

import (
	"github.com/oarkflow/fh/ref/fact"
)

// NodeKind classifies nodes for scheduler safety rules.
type NodeKind uint8

const (
	PureNode      NodeKind = iota // deterministic computation, no I/O
	ReadNode                      // reads external state
	DecisionNode                  // policy/authorization — can deny
	OperationNode                 // the intent's business logic (no direct writes)
	EffectNode                    // mutates external state — requires commit phase
	AsyncEffect                   // mutation that may complete later
	StreamNode                    // produces incremental results
)

func (k NodeKind) String() string {
	switch k {
	case PureNode:
		return "pure"
	case ReadNode:
		return "read"
	case DecisionNode:
		return "decision"
	case OperationNode:
		return "operation"
	case EffectNode:
		return "effect"
	case AsyncEffect:
		return "async_effect"
	case StreamNode:
		return "stream"
	default:
		return "unknown"
	}
}

// SpeculationClass controls whether a node may execute before
// authorization completes. Default is NoSpeculation.
type SpeculationClass uint8

const (
	NoSpeculation    SpeculationClass = iota // never speculate (default)
	PreAuthSafe                              // safe before any identity (JSON parse, crypto verify)
	PostIdentitySafe                         // safe after identity is known but before permission
	PostPolicySafe                           // safe after all policies pass
)

func (s SpeculationClass) String() string {
	switch s {
	case NoSpeculation:
		return "no_speculation"
	case PreAuthSafe:
		return "pre_auth_safe"
	case PostIdentitySafe:
		return "post_identity_safe"
	case PostPolicySafe:
		return "post_policy_safe"
	default:
		return "unknown"
	}
}

// NodeID is a plan-local dense node identifier.
type NodeID uint32

// NodeInfo contains descriptive metadata for observers.
type NodeInfo struct {
	ID   NodeID
	Name string
	Kind NodeKind
}

// Node represents one computation unit in the execution graph.
// It is purely descriptive — executable callbacks live in the runtime layer.
type Node struct {
	ID          NodeID
	Name        string
	Kind        NodeKind
	Speculation SpeculationClass

	Requires []fact.PlanSlot // facts this node reads
	Provides []fact.PlanSlot // facts this node produces
}

// Info returns the NodeInfo metadata for this node.
func (n *Node) Info() NodeInfo {
	return NodeInfo{
		ID:   n.ID,
		Name: n.Name,
		Kind: n.Kind,
	}
}
