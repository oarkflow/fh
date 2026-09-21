package graph

import (
	"fmt"

	"github.com/oarkflow/fh/ref/fact"
)

// Plan is a compiled execution plan for one intent.
// Created at startup, immutable, reused for every invocation.
type Plan struct {
	ID            uint32
	IntentName    string
	Version       int

	Nodes         []*Node
	Adjacency     [][]NodeID // downstream dependencies
	InitialDeps   []int32    // initial dependency counts per node (template)
	SlotCount     int        // fact slots needed
	DefToSlot     map[fact.DefinitionID]fact.PlanSlot // mapping from stable DefinitionID to dense PlanSlot

	DecisionCount int    // number of decision nodes
	OperationNode NodeID // the intent's business logic node
	HasOperation  bool   // whether an operation node was found
	HasDecisions  bool   // whether any decision nodes exist
	EffectBarrier int    // visualization: stage index of barrier

	// Stages are for visualization/debug only — not used by the scheduler.
	Stages []Stage
}

// Stage is a wave of concurrently executable nodes (visualization only).
type Stage struct {
	Nodes []NodeID
	Label string // "compute", "decision", "barrier", "operation", "effect", "projection"
}

// Compile transforms a validated Graph into a Plan.
// Computes: wave analysis, effect barrier placement, initial dependency counts.
func Compile(g *Graph, intentName string, version int) (*Plan, error) {
	if g == nil {
		return nil, fmt.Errorf("ref: cannot compile nil graph")
	}

	plan := &Plan{
		IntentName:  intentName,
		Version:     version,
		Nodes:       g.Nodes,
		Adjacency:   g.Adjacency,
		InitialDeps: make([]int32, len(g.Nodes)),
		SlotCount:   g.SlotCount,
	}

	for i, d := range g.InDegree {
		plan.InitialDeps[i] = int32(d)
	}

	// Find the OperationNode and check for Decision nodes
	for _, n := range g.Nodes {
		if n.Kind == DecisionNode {
			plan.HasDecisions = true
			plan.DecisionCount++
		}
		if n.Kind == OperationNode {
			plan.OperationNode = n.ID
			plan.HasOperation = true
		}
	}

	// Wave-level analysis for visualization (Kahn's algorithm)
	plan.Stages = computeStages(g)

	// Determine effect barrier stage index
	plan.EffectBarrier = -1
	for idx, s := range plan.Stages {
		hasBarrierItem := false
		for _, nid := range s.Nodes {
			k := g.Nodes[nid].Kind
			if k == EffectNode || k == AsyncEffect || k == OperationNode {
				hasBarrierItem = true
				break
			}
		}
		if hasBarrierItem {
			plan.EffectBarrier = idx
			break
		}
	}

	return plan, nil
}

func computeStages(g *Graph) []Stage {
	inDeg := make([]int, len(g.Nodes))
	copy(inDeg, g.InDegree)

	var stages []Stage
	var queue []NodeID
	for i, d := range inDeg {
		if d == 0 {
			queue = append(queue, NodeID(i))
		}
	}

	for len(queue) > 0 {
		stage := Stage{Nodes: queue}
		if len(queue) > 0 {
			dominantKind := g.Nodes[queue[0]].Kind
			switch dominantKind {
			case DecisionNode:
				stage.Label = "decision"
			case OperationNode:
				stage.Label = "operation"
			case EffectNode, AsyncEffect:
				stage.Label = "effect"
			case PureNode:
				stage.Label = "pure"
			case ReadNode:
				stage.Label = "read"
			default:
				stage.Label = "compute"
			}
		}
		stages = append(stages, stage)

		var next []NodeID
		for _, n := range queue {
			for _, dep := range g.Adjacency[n] {
				inDeg[dep]--
				if inDeg[dep] == 0 {
					next = append(next, dep)
				}
			}
		}
		queue = next
	}

	return stages
}
