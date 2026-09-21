package graph_test

import (
	"testing"

	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
)

func TestGraphBuildSuccess(t *testing.T) {
	// A (provides 0)
	// B (requires 0, provides 1)
	// C (requires 0, provides 2)
	// D (requires 1, 2, provides 3, OperationNode)
	nodes := []*graph.Node{
		{ID: 0, Name: "A", Kind: graph.PureNode, Provides: []fact.PlanSlot{0}},
		{ID: 1, Name: "B", Kind: graph.ReadNode, Requires: []fact.PlanSlot{0}, Provides: []fact.PlanSlot{1}},
		{ID: 2, Name: "C", Kind: graph.DecisionNode, Requires: []fact.PlanSlot{0}, Provides: []fact.PlanSlot{2}},
		{ID: 3, Name: "D", Kind: graph.OperationNode, Requires: []fact.PlanSlot{1, 2}, Provides: []fact.PlanSlot{3}},
	}

	g, err := graph.Build(nodes, 4)
	if err != nil {
		t.Fatalf("unexpected build error: %v", err)
	}

	if g.InDegree[0] != 0 || g.InDegree[1] != 1 || g.InDegree[2] != 1 || g.InDegree[3] != 2 {
		t.Errorf("unexpected in-degrees: %v", g.InDegree)
	}

	plan, err := graph.Compile(g, "TestIntent", 1)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}

	if !plan.HasOperation || plan.OperationNode != 3 {
		t.Errorf("expected operation node 3, got %v (has=%v)", plan.OperationNode, plan.HasOperation)
	}

	// 3 stages: [0], [1, 2], [3]
	if len(plan.Stages) != 3 {
		t.Fatalf("expected 3 stages, got %d", len(plan.Stages))
	}
	if len(plan.Stages[0].Nodes) != 1 || plan.Stages[0].Nodes[0] != 0 {
		t.Errorf("stage 0 expected [0], got %v", plan.Stages[0].Nodes)
	}
	if len(plan.Stages[1].Nodes) != 2 {
		t.Errorf("stage 1 expected 2 nodes, got %v", plan.Stages[1].Nodes)
	}
	if len(plan.Stages[2].Nodes) != 1 || plan.Stages[2].Nodes[0] != 3 {
		t.Errorf("stage 2 expected [3], got %v", plan.Stages[2].Nodes)
	}
}

func TestGraphCycleDetection(t *testing.T) {
	// A requires 1, provides 0
	// B requires 0, provides 1
	nodes := []*graph.Node{
		{ID: 0, Name: "A", Kind: graph.PureNode, Requires: []fact.PlanSlot{1}, Provides: []fact.PlanSlot{0}},
		{ID: 1, Name: "B", Kind: graph.PureNode, Requires: []fact.PlanSlot{0}, Provides: []fact.PlanSlot{1}},
	}

	_, err := graph.Build(nodes, 2)
	if err == nil {
		t.Fatalf("expected cycle error, got nil")
	}
}

func TestGraphConflictingFactProducers(t *testing.T) {
	nodes := []*graph.Node{
		{ID: 0, Name: "A1", Kind: graph.PureNode, Provides: []fact.PlanSlot{0}},
		{ID: 1, Name: "A2", Kind: graph.PureNode, Provides: []fact.PlanSlot{0}},
	}

	_, err := graph.Build(nodes, 1)
	if err == nil {
		t.Fatalf("expected conflict error, got nil")
	}
}

func TestGraphMissingFactProducer(t *testing.T) {
	nodes := []*graph.Node{
		{ID: 0, Name: "A", Kind: graph.PureNode, Requires: []fact.PlanSlot{99}},
	}

	_, err := graph.Build(nodes, 100)
	if err == nil {
		t.Fatalf("expected missing producer error, got nil")
	}
}
