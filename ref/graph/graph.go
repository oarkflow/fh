package graph

import (
	"fmt"

	"github.com/oarkflow/fh/ref/fact"
)

// Graph is the validated dependency DAG for one intent.
type Graph struct {
	Nodes      []*Node
	Adjacency  [][]NodeID               // node -> downstream nodes
	InDegree   []int                    // node -> number of upstream dependencies
	FactToNode map[fact.PlanSlot]NodeID // which node produces each fact
	SlotCount  int                      // total plan slots
}

// Build constructs and validates a graph from nodes.
// Validates:
//   - Node IDs form contiguous 0..N-1 sequence
//   - Every required fact has a producer
//   - No conflicting providers for any fact slot
//   - No dependency cycles (via topological sort)
func Build(nodes []*Node, slotCount int) (*Graph, error) {
	n := len(nodes)
	g := &Graph{
		Nodes:      nodes,
		Adjacency:  make([][]NodeID, n),
		InDegree:   make([]int, n),
		FactToNode: make(map[fact.PlanSlot]NodeID),
		SlotCount:  slotCount,
	}

	// Validate node IDs
	for i, node := range nodes {
		if node.ID != NodeID(i) {
			return nil, fmt.Errorf("ref: node %q has non-contiguous ID %d, expected %d", node.Name, node.ID, i)
		}
	}

	// Index: fact slot -> producing node
	for _, node := range nodes {
		for _, slot := range node.Provides {
			if existing, conflict := g.FactToNode[slot]; conflict {
				return nil, fmt.Errorf("ref: fact slot %d provided by both node %d (%q) and %d (%q)",
					slot, existing, g.Nodes[existing].Name, node.ID, node.Name)
			}
			g.FactToNode[slot] = node.ID
		}
	}

	// Build edges from requirements
	for _, node := range nodes {
		for _, req := range node.Requires {
			producer, ok := g.FactToNode[req]
			if !ok {
				return nil, fmt.Errorf("ref: node %q requires fact slot %d with no producer",
					node.Name, req)
			}
			if producer == node.ID {
				return nil, fmt.Errorf("ref: node %q depends on its own output slot %d",
					node.Name, req)
			}
			g.Adjacency[producer] = append(g.Adjacency[producer], node.ID)
			g.InDegree[node.ID]++
		}
	}

	// Topological sort to detect cycles
	if err := g.validateAcyclic(); err != nil {
		return nil, err
	}

	return g, nil
}

func (g *Graph) validateAcyclic() error {
	inDeg := make([]int, len(g.Nodes))
	copy(inDeg, g.InDegree)

	var queue []NodeID
	for i, d := range inDeg {
		if d == 0 {
			queue = append(queue, NodeID(i))
		}
	}

	visited := 0
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		visited++
		for _, dep := range g.Adjacency[curr] {
			inDeg[dep]--
			if inDeg[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}

	if visited != len(g.Nodes) {
		return fmt.Errorf("ref: dependency cycle detected (%d/%d nodes reachable)",
			visited, len(g.Nodes))
	}
	return nil
}
