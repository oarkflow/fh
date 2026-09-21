package debug

import (
	"fmt"
	"strings"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/runtime"
)

// PlanSummaryDTO provides a JSON serializable representation of a compiled plan.
type PlanSummaryDTO struct {
	IntentName    string       `json:"intent_name"`
	Version       int          `json:"version"`
	NodeCount     int          `json:"node_count"`
	SlotCount     int          `json:"slot_count"`
	EffectBarrier int          `json:"effect_barrier"`
	Stages        []StageDTO   `json:"stages"`
	Nodes         []NodeDTO    `json:"nodes"`
}

type StageDTO struct {
	Index int      `json:"index"`
	Label string   `json:"label"`
	Nodes []string `json:"nodes"`
}

type NodeDTO struct {
	ID          uint32   `json:"id"`
	Name        string   `json:"name"`
	Kind        string   `json:"kind"`
	Speculation string   `json:"speculation"`
	Downstream  []uint32 `json:"downstream"`
}

// InspectPlan creates a PlanSummaryDTO from a compiled plan.
func InspectPlan(plan *graph.Plan) PlanSummaryDTO {
	if plan == nil {
		return PlanSummaryDTO{}
	}

	dto := PlanSummaryDTO{
		IntentName:    plan.IntentName,
		Version:       plan.Version,
		NodeCount:     len(plan.Nodes),
		SlotCount:     plan.SlotCount,
		EffectBarrier: plan.EffectBarrier,
	}

	for idx, s := range plan.Stages {
		var nodeNames []string
		for _, nid := range s.Nodes {
			nodeNames = append(nodeNames, plan.Nodes[nid].Name)
		}
		dto.Stages = append(dto.Stages, StageDTO{
			Index: idx,
			Label: s.Label,
			Nodes: nodeNames,
		})
	}

	for _, n := range plan.Nodes {
		var down []uint32
		if int(n.ID) < len(plan.Adjacency) {
			for _, d := range plan.Adjacency[n.ID] {
				down = append(down, uint32(d))
			}
		}
		dto.Nodes = append(dto.Nodes, NodeDTO{
			ID:          uint32(n.ID),
			Name:        n.Name,
			Kind:        n.Kind.String(),
			Speculation: n.Speculation.String(),
			Downstream:  down,
		})
	}

	return dto
}

// ToMermaid generates a Mermaid diagram representing the DAG.
func ToMermaid(plan *graph.Plan) string {
	if plan == nil {
		return "graph TD\n"
	}

	var sb strings.Builder
	sb.WriteString("graph TD\n")

	for _, n := range plan.Nodes {
		shapeLeft, shapeRight := "[", "]"
		if n.Kind == graph.DecisionNode {
			shapeLeft, shapeRight = "{" , "}"
		} else if n.Kind == graph.OperationNode {
			shapeLeft, shapeRight = "([", "])"
		} else if n.Kind == graph.EffectNode || n.Kind == graph.AsyncEffect {
			shapeLeft, shapeRight = "[[", "]]"
		}

		sb.WriteString(fmt.Sprintf("    node%d%s\"%s (%s)\"%s\n", n.ID, shapeLeft, n.Name, n.Kind.String(), shapeRight))
	}

	for fromID, targets := range plan.Adjacency {
		for _, toID := range targets {
			sb.WriteString(fmt.Sprintf("    node%d --> node%d\n", fromID, toID))
		}
	}

	return sb.String()
}

// ToDOT generates a Graphviz DOT representation of the DAG.
func ToDOT(plan *graph.Plan) string {
	if plan == nil {
		return "digraph G {}\n"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("digraph %q {\n", plan.IntentName))
	sb.WriteString("    rankdir=LR;\n")
	sb.WriteString("    node [fontname=\"sans-serif\", fontsize=10];\n")

	for _, n := range plan.Nodes {
		shape := "box"
		if n.Kind == graph.DecisionNode {
			shape = "diamond"
		} else if n.Kind == graph.OperationNode {
			shape = "oval"
		} else if n.Kind == graph.EffectNode {
			shape = "component"
		}
		sb.WriteString(fmt.Sprintf("    n%d [label=%q, shape=%s];\n", n.ID, n.Name, shape))
	}

	for fromID, targets := range plan.Adjacency {
		for _, toID := range targets {
			sb.WriteString(fmt.Sprintf("    n%d -> n%d;\n", fromID, toID))
		}
	}

	sb.WriteString("}\n")
	return sb.String()
}

// HTTPHandler mounts a debug introspection endpoint for plans on the framework.
func HTTPHandler(engine *runtime.Engine) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		intentName := c.Param("intent")
		if intentName == "" {
			intentName = c.Query("intent")
		}
		if intentName == "" {
			return c.Status(400).JSON(map[string]string{"error": "intent query or param required"})
		}

		plan, ok := engine.Plan(intent.Name(intentName))
		if !ok {
			return c.Status(404).JSON(map[string]string{"error": "intent plan not found"})
		}

		format := c.Query("format")
		switch format {
		case "mermaid":
			c.Set("Content-Type", "text/plain; charset=utf-8")
			return c.SendString(ToMermaid(plan))
		case "dot":
			c.Set("Content-Type", "text/vnd.graphviz")
			return c.SendString(ToDOT(plan))
		default:
			return c.JSON(InspectPlan(plan))
		}
	}
}
