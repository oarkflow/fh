package dagflow

import (
	"fmt"
	"html"
	"sort"
	"strings"
)

func (e *Engine) WorkflowSVG(workflowID string, nested bool) (string, error) {
	wf, err := e.workflow(workflowID)
	if err != nil {
		return "", err
	}
	if !nested {
		return e.workflowSVGSingle(wf, 0, 0, 0, 0)
	}
	flows, err := e.collectWorkflowTree(wf, map[string]bool{})
	if err != nil {
		return "", err
	}
	heights := make([]int, len(flows))
	width := 900
	total := 30
	for i, f := range flows {
		_, h := graphSize(f)
		heights[i] = h + 70
		if w, _ := graphSize(f); w > width {
			width = w
		}
		total += heights[i]
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">`, width, total, width, total))
	b.WriteString(svgDefs())
	y := 20
	for i, f := range flows {
		b.WriteString(fmt.Sprintf(`<g transform="translate(0,%d)">`, y))
		b.WriteString(renderWorkflowBody(f, fmt.Sprintf("%d", i+1)))
		b.WriteString(`</g>`)
		y += heights[i]
	}
	b.WriteString(`</svg>`)
	return b.String(), nil
}

func (e *Engine) collectWorkflowTree(wf *Workflow, seen map[string]bool) ([]*Workflow, error) {
	if seen[wf.ID] {
		return nil, fmt.Errorf("recursive nested workflow reference at %s", wf.ID)
	}
	seen[wf.ID] = true
	out := []*Workflow{wf}
	nodeIDs := make([]string, 0, len(wf.Nodes))
	for id := range wf.Nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)
	for _, id := range nodeIDs {
		n := wf.Nodes[id]
		if n.Type != NodeWorkflow {
			continue
		}
		child, err := e.workflow(n.Workflow)
		if err != nil {
			return nil, err
		}
		items, err := e.collectWorkflowTree(child, seen)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
	}
	return out, nil
}

func (e *Engine) workflowSVGSingle(wf *Workflow, _ int, _ int, _ int, _ int) (string, error) {
	w, h := graphSize(wf)
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">`, w, h, w, h))
	b.WriteString(svgDefs())
	b.WriteString(renderWorkflowBody(wf, ""))
	b.WriteString(`</svg>`)
	return b.String(), nil
}

func svgDefs() string {
	return `<defs><marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="#333"/></marker></defs><style>text{font-family:Arial,Helvetica,sans-serif}.title{font-size:18px;font-weight:bold}.node{fill:#fff;stroke:#333;stroke-width:1.5}.workflow{fill:#f7fbff;stroke:#06c;stroke-width:2}.first{stroke:#0a7;stroke-width:3}.last{stroke:#a50;stroke-width:3}.edge{stroke:#333;stroke-width:1.4;fill:none;marker-end:url(#arrow)}.branch{stroke-dasharray:6 4}.iterator{stroke-dasharray:2 4}.error{stroke:#b00;stroke-dasharray:8 4}.small{font-size:11px;fill:#555}.label{font-size:12px;fill:#111}</style>`
}

func renderWorkflowBody(wf *Workflow, ordinal string) string {
	levels := layoutLevels(wf)
	pos := map[string][2]int{}
	maxLevel := 0
	for nodeID, level := range levels {
		if level > maxLevel {
			maxLevel = level
		}
		idx := countLevelBefore(levels, nodeID, level)
		pos[nodeID] = [2]int{80 + level*240, 90 + idx*120}
	}
	for id := range wf.Nodes {
		if _, ok := pos[id]; !ok {
			pos[id] = [2]int{80 + (maxLevel+1)*240, 90}
		}
	}
	var b strings.Builder
	title := "Workflow: " + wf.ID
	if ordinal != "" {
		title = "Nested " + ordinal + " — " + title
	}
	b.WriteString(fmt.Sprintf(`<text x="24" y="32" class="title">%s</text>`, esc(title)))
	b.WriteString(fmt.Sprintf(`<text x="24" y="52" class="small">%s %s</text>`, esc(wf.Name), esc(wf.Version)))
	for _, edge := range wf.Edges {
		for _, fromID := range edge.Sources {
			for _, toID := range edge.Targets {
				from, fok := pos[fromID]
				to, tok := pos[toID]
				if !fok || !tok {
					continue
				}
				cls := "edge"
				if edge.Type == EdgeBranch {
					cls += " branch"
				}
				if edge.Type == EdgeIterator {
					cls += " iterator"
				}
				if edge.Type == EdgeError {
					cls += " error"
				}
				x1, y1 := from[0]+170, from[1]+30
				x2, y2 := to[0], to[1]+30
				mx := (x1 + x2) / 2
				b.WriteString(fmt.Sprintf(`<path class="%s" d="M %d %d C %d %d, %d %d, %d %d"/>`, cls, x1, y1, mx, y1, mx, y2, x2, y2))
				b.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="small">%s/%s</text>`, mx-35, (y1+y2)/2-5, esc(edge.ID), esc(string(edge.Type))))
			}
		}
	}
	nodeIDs := make([]string, 0, len(wf.Nodes))
	for id := range wf.Nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)
	for _, id := range nodeIDs {
		n := wf.Nodes[id]
		p := pos[id]
		cls := "node"
		if n.Type == NodeWorkflow {
			cls += " workflow"
		}
		if id == wf.First {
			cls += " first"
		}
		if n.Last {
			cls += " last"
		}
		b.WriteString(fmt.Sprintf(`<rect class="%s" x="%d" y="%d" width="170" height="64" rx="10"/>`, cls, p[0], p[1]))
		b.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="label">%s</text>`, p[0]+10, p[1]+21, esc(id)))
		b.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="small">type=%s mode=%s</text>`, p[0]+10, p[1]+40, esc(string(n.Type)), esc(string(n.Mode))))
		if n.Type == NodeWorkflow {
			b.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="small">workflow=%s</text>`, p[0]+10, p[1]+56, esc(n.Workflow)))
		}
	}
	return b.String()
}

func graphSize(wf *Workflow) (int, int) {
	levels := layoutLevels(wf)
	maxLevel, levelCount := 0, map[int]int{}
	for _, l := range levels {
		if l > maxLevel {
			maxLevel = l
		}
		levelCount[l]++
	}
	maxRows := 1
	for _, n := range levelCount {
		if n > maxRows {
			maxRows = n
		}
	}
	return 320 + (maxLevel+1)*240, 130 + maxRows*120
}

func layoutLevels(wf *Workflow) map[string]int {
	levels := map[string]int{wf.First: 0}
	queue := []string{wf.First}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		base := levels[id]
		for _, e := range wf.Outgoing[id] {
			for _, to := range e.Targets {
				if old, ok := levels[to]; !ok || old < base+1 {
					levels[to] = base + 1
					queue = append(queue, to)
				}
			}
		}
	}
	return levels
}

func countLevelBefore(levels map[string]int, nodeID string, level int) int {
	ids := make([]string, 0)
	for id, l := range levels {
		if l == level {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for i, id := range ids {
		if id == nodeID {
			return i
		}
	}
	return 0
}

func esc(s string) string { return html.EscapeString(s) }

func (e *Engine) TaskSVG(taskID string) (string, error) {
	task, err := e.store.Get(taskID)
	if err != nil {
		return "", err
	}
	wf, err := e.workflow(task.WorkflowID)
	if err != nil {
		return "", err
	}
	w, h := graphSize(wf)
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">`, w, h, w, h))
	b.WriteString(svgDefs())
	b.WriteString(`<style>.completed{fill:#e9fbe9;stroke:#168a16}.failed{fill:#ffe8e8;stroke:#bb1515}.running{fill:#e7f0ff;stroke:#1f5fbf}.waiting{fill:#fff7d6;stroke:#b88a00}.skipped{fill:#f2e8ff;stroke:#7a3db8}</style>`)
	b.WriteString(renderTaskWorkflowBody(wf, task))
	b.WriteString(`</svg>`)
	return b.String(), nil
}

func renderTaskWorkflowBody(wf *Workflow, task *Task) string {
	levels := layoutLevels(wf)
	pos := map[string][2]int{}
	maxLevel := 0
	for _, level := range levels {
		if level > maxLevel {
			maxLevel = level
		}
	}
	for nodeID, level := range levels {
		idx := countLevelBefore(levels, nodeID, level)
		pos[nodeID] = [2]int{80 + level*240, 90 + idx*120}
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<text x="24" y="32" class="title">Task: %s / %s</text>`, esc(task.ID), esc(task.Status.String())))
	b.WriteString(fmt.Sprintf(`<text x="24" y="52" class="small">workflow=%s current=%s last_error=%s</text>`, esc(task.WorkflowID), esc(task.CurrentNode), esc(task.LastError)))
	for _, edge := range wf.Edges {
		for _, fromID := range edge.Sources {
			for _, toID := range edge.Targets {
				from, fok := pos[fromID]
				to, tok := pos[toID]
				if !fok || !tok {
					continue
				}
				x1, y1 := from[0]+170, from[1]+30
				x2, y2 := to[0], to[1]+30
				mx := (x1 + x2) / 2
				cls := "edge"
				if edge.Type == EdgeBranch {
					cls += " branch"
				}
				if edge.Type == EdgeIterator {
					cls += " iterator"
				}
				if edge.Type == EdgeError {
					cls += " error"
				}
				b.WriteString(fmt.Sprintf(`<path class="%s" d="M %d %d C %d %d, %d %d, %d %d"/>`, cls, x1, y1, mx, y1, mx, y2, x2, y2))
				b.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="small">%s/%s</text>`, mx-35, (y1+y2)/2-5, esc(edge.ID), esc(string(edge.Type))))
			}
		}
	}
	nodeIDs := make([]string, 0, len(wf.Nodes))
	for id := range wf.Nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)
	for _, id := range nodeIDs {
		n := wf.Nodes[id]
		p := pos[id]
		cls := "node"
		if n.Type == NodeWorkflow {
			cls += " workflow"
		}
		if id == wf.First {
			cls += " first"
		}
		if n.Last {
			cls += " last"
		}
		if st := task.NodeStates[id]; st != nil {
			cls += " " + string(st.Status)
		}
		b.WriteString(fmt.Sprintf(`<rect class="%s" x="%d" y="%d" width="170" height="64" rx="10"/>`, cls, p[0], p[1]))
		b.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="label">%s</text>`, p[0]+10, p[1]+21, esc(id)))
		status := "pending"
		attempts := 0
		dur := ""
		if st := task.NodeStates[id]; st != nil {
			status = string(st.Status)
			attempts = st.Attempts
			dur = st.Duration.String()
		}
		b.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="small">%s attempts=%d</text>`, p[0]+10, p[1]+40, esc(status), attempts))
		b.WriteString(fmt.Sprintf(`<text x="%d" y="%d" class="small">%s</text>`, p[0]+10, p[1]+56, esc(dur)))
	}
	return b.String()
}

func (s TaskStatus) String() string { return string(s) }
