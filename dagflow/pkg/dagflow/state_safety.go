package dagflow

import "time"

// ensureTaskRuntimeState restores all runtime maps/slices that may be nil after
// JSON decoding, partial store loading, or manually-created task values. Every
// hot execution path should call this before mutating task state.
func ensureTaskRuntimeState(task *Task) {
	if task == nil {
		return
	}
	if task.Status == "" {
		task.Status = TaskPending
	}
	if task.NodeStates == nil {
		task.NodeStates = map[string]*NodeState{}
	}
	if task.NodeResults == nil {
		task.NodeResults = map[string]any{}
	}
	if task.JoinStates == nil {
		task.JoinStates = map[string]*JoinState{}
	}
	if task.Visits == nil {
		task.Visits = map[string]int{}
	}
	if task.Audit == nil {
		task.Audit = []AuditEvent{}
	}
	if task.Errors == nil {
		task.Errors = []TaskError{}
	}
	if task.CurrentNodes == nil {
		task.CurrentNodes = []string{}
	}
	if task.PreviousNodes == nil {
		task.PreviousNodes = []string{}
	}
	if task.Cursor == nil {
		task.Cursor = []RunItem{}
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	if task.UpdatedAt.IsZero() {
		task.UpdatedAt = task.CreatedAt
	}
	for id, st := range task.NodeStates {
		if st == nil {
			task.NodeStates[id] = &NodeState{NodeID: id, Status: NodePending}
			continue
		}
		if st.NodeID == "" {
			st.NodeID = id
		}
		if st.Status == "" {
			st.Status = NodePending
		}
	}
	for id, js := range task.JoinStates {
		ensureJoinStateRuntime(id, js)
	}
}

func ensureJoinStateRuntime(id string, js *JoinState) {
	if js == nil {
		return
	}
	if js.EdgeID == "" {
		js.EdgeID = id
	}
	if js.CompletedSources == nil {
		js.CompletedSources = map[string]bool{}
	}
	if js.Results == nil {
		js.Results = map[string]any{}
	}
	if js.Errors == nil {
		js.Errors = map[string]string{}
	}
	if js.Sources == nil {
		js.Sources = []string{}
	}
}

func ensureChainRunRuntimeState(run *ChainRun) {
	if run == nil {
		return
	}
	if run.Status == "" {
		run.Status = TaskPending
	}
	if run.WorkflowIDs == nil {
		run.WorkflowIDs = []string{}
	}
	if run.Tasks == nil {
		run.Tasks = []*Task{}
	}
	if run.Audit == nil {
		run.Audit = []AuditEvent{}
	}
	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now()
	}
	if run.UpdatedAt.IsZero() {
		run.UpdatedAt = run.CreatedAt
	}
	for _, task := range run.Tasks {
		ensureTaskRuntimeState(task)
	}
}
