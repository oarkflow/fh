package dagflow

import "time"

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
	for _, st := range task.JoinStates {
		ensureJoinStateRuntimeState(st)
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	if task.UpdatedAt.IsZero() {
		task.UpdatedAt = task.CreatedAt
	}
}

func ensureJoinStateRuntimeState(st *JoinState) {
	if st == nil {
		return
	}
	if st.CompletedSources == nil {
		st.CompletedSources = map[string]bool{}
	}
	if st.Results == nil {
		st.Results = map[string]any{}
	}
	if st.Errors == nil {
		st.Errors = map[string]string{}
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
	for _, task := range run.Tasks {
		ensureTaskRuntimeState(task)
	}
	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now()
	}
	if run.UpdatedAt.IsZero() {
		run.UpdatedAt = run.CreatedAt
	}
}
