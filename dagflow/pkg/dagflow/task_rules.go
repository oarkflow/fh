package dagflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type TaskActionType string

type ApprovalStatus string

const (
	ActionNotify          TaskActionType = "notify"
	ActionReject          TaskActionType = "reject"
	ActionApprove         TaskActionType = "approve"
	ActionRequireApproval TaskActionType = "require_approval"
	ActionPause           TaskActionType = "pause"
	ActionCancel          TaskActionType = "cancel"

	ApprovalPending  ApprovalStatus = "pending"
	ApprovalApproved ApprovalStatus = "approved"
	ApprovalRejected ApprovalStatus = "rejected"
)

type TaskRule struct {
	ID        string           `json:"id" bcl:",id"`
	Enabled   *bool            `json:"enabled,omitempty" bcl:"enabled,omitempty"`
	Events    []string         `json:"events,omitempty" bcl:"events,omitempty"`
	When      string           `json:"when,omitempty" bcl:"when,omitempty"`
	Condition string           `json:"condition,omitempty" bcl:"condition,omitempty"`
	Action    TaskActionConfig `json:"action" bcl:"action,block"`
	Message   string           `json:"message,omitempty" bcl:"message,omitempty"`
	Data      DataSpec         `json:"data,omitempty" bcl:"data,block,omitempty"`
}

type TaskActionConfig struct {
	Type      TaskActionType `json:"type" bcl:"type,ident"`
	Channels  []string       `json:"channels,omitempty" bcl:"channels,omitempty"`
	Reason    string         `json:"reason,omitempty" bcl:"reason,omitempty"`
	Mode      string         `json:"mode,omitempty" bcl:"mode,ident,omitempty"`
	Approvers []string       `json:"approvers,omitempty" bcl:"approvers,omitempty"`
	Timeout   string         `json:"timeout,omitempty" bcl:"timeout,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty" bcl:"metadata,omitempty"`
}

type ApprovalRequest struct {
	ID          string             `json:"id"`
	TaskID      string             `json:"task_id"`
	WorkflowID  string             `json:"workflow_id"`
	NodeID      string             `json:"node_id,omitempty"`
	RuleID      string             `json:"rule_id,omitempty"`
	Mode        string             `json:"mode,omitempty"`
	Status      ApprovalStatus     `json:"status"`
	Reason      string             `json:"reason,omitempty"`
	Input       any                `json:"input,omitempty"`
	Approvers   []string           `json:"approvers,omitempty"`
	Decisions   []ApprovalDecision `json:"decisions,omitempty"`
	ResumeQueue []RunItem          `json:"resume_queue,omitempty"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
	ExpiresAt   *time.Time         `json:"expires_at,omitempty"`
}

type ApprovalDecision struct {
	Approver string         `json:"approver,omitempty"`
	Status   ApprovalStatus `json:"status"`
	Reason   string         `json:"reason,omitempty"`
	At       time.Time      `json:"at"`
}

type ApprovalStore interface {
	SaveApproval(ApprovalRequest) error
	GetApproval(id string) (*ApprovalRequest, error)
	ListApprovals(status ApprovalStatus) []ApprovalRequest
}

var ErrApprovalRequired = errors.New("approval required")
var ErrTaskRejected = errors.New("task rejected")

type TaskRejectedError struct {
	Reason string
}

func (e TaskRejectedError) Error() string {
	if e.Reason == "" {
		return ErrTaskRejected.Error()
	}
	return ErrTaskRejected.Error() + ": " + e.Reason
}

func (e TaskRejectedError) Unwrap() error { return ErrTaskRejected }

func IsTaskRejectedError(err error) bool {
	var rejected TaskRejectedError
	return errors.As(err, &rejected) || errors.Is(err, ErrTaskRejected)
}

type PermanentError struct {
	Err error
}

func (e PermanentError) Error() string {
	if e.Err == nil {
		return "permanent error"
	}
	return e.Err.Error()
}

func (e PermanentError) Unwrap() error { return e.Err }

func NewPermanentError(format string, args ...any) error {
	return PermanentError{Err: fmt.Errorf(format, args...)}
}

func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}
	var pe PermanentError
	if errors.As(err, &pe) {
		return true
	}
	return IsTaskRejectedError(err) || isPermanentErrorText(err.Error())
}

func isPermanentErrorText(s string) bool {
	s = strings.ToLower(s)
	return strings.Contains(s, "task rejected") ||
		strings.Contains(s, "invalid condition shape") ||
		strings.Contains(s, "missing when/condition expression") ||
		strings.Contains(s, "did not return a boolean") ||
		strings.Contains(s, "condition must be a string expression") ||
		strings.Contains(s, "condition ") && strings.Contains(s, " not found") ||
		strings.Contains(s, "unsupported action") ||
		strings.Contains(s, "schema") && strings.Contains(s, "not found")
}

func controlActionRequiresCondition(action TaskActionType) bool {
	switch action {
	case ActionReject, ActionApprove, ActionRequireApproval, ActionPause, ActionCancel:
		return true
	default:
		return false
	}
}

func (e *Engine) evalTaskRuleMatch(rule TaskRule, facts map[string]any) (bool, error) {
	condition := strings.TrimSpace(rule.Condition)
	when := normalizeBCLExpr(rule.When)
	if condition == "" && when == "" {
		if controlActionRequiresCondition(rule.Action.Type) {
			return false, NewPermanentError("missing when/condition expression for control action %q", rule.Action.Type)
		}
		return true, nil
	}
	return e.evalNamedOrInline(condition, when, facts)
}

func (e *Engine) applyTaskRules(ctx context.Context, wf *Workflow, task *Task, node *Node, event string, input any) error {
	for _, rule := range collectTaskRules(wf, node) {
		if !ruleEnabled(rule.Enabled) || !matchesEvent(rule.Events, event) {
			continue
		}
		facts := e.workflowFacts(task, node, input, map[string]any{"event": event})
		ok, err := e.evalTaskRuleMatch(rule, facts)
		if err != nil {
			return fmt.Errorf("task rule %s: %w", rule.ID, err)
		}
		if !ok {
			continue
		}
		payload := input
		if !rule.Data.Empty() {
			payload, err = e.applyData(ctx, rule.Data, &DataContext{Workflow: wf, Task: task, Node: node, Input: input, Result: input}, input)
			if err != nil {
				if errors.Is(err, ErrDataFiltered) {
					continue
				}
				return err
			}
		}
		switch rule.Action.Type {
		case "", ActionNotify:
			e.emitConfiguredNotification(ctx, wf, task, node, event, NotificationRule{ID: rule.ID, Channels: rule.Action.Channels, Message: firstNonEmpty(rule.Message, rule.Action.Reason), Severity: "info"}, payload)
		case ActionApprove:
			e.audit(task, "task.auto_approved", firstNonEmpty(rule.Message, rule.Action.Reason, "task auto approved"), map[string]any{"rule": rule.ID})
		case ActionReject:
			reason := firstNonEmpty(rule.Action.Reason, rule.Message, "task rejected by rule")
			task.Status = TaskFailed
			task.Error = reason
			task.LastError = reason
			e.audit(task, "task.rejected", reason, map[string]any{"rule": rule.ID, "input": payload})
			return TaskRejectedError{Reason: reason}
		case ActionPause:
			task.Status = TaskPaused
			task.Cursor = task.Cursor
			e.audit(task, "task.paused_by_rule", firstNonEmpty(rule.Message, rule.Action.Reason, "task paused by rule"), map[string]any{"rule": rule.ID})
			return ErrApprovalRequired
		case ActionCancel:
			task.Status = TaskCancelled
			now := time.Now()
			task.CompletedAt = &now
			e.audit(task, "task.cancelled_by_rule", firstNonEmpty(rule.Message, rule.Action.Reason, "task cancelled by rule"), map[string]any{"rule": rule.ID})
			return ErrApprovalRequired
		case ActionRequireApproval:
			if e.approvalAlreadyApproved(task.ID, rule.ID, safeNodeID(node)) {
				e.audit(task, "approval.already_approved", "approval already approved; continuing", map[string]any{"rule": rule.ID})
				continue
			}
			if err := e.createApproval(ctx, wf, task, node, rule, payload); err != nil {
				return err
			}
			return ErrApprovalRequired
		default:
			return fmt.Errorf("task rule %s unsupported action %q", rule.ID, rule.Action.Type)
		}
	}
	return nil
}

func (e *Engine) approvalAlreadyApproved(taskID, ruleID, nodeID string) bool {
	as, ok := e.store.(ApprovalStore)
	if !ok {
		return false
	}
	for _, a := range as.ListApprovals("") {
		if a.TaskID == taskID && a.RuleID == ruleID && a.NodeID == nodeID && a.Status == ApprovalApproved {
			return true
		}
	}
	return false
}
func collectTaskRules(wf *Workflow, node *Node) []TaskRule {
	var out []TaskRule
	if wf != nil {
		out = append(out, wf.Rules...)
	}
	if node != nil {
		out = append(out, node.Rules...)
	}
	return out
}
func ruleEnabled(v *bool) bool { return v == nil || *v }
func matchesEvent(events []string, event string) bool {
	if len(events) == 0 {
		return true
	}
	for _, ev := range events {
		if ev == event || ev == "*" || strings.HasSuffix(ev, ".*") && strings.HasPrefix(event, strings.TrimSuffix(ev, "*")) {
			return true
		}
	}
	return false
}

func (e *Engine) createApproval(ctx context.Context, wf *Workflow, task *Task, node *Node, rule TaskRule, input any) error {
	st, ok := e.store.(ApprovalStore)
	if !ok {
		return errors.New("approval requires store implementing ApprovalStore")
	}
	now := time.Now()
	ar := ApprovalRequest{ID: newID("approval"), TaskID: task.ID, WorkflowID: task.WorkflowID, RuleID: rule.ID, Status: ApprovalPending, Reason: firstNonEmpty(rule.Action.Reason, rule.Message, "approval required"), Input: input, Approvers: append([]string(nil), rule.Action.Approvers...), Mode: rule.Action.Mode, CreatedAt: now, UpdatedAt: now}
	if node != nil {
		ar.NodeID = node.ID
	}
	if ar.Mode == "" {
		ar.Mode = "single"
	}
	if rule.Action.Timeout != "" {
		if d, err := time.ParseDuration(rule.Action.Timeout); err == nil && d > 0 {
			exp := now.Add(d)
			ar.ExpiresAt = &exp
		}
	}
	ar.ResumeQueue = append([]RunItem(nil), task.Cursor...)
	if err := st.SaveApproval(ar); err != nil {
		return err
	}
	task.Status = TaskWaiting
	task.WaitingNodeID = ar.NodeID
	task.ResumeToken = SignResumeToken(task.ID, wf.ID, ar.NodeID, 24*time.Hour)
	task.Cursor = ar.ResumeQueue
	e.audit(task, "approval.required", ar.Reason, map[string]any{"approval_id": ar.ID, "rule": rule.ID, "mode": ar.Mode, "approvers": ar.Approvers})
	e.emitConfiguredNotification(ctx, wf, task, node, "approval.required", NotificationRule{ID: rule.ID, Channels: rule.Action.Channels, Title: "Approval required", Message: ar.Reason, Severity: "warning"}, map[string]any{"approval": ar, "input": input})
	_ = e.store.Save(task)
	return nil
}

func (e *Engine) ApproveTask(ctx context.Context, taskID, approver, reason string) (*Task, error) {
	return e.decideTaskApproval(ctx, taskID, approver, reason, ApprovalApproved)
}
func (e *Engine) RejectTask(ctx context.Context, taskID, approver, reason string) (*Task, error) {
	return e.decideTaskApproval(ctx, taskID, approver, reason, ApprovalRejected)
}

func (e *Engine) BulkApproveTasks(ctx context.Context, taskIDs []string, approver, reason string) ([]*Task, []error) {
	return e.bulkDecide(ctx, taskIDs, approver, reason, ApprovalApproved)
}
func (e *Engine) BulkRejectTasks(ctx context.Context, taskIDs []string, approver, reason string) ([]*Task, []error) {
	return e.bulkDecide(ctx, taskIDs, approver, reason, ApprovalRejected)
}
func (e *Engine) bulkDecide(ctx context.Context, taskIDs []string, approver, reason string, status ApprovalStatus) ([]*Task, []error) {
	tasks := make([]*Task, 0, len(taskIDs))
	errs := []error{}
	for _, id := range taskIDs {
		t, err := e.decideTaskApproval(ctx, id, approver, reason, status)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", id, err))
			continue
		}
		tasks = append(tasks, t)
	}
	return tasks, errs
}

func (e *Engine) decideTaskApproval(ctx context.Context, taskID, approver, reason string, decision ApprovalStatus) (*Task, error) {
	unlock := e.lockTask(taskID)
	defer unlock()
	as, ok := e.store.(ApprovalStore)
	if !ok {
		return nil, errors.New("approval store is not available")
	}
	var ar *ApprovalRequest
	for _, candidate := range as.ListApprovals(ApprovalPending) {
		if candidate.TaskID == taskID {
			cp := candidate
			ar = &cp
			break
		}
	}
	if ar == nil {
		return nil, fmt.Errorf("pending approval for task %s not found", taskID)
	}
	task, err := e.store.Get(taskID)
	if err != nil {
		return nil, err
	}
	wf, err := e.workflow(task.WorkflowID)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	ar.Decisions = append(ar.Decisions, ApprovalDecision{Approver: approver, Status: decision, Reason: reason, At: now})
	ar.UpdatedAt = now
	if decision == ApprovalRejected {
		ar.Status = ApprovalRejected
		_ = as.SaveApproval(*ar)
		task.Status = TaskFailed
		task.Error = firstNonEmpty(reason, ar.Reason, "approval rejected")
		task.LastError = task.Error
		e.audit(task, "approval.rejected", task.Error, map[string]any{"approval_id": ar.ID, "approver": approver})
		e.finishTask(task, errors.New(task.Error))
		return task, nil
	}
	ar.Status = ApprovalApproved
	_ = as.SaveApproval(*ar)
	task.Status = TaskRunning
	task.WaitingNodeID = ""
	task.ResumeToken = ""
	e.audit(task, "approval.approved", firstNonEmpty(reason, "approval approved"), map[string]any{"approval_id": ar.ID, "approver": approver})
	queue := ar.ResumeQueue
	if len(queue) == 0 && ar.NodeID != "" {
		if node := wf.Nodes[ar.NodeID]; node != nil {
			next, err := e.resolveEdges(ctx, wf, task, node, ar.Input, false)
			if err != nil {
				return nil, err
			}
			queue = next
		}
	}
	_ = e.store.Save(task)
	err = e.executeTask(ctx, wf, task, queue)
	if err == nil && task.Status != TaskWaiting && task.Status != TaskPaused && task.Status != TaskCancelled {
		task.Result, err = e.applyWorkflowOutput(ctx, wf, task, task.Result)
	}
	e.finishTask(task, err)
	return task, err
}
