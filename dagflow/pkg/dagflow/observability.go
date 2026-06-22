package dagflow

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"time"
)

type TaskStatusCount struct {
	Status TaskStatus `json:"status"`
	Count  int        `json:"count"`
}

type WorkflowStatusCount struct {
	WorkflowID string             `json:"workflow_id"`
	Counts     map[TaskStatus]int `json:"counts"`
}

type RuntimeDiagnostics struct {
	Time             time.Time             `json:"time"`
	Uptime           string                `json:"uptime"`
	GoRoutines       int                   `json:"goroutines"`
	Workflows        int                   `json:"workflows"`
	Chains           int                   `json:"chains"`
	Handlers         int                   `json:"handlers"`
	Scripts          int                   `json:"scripts"`
	Tasks            int                   `json:"tasks"`
	TaskStatus       []TaskStatusCount     `json:"task_status"`
	WorkflowStatus   []WorkflowStatusCount `json:"workflow_status"`
	Queues           []QueueInfo           `json:"queues"`
	Consumers        []QueueConsumerInfo   `json:"consumers"`
	RecentBrokerLogs []BrokerEvent         `json:"recent_broker_events"`
	Metrics          MetricSnapshot        `json:"metrics"`
	Health           HealthReport          `json:"health"`
	Warnings         []string              `json:"warnings,omitempty"`
}

func (e *Engine) BrokerEvents(limit int) []BrokerEvent {
	if ob, ok := e.broker.(ObservableBroker); ok {
		return ob.BrokerEvents(limit)
	}
	return nil
}

func (e *Engine) BrokerDiagnostics(limit int) BrokerDiagnostics {
	queues := e.QueueInfo()
	consumers := e.ConsumerInfo()
	kind := fmt.Sprintf("%T", e.broker)
	return BrokerDiagnostics{Kind: kind, Queues: queues, Consumers: consumers, RecentEvents: e.BrokerEvents(limit), ConsumerCount: len(consumers), QueueCount: len(queues)}
}

func (e *Engine) Diagnostics(ctx context.Context, eventLimit int) RuntimeDiagnostics {
	now := time.Now()
	started := e.startedAt
	if started.IsZero() {
		started = now
	}
	tasks := e.store.List()
	statusCounts := map[TaskStatus]int{}
	workflowCounts := map[string]map[TaskStatus]int{}
	for _, t := range tasks {
		statusCounts[t.Status]++
		if workflowCounts[t.WorkflowID] == nil {
			workflowCounts[t.WorkflowID] = map[TaskStatus]int{}
		}
		workflowCounts[t.WorkflowID][t.Status]++
	}
	orderedStatuses := make([]TaskStatus, 0, len(statusCounts))
	for st := range statusCounts {
		orderedStatuses = append(orderedStatuses, st)
	}
	sort.Slice(orderedStatuses, func(i, j int) bool { return orderedStatuses[i] < orderedStatuses[j] })
	statusOut := make([]TaskStatusCount, 0, len(orderedStatuses))
	for _, st := range orderedStatuses {
		statusOut = append(statusOut, TaskStatusCount{Status: st, Count: statusCounts[st]})
	}
	workflowIDs := make([]string, 0, len(workflowCounts))
	for id := range workflowCounts {
		workflowIDs = append(workflowIDs, id)
	}
	sort.Strings(workflowIDs)
	workflowOut := make([]WorkflowStatusCount, 0, len(workflowIDs))
	for _, id := range workflowIDs {
		workflowOut = append(workflowOut, WorkflowStatusCount{WorkflowID: id, Counts: workflowCounts[id]})
	}
	e.mu.RLock()
	workflows := len(e.flows)
	chains := len(e.chains)
	handlers := len(e.handlers)
	scripts := len(e.scriptHandlers)
	e.mu.RUnlock()
	queues := e.QueueInfo()
	consumers := e.ConsumerInfo()
	warnings := make([]string, 0, 4)
	for _, q := range queues {
		if q.Depth > 0 && q.Consumers == 0 {
			warnings = append(warnings, fmt.Sprintf("queue %s has depth %d but no running consumer", q.ID, q.Depth))
		}
	}
	for _, c := range consumers {
		if c.Status == ConsumerRunning && !c.LastHeartbeat.IsZero() && now.Sub(c.LastHeartbeat) > 2*time.Minute {
			warnings = append(warnings, fmt.Sprintf("consumer %s heartbeat is stale: %s", c.ID, now.Sub(c.LastHeartbeat).Round(time.Second)))
		}
	}
	return RuntimeDiagnostics{Time: now, Uptime: now.Sub(started).String(), GoRoutines: runtime.NumGoroutine(), Workflows: workflows, Chains: chains, Handlers: handlers, Scripts: scripts, Tasks: len(tasks), TaskStatus: statusOut, WorkflowStatus: workflowOut, Queues: queues, Consumers: consumers, RecentBrokerLogs: e.BrokerEvents(eventLimit), Metrics: e.Metrics(), Health: e.Health(ctx), Warnings: warnings}
}
