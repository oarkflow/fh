package dagflow

import (
	"context"
	"testing"
	"time"
)

func TestTaskRuleRejectIsPermanentAndSetsRejected(t *testing.T) {
	e := NewEngine(NewMemoryTaskStore(), NewMemoryChainStore(), NewMemoryBroker())
	if err := e.AddCondition(ConditionSpec{Name: "blocked", All: []string{`node.id == "validate"`, `input.to == "blocked@blocked.test"`}}); err != nil {
		t.Fatal(err)
	}
	wf := &Workflow{ID: "wf", First: "validate", Nodes: map[string]*Node{"validate": {ID: "validate", Type: NodeFunction, Handler: "noop"}}, Rules: []TaskRule{{ID: "reject", Events: []string{"node.before"}, Condition: "blocked", Action: TaskActionConfig{Type: ActionReject, Reason: "blocked"}}}}
	task := newTask("wf", map[string]any{"to": "blocked@blocked.test"})
	err := e.applyTaskRules(context.Background(), wf, task, wf.Nodes["validate"], "node.before", task.Input)
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !IsPermanentError(err) {
		t.Fatalf("expected permanent rejection, got %T %v", err, err)
	}
	if task.Status != TaskRejected {
		t.Fatalf("expected rejected status, got %s", task.Status)
	}
}

func TestTaskRuleNamedConditionDoesNotMatch(t *testing.T) {
	e := NewEngine(NewMemoryTaskStore(), NewMemoryChainStore(), NewMemoryBroker())
	if err := e.AddCondition(ConditionSpec{Name: "blocked", All: []string{`node.id == "validate"`, `input.to == "blocked@blocked.test"`}}); err != nil {
		t.Fatal(err)
	}
	wf := &Workflow{ID: "wf", First: "validate", Nodes: map[string]*Node{"validate": {ID: "validate", Type: NodeFunction, Handler: "noop"}}, Rules: []TaskRule{{ID: "reject", Events: []string{"node.before"}, Condition: "blocked", Action: TaskActionConfig{Type: ActionReject, Reason: "blocked"}}}}
	task := newTask("wf", map[string]any{"to": "user@example.com"})
	if err := e.applyTaskRules(context.Background(), wf, task, wf.Nodes["validate"], "node.before", task.Input); err != nil {
		t.Fatalf("did not expect rejection: %v", err)
	}
}

func TestMemoryBrokerQueuePauseResumePurge(t *testing.T) {
	b := NewMemoryBroker()
	if err := b.EnsureQueue(QueueConfig{ID: "q", Capacity: 8}); err != nil {
		t.Fatal(err)
	}
	if err := b.PauseQueue("q"); err != nil {
		t.Fatal(err)
	}
	if queues := b.ListQueues(); len(queues) != 1 || !queues[0].Paused {
		t.Fatalf("expected paused queue, got %+v", queues)
	}
	if err := b.PublishToQueue(context.Background(), "q", Job{ID: "j1", WorkflowID: "wf", TaskID: "t", Attempt: 1}); err != nil {
		t.Fatal(err)
	}
	purged, err := b.PurgeQueue("q")
	if err != nil {
		t.Fatal(err)
	}
	if purged != 1 {
		t.Fatalf("expected 1 purged, got %d", purged)
	}
	if err := b.ResumeQueue("q"); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryBrokerWaitResult(t *testing.T) {
	b := NewMemoryBroker()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		_ = b.Complete(context.Background(), JobResult{JobID: "j", Queue: "q", Status: "succeeded"})
	}()
	jr, err := b.WaitResult(ctx, "j")
	if err != nil {
		t.Fatal(err)
	}
	if jr.Status != "succeeded" {
		t.Fatalf("unexpected result: %+v", jr)
	}
}
