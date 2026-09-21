package execution_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/invocation"
)

func TestBudgetEnforcement(t *testing.T) {
	b := execution.NewBudget(50*time.Millisecond, 2, 1, 1024, 1)

	// Acquire DB queries
	if err := b.AcquireDBQuery(1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := b.AcquireDBQuery(1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := b.AcquireDBQuery(1); !errors.Is(err, execution.ErrBudgetExhausted) {
		t.Fatalf("expected ErrBudgetExhausted, got %v", err)
	}

	snap := b.Snapshot()
	if snap.DBQueries != 3 {
		t.Errorf("expected 3 attempted DB queries, got %d", snap.DBQueries)
	}

	// Wait for deadline expiry
	time.Sleep(60 * time.Millisecond)
	if err := b.AcquireExternalIO(1); !errors.Is(err, execution.ErrBudgetTimeout) {
		t.Fatalf("expected ErrBudgetTimeout, got %v", err)
	}
}

func TestDecisionComposition(t *testing.T) {
	ds := execution.NewDecisionSet()
	if ds.Verdict() != execution.VerdictAllow {
		// With 0 required, empty decision set allows
		t.Errorf("expected initial verdict allow when 0 required")
	}

	dsReq := execution.NewDecisionSet(2)
	if dsReq.Verdict() != execution.VerdictPending {
		t.Errorf("expected initial verdict pending when 2 required")
	}

	// Policy 1 allows with tenant constraint
	dsReq.RecordAllow("tenant_check", []execution.Constraint{
		{Field: "tenant_id", Values: []string{"org-1"}},
		{Field: "region", Values: []string{"us-east-1", "eu-west-1"}},
	}, execution.Obligation{Name: "audit_log", Action: "write"})

	if dsReq.Verdict() != execution.VerdictPending {
		t.Errorf("expected verdict to still be pending after only 1 of 2 allows")
	}

	// Policy 2 intersects regions and completes requirement
	dsReq.RecordAllow("geo_policy", []execution.Constraint{
		{Field: "region", Values: []string{"us-east-1", "ap-southeast-1"}},
	})

	if dsReq.Verdict() != execution.VerdictAllow {
		t.Errorf("expected allow verdict after both required policies allowed")
	}

	cs := dsReq.Constraints()
	if cs.TenantID != "org-1" {
		t.Errorf("expected tenant_id org-1, got %s", cs.TenantID)
	}
	if len(cs.Regions) != 1 || cs.Regions[0] != "us-east-1" {
		t.Errorf("expected region intersection [us-east-1], got %v", cs.Regions)
	}
	if len(dsReq.Obligations()) != 1 {
		t.Errorf("expected 1 obligation, got %d", len(dsReq.Obligations()))
	}

	// Policy 3 denies -> DENY dominates
	dsReq.RecordDeny("fraud_check", "high risk score")
	if dsReq.Verdict() != execution.VerdictDeny {
		t.Errorf("expected verdict DENY to dominate")
	}

	// Subsequent allow cannot override deny
	dsReq.RecordAllow("another_check", nil)
	if dsReq.Verdict() != execution.VerdictDeny {
		t.Errorf("expected verdict DENY to remain dominant")
	}
}

func TestSchedulerExecution(t *testing.T) {
	// Node 0: Auth -> produces fact slot 0
	// Node 1: Tenant -> produces fact slot 1 (requires 0)
	// Node 2: Decision (Allow)
	// Node 3: Operation -> requires 1, records effect
	var opExecuted atomic.Bool
	var fxExecuted atomic.Bool

	nodes := []*graph.Node{
		{
			ID:       0,
			Name:     "Auth",
			Kind:     graph.PureNode,
			Provides: []fact.PlanSlot{0},
		},
		{
			ID:       1,
			Name:     "Tenant",
			Kind:     graph.ReadNode,
			Requires: []fact.PlanSlot{0},
			Provides: []fact.PlanSlot{1},
		},
		{
			ID:   2,
			Name: "Policy",
			Kind: graph.DecisionNode,
		},
		{
			ID:       3,
			Name:     "Operation",
			Kind:     graph.OperationNode,
			Requires: []fact.PlanSlot{1},
		},
		{
			ID:   4,
			Name: "EffectNode",
			Kind: graph.EffectNode,
		},
	}

	runners := []execution.NodeExecutor{
		func(nc *execution.NodeContext) error {
			execution.PublishFact(nc, 0, "user-42")
			return nil
		},
		func(nc *execution.NodeContext) error {
			u, err := execution.RequireFact[string](nc, 0)
			if err != nil || u != "user-42" {
				return errors.New("missing user fact")
			}
			execution.PublishFact(nc, 1, "tenant-abc")
			return nil
		},
		func(nc *execution.NodeContext) error {
			nc.Decisions().RecordAllow("allow_all", nil)
			return nil
		},
		func(nc *execution.NodeContext) error {
			nc.RecordEffect("send-welcome-email")
			opExecuted.Store(true)
			return nil
		},
		func(nc *execution.NodeContext) error {
			fxExecuted.Store(true)
			return nil
		},
	}

	g, err := graph.Build(nodes, 2)
	if err != nil {
		t.Fatalf("graph build failed: %v", err)
	}

	plan, err := graph.Compile(g, "CreateTenantUser", 1)
	if err != nil {
		t.Fatalf("graph compile failed: %v", err)
	}

	sched := execution.NewScheduler()
	inv := &invocation.Invocation{
		ID:     "inv-1",
		Intent: "CreateTenantUser",
		Input:  invocation.NewInput([]byte("{}"), "application/json"),
	}

	budget := execution.NewBudget(time.Second, 10, 10, 1024, 10)
	outcome, err := sched.Execute(context.Background(), inv, plan, runners, budget)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	if outcome.State != execution.StateCompleted {
		t.Errorf("expected StateCompleted, got %v", outcome.State)
	}
	if !opExecuted.Load() {
		t.Errorf("expected operation to execute")
	}
	if !fxExecuted.Load() {
		t.Errorf("expected effect node to execute")
	}
	if len(outcome.Effects) != 1 || outcome.Effects[0] != "send-welcome-email" {
		t.Errorf("expected recorded effect, got %v", outcome.Effects)
	}
}

func TestSchedulerDenyDominance(t *testing.T) {
	var opExecuted atomic.Bool

	nodes := []*graph.Node{
		{
			ID:   0,
			Name: "AuthDeny",
			Kind: graph.DecisionNode,
		},
		{
			ID:   1,
			Name: "Operation",
			Kind: graph.OperationNode,
		},
	}

	runners := []execution.NodeExecutor{
		func(nc *execution.NodeContext) error {
			nc.Decisions().RecordDeny("auth", "invalid credentials")
			return nil
		},
		func(nc *execution.NodeContext) error {
			opExecuted.Store(true)
			return nil
		},
	}

	g, err := graph.Build(nodes, 0)
	if err != nil {
		t.Fatalf("graph build error: %v", err)
	}

	plan, err := graph.Compile(g, "SecureIntent", 1)
	if err != nil {
		t.Fatalf("graph compile error: %v", err)
	}

	sched := execution.NewScheduler()
	inv := &invocation.Invocation{Intent: "SecureIntent"}

	outcome, err := sched.Execute(context.Background(), inv, plan, runners, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if outcome.State != execution.StateDenied {
		t.Errorf("expected StateDenied, got %v", outcome.State)
	}
	if opExecuted.Load() {
		t.Errorf("operation should NOT execute when policy denies")
	}
}

func TestSchedulerShortCircuit(t *testing.T) {
	var opExecuted atomic.Bool

	nodes := []*graph.Node{
		{
			ID:   0,
			Name: "CacheLookup",
			Kind: graph.PureNode,
		},
		{
			ID:   1,
			Name: "SlowOperation",
			Kind: graph.OperationNode,
		},
	}

	runners := []execution.NodeExecutor{
		func(nc *execution.NodeContext) error {
			nc.SetShortCircuit("cached-response-data")
			return nil
		},
		func(nc *execution.NodeContext) error {
			select {
			case <-nc.Done():
				return nc.Err()
			case <-time.After(100 * time.Millisecond):
			}
			opExecuted.Store(true)
			return nil
		},
	}

	g, err := graph.Build(nodes, 0)
	if err != nil {
		t.Fatalf("graph build error: %v", err)
	}

	plan, err := graph.Compile(g, "CacheableIntent", 1)
	if err != nil {
		t.Fatalf("graph compile error: %v", err)
	}

	sched := execution.NewScheduler()
	inv := &invocation.Invocation{Intent: "CacheableIntent"}

	outcome, err := sched.Execute(context.Background(), inv, plan, runners, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if outcome.State != execution.StateShortCircuited {
		t.Errorf("expected StateShortCircuited, got %v", outcome.State)
	}
	if outcome.Value != "cached-response-data" {
		t.Errorf("expected cached value, got %v", outcome.Value)
	}
	if opExecuted.Load() {
		t.Errorf("slow operation should have been cancelled by short circuit")
	}
}

func TestSchedulerPanicRecovery(t *testing.T) {
	nodes := []*graph.Node{
		{
			ID:   0,
			Name: "PanickingNode",
			Kind: graph.PureNode,
		},
	}

	runners := []execution.NodeExecutor{
		func(nc *execution.NodeContext) error {
			panic("critical calculation failure")
		},
	}

	g, err := graph.Build(nodes, 0)
	if err != nil {
		t.Fatalf("build error: %v", err)
	}
	plan, err := graph.Compile(g, "PanicIntent", 1)
	if err != nil {
		t.Fatalf("compile error: %v", err)
	}

	sched := execution.NewScheduler()
	outcome, err := sched.Execute(context.Background(), &invocation.Invocation{Intent: "PanicIntent"}, plan, runners, nil)

	if err == nil {
		t.Fatalf("expected error from panic recovery, got nil")
	}
	if outcome.State != execution.StateFailed {
		t.Errorf("expected StateFailed, got %v", outcome.State)
	}
}
