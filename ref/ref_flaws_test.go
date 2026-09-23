package ref_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
)

type Flaw1TestIntent struct {
	kTenant fact.Key[string]
	kQuota  fact.Key[string]
}

func (Flaw1TestIntent) Name() intent.Name { return "flaw1.test" }
func (f Flaw1TestIntent) Spec() intent.Spec {
	return intent.Spec{
		Requires: []fact.AnyKey{capability.PrincipalKey.Any(), f.kTenant.Any(), f.kQuota.Any()},
	}
}
func (f Flaw1TestIntent) Run(nc *execution.NodeContext, in BenchInput) (intent.Outcome[string], error) {
	p, _ := execution.Require(nc, capability.PrincipalKey)
	t, _ := execution.Require(nc, f.kTenant)
	q, _ := execution.Require(nc, f.kQuota)
	return intent.Outcome[string]{Value: p.ID + ":" + t + ":" + q}, nil
}

type Flaw3TestIntent struct {
	kConfig fact.Key[string]
	ran     *atomic.Bool
}

func (Flaw3TestIntent) Name() intent.Name { return "flaw3.test" }
func (f Flaw3TestIntent) Spec() intent.Spec {
	return intent.Spec{
		Requires: []fact.AnyKey{capability.PrincipalKey.Any(), f.kConfig.Any()},
	}
}
func (f Flaw3TestIntent) Run(nc *execution.NodeContext, in BenchInput) (intent.Outcome[string], error) {
	if f.ran != nil {
		f.ran.Store(true)
	}
	return intent.Outcome[string]{Value: "sensitive-data"}, nil
}

// ============================================================================
// TEST 1: Pipeline Tyranny — Concurrent DAG vs Sequential Pipeline
// ============================================================================
func TestFlaw1_PipelineTyranny_ParityAndConcurrency(t *testing.T) {
	const stepDelay = 5 * time.Millisecond

	// Traditional Sequential Pipeline
	seqStart := time.Now()
	time.Sleep(stepDelay) // Step 1: Auth
	p := "usr-1"
	time.Sleep(stepDelay) // Step 2: Tenant
	ten := "tenant-alpha"
	time.Sleep(stepDelay) // Step 3: Quota
	q := "allowed"
	traditionalResult := p + ":" + ten + ":" + q
	seqDuration := time.Since(seqStart)

	if seqDuration < 3*stepDelay {
		t.Fatalf("expected traditional sequential pipeline to take at least %v, took %v", 3*stepDelay, seqDuration)
	}

	// REF Concurrent DAG
	kTenant := fact.NewKey[string]("test.flaw1.tenant")
	kQuota := fact.NewKey[string]("test.flaw1.quota")

	authCap := capability.NewAuthCapability("auth.flaw1", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
		time.Sleep(stepDelay)
		return capability.PrincipalFact{ID: "usr-1"}, nil
	})

	pre := capability.WithSpeculation(graph.PreAuthSafe)
	tenantCap := capability.Read("read.tenant", pre).WithProvides(kTenant.Any()).WithRun(func(nc *execution.NodeContext) error {
		time.Sleep(stepDelay)
		execution.Publish(nc, kTenant, "tenant-alpha")
		return nil
	})
	quotaCap := capability.Read("read.quota", pre).WithProvides(kQuota.Any()).WithRun(func(nc *execution.NodeContext) error {
		time.Sleep(stepDelay)
		execution.Publish(nc, kQuota, "allowed")
		return nil
	})

	it := Flaw1TestIntent{kTenant: kTenant, kQuota: kQuota}
	engine := ref.NewEngine(ref.WithCapability(authCap), ref.WithCapability(tenantCap), ref.WithCapability(quotaCap))

	if err := ref.Register(engine, it); err != nil {
		t.Fatal(err)
	}
	if err := engine.Compile(); err != nil {
		t.Fatal(err)
	}

	refStart := time.Now()
	inv := &invocation.Invocation{
		ID:        "flaw1-test",
		Intent:    "flaw1.test",
		Principal: invocation.PrincipalHint{BearerToken: "token"},
	}
	res, err := engine.Dispatch(context.Background(), inv)
	if err != nil {
		t.Fatal(err)
	}
	refDuration := time.Since(refStart)

	refResult, ok := res.Value.(string)
	if !ok || refResult != traditionalResult {
		t.Fatalf("result mismatch: traditional=%q, ref=%q", traditionalResult, refResult)
	}

	// REF DAG overlaps all 3 waits concurrently: elapsed should be ~stepDelay + small overhead,
	// strictly less than the sequential sum of waits.
	t.Logf("Flaw 1 Latency: Traditional=%v, REF=%v (Speedup: %.2fx)", seqDuration, refDuration, float64(seqDuration)/float64(refDuration))
	if refDuration >= seqDuration {
		t.Fatalf("REF DAG (%v) was expected to be faster than sequential pipeline (%v)", refDuration, seqDuration)
	}
}

// ============================================================================
// TEST 2: Context Passing — Compile-Time Type Safety vs Untyped String Maps
// ============================================================================
func TestFlaw2_ContextTypeSafetyAndMissingKey(t *testing.T) {
	// Traditional Flaw A: Typos in string keys cause silent missing values or runtime panics
	traditionalMap := map[string]any{
		"user_id": "usr-123",
	}
	// Developer typo: "userid" instead of "user_id"
	val := traditionalMap["userid"]
	if val != nil {
		t.Fatalf("expected nil for misspelled key")
	}

	// Traditional Flaw B: Type assertion panic
	traditionalMap["tenant_id"] = 12345 // Stored int instead of string
	assertPanics(t, func() {
		_ = traditionalMap["tenant_id"].(string) // PANIC: interface conversion
	})

	// REF Solution: Typed Fact Keys provide compile-time safety and graceful missing-fact handling
	keyTenant := fact.NewKey[string]("test.flaw2.tenant")
	_ = keyTenant.Name
	store := fact.AcquireStore(2)
	defer fact.ReleaseStore(store)

	// Missing fact check: returns false, zero-value without panicking
	got, ok := fact.Get[string](store, 0)
	if ok || got != "" {
		t.Fatalf("expected missing fact to return ok=false, got ok=%v, val=%v", ok, got)
	}

	// Type-safe put and get
	fact.Put(store, 0, "tenant-safe")
	got, ok = fact.Get[string](store, 0)
	if !ok || got != "tenant-safe" {
		t.Fatalf("expected fact %q, got %q", "tenant-safe", got)
	}
}

// ============================================================================
// TEST 3: Speculation Dilemma — PreAuthSafe Speculation and Cancellation
// ============================================================================
func TestFlaw3_SpeculativeExecutionAndSecurityCancellation(t *testing.T) {
	var sensitiveExecuted atomic.Bool
	var speculativeExecuted atomic.Bool

	kPublicConfig := fact.NewKey[string]("test.flaw3.public_config")

	// PreAuthSafe capability: safe to run speculatively
	preCap := capability.Read("read.public", capability.WithSpeculation(graph.PreAuthSafe)).
		WithProvides(kPublicConfig.Any()).
		WithRun(func(nc *execution.NodeContext) error {
			speculativeExecuted.Store(true)
			execution.Publish(nc, kPublicConfig, "public-branding-v1")
			return nil
		})

	// Auth capability that takes 5ms to introspect token, then FAILS
	authFailing := capability.NewAuthCapability("auth.fail", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
		time.Sleep(5 * time.Millisecond)
		return capability.PrincipalFact{}, errors.New("invalid signature")
	})

	engine := ref.NewEngine(ref.WithCapability(authFailing), ref.WithCapability(preCap))

	it := Flaw3TestIntent{kConfig: kPublicConfig, ran: &sensitiveExecuted}
	if err := ref.Register(engine, it); err != nil {
		t.Fatal(err)
	}
	if err := engine.Compile(); err != nil {
		t.Fatal(err)
	}

	inv := &invocation.Invocation{
		ID:        "flaw3-test",
		Intent:    "flaw3.test",
		Principal: invocation.PrincipalHint{BearerToken: "bad-token"},
	}

	_, err := engine.Dispatch(context.Background(), inv)
	if err == nil {
		t.Fatalf("expected dispatch to fail on authentication error")
	}

	// Verify that sensitive business operation was NEVER executed
	if sensitiveExecuted.Load() {
		t.Fatalf("CRITICAL SECURITY FLAW: sensitive operation executed despite authentication failure!")
	}

	// Verify speculative PreAuthSafe read ran safely
	if !speculativeExecuted.Load() {
		t.Fatalf("expected speculative pre-auth read to have executed")
	}
}

// ============================================================================
// TEST 4: Multi-Policy Evaluation — Deny Dominance & Contradiction Detection
// ============================================================================
func TestFlaw4_AlgebraicPolicyGateAndContradiction(t *testing.T) {
	// Case A: DENY Dominates
	ds := execution.AcquireDecisionSet(3)
	defer execution.ReleaseDecisionSet(ds)

	ds.RecordAllow("policy.auth", nil)
	ds.RecordDeny("policy.rate_limit", "rate limit exceeded")
	ds.RecordAllow("policy.tenant", nil)

	if ds.Verdict() != execution.VerdictDeny {
		t.Fatalf("expected VerdictDeny because DENY dominates, got %v", ds.Verdict())
	}

	// Case B: Contradictory Constraints (Conflicting Tenant Isolation)
	// Traditional middleware would overwrite: c.Locals("tenant", "B"), leaking cross-tenant data.
	// REF detects contradictory constraints and converts to DENY.
	ds2 := execution.AcquireDecisionSet(2)
	defer execution.ReleaseDecisionSet(ds2)

	ds2.RecordAllow("policy.region_east", []execution.Constraint{
		{Field: "tenant_id", Operator: "eq", Values: []string{"tenant-100"}},
	})
	ds2.RecordAllow("policy.region_west", []execution.Constraint{
		{Field: "tenant_id", Operator: "eq", Values: []string{"tenant-200"}},
	})

	if ds2.Verdict() != execution.VerdictDeny {
		t.Fatalf("expected contradictory tenant constraints to produce VerdictDeny, got %v", ds2.Verdict())
	}
}

// ============================================================================
// TEST 5: Uncontrolled Mutation Trap — 2-Phase Commit and Reverse Compensation
// ============================================================================
type trackingCompensatingEffect struct {
	name        string
	committed   bool
	compensated bool
	failCommit  bool
}

func (e *trackingCompensatingEffect) Name() string            { return e.name }
func (e *trackingCompensatingEffect) Kind() effect.EffectKind { return effect.LocalTransactional }
func (e *trackingCompensatingEffect) Commit(ctx context.Context) error {
	if e.failCommit {
		return errors.New("simulated commit failure")
	}
	e.committed = true
	return nil
}
func (e *trackingCompensatingEffect) Compensate(ctx context.Context) error {
	e.compensated = true
	return nil
}

func TestFlaw5_TwoPhaseCommitAndCompensation(t *testing.T) {
	fx1 := &trackingCompensatingEffect{name: "db.insert_order"}
	fx2 := &trackingCompensatingEffect{name: "db.charge_card", failCommit: true} // Fails

	effects := []effect.Effect{fx1, fx2}
	err := effect.CommitPlan(context.Background(), nil, "exec-tx-test", effects)
	if err == nil {
		t.Fatalf("expected commit error")
	}

	// In traditional code, fx1 would remain committed (leaking uncompensated database state).
	// In REF, fx1 must be automatically compensated in reverse order!
	if !fx1.committed {
		t.Fatalf("expected fx1 to have committed before failure")
	}
	if !fx1.compensated {
		t.Fatalf("expected fx1 to be compensated after fx2 failed commit!")
	}
}

// ============================================================================
// TEST 6: Transport Entanglement — Single Intent Reused Across Transports
// ============================================================================
type UniversalEchoIntent struct{}

func (UniversalEchoIntent) Name() intent.Name { return "universal.echo" }
func (UniversalEchoIntent) Spec() intent.Spec { return intent.Spec{} }
func (UniversalEchoIntent) Run(nc *execution.NodeContext, in BenchInput) (intent.Outcome[string], error) {
	transportName := "direct"
	if nc.Invocation() != nil && nc.Invocation().Transport.Protocol != "" {
		transportName = nc.Invocation().Transport.Protocol
	}
	return intent.Outcome[string]{Value: fmt.Sprintf("[%s] hello %s", transportName, in.ID)}, nil
}

func TestFlaw6_MultiTransportEquivalence(t *testing.T) {
	engine := ref.NewEngine()
	if err := ref.Register(engine, UniversalEchoIntent{}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Compile(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// 1. HTTP Transport
	httpInv := &invocation.Invocation{
		ID:        "http-1",
		Intent:    "universal.echo",
		Input:     ref.NewInput([]byte(`{"id":"order-99"}`), "application/json"),
		Transport: invocation.Transport{Protocol: "http"},
	}
	resHTTP, err := engine.Dispatch(ctx, httpInv)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Queue Consumer Transport
	queueInv := &invocation.Invocation{
		ID:        "queue-1",
		Intent:    "universal.echo",
		Input:     ref.NewInput([]byte(`{"id":"order-99"}`), "application/json"),
		Transport: invocation.Transport{Protocol: "queue"},
	}
	resQueue, err := engine.Dispatch(ctx, queueInv)
	if err != nil {
		t.Fatal(err)
	}

	// 3. CLI Transport
	cliInv := &invocation.Invocation{
		ID:        "cli-1",
		Intent:    "universal.echo",
		Input:     ref.NewInput([]byte(`{"id":"order-99"}`), "application/json"),
		Transport: invocation.Transport{Protocol: "cli"},
	}
	resCLI, err := engine.Dispatch(ctx, cliInv)
	if err != nil {
		t.Fatal(err)
	}

	if resHTTP.Value != "[http] hello order-99" {
		t.Fatalf("unexpected HTTP result: %v", resHTTP.Value)
	}
	if resQueue.Value != "[queue] hello order-99" {
		t.Fatalf("unexpected Queue result: %v", resQueue.Value)
	}
	if resCLI.Value != "[cli] hello order-99" {
		t.Fatalf("unexpected CLI result: %v", resCLI.Value)
	}
}

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected function to panic, but it completed normally")
		}
	}()
	fn()
}
