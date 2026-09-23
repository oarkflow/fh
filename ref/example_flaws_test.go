package ref_test

import (
	"context"
	"fmt"

	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
)

// Example_flaw1_PipelineTyranny demonstrates how REF overlaps independent
// operations concurrently in a DAG instead of running them serially in a pipeline.
func Example_flaw1_PipelineTyranny() {
	// Traditional Approach:
	//   Step 1: Authenticate token (10ms)
	//   Step 2: Fetch tenant metadata (10ms)
	//   Total serial time = 10ms + 10ms = 20ms

	// REF Approach: Declare independent capabilities in a DAG
	kTenant := fact.NewKey[string]("example.flaw1.tenant")

	authCap := capability.NewAuthCapability("auth.user", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
		return capability.PrincipalFact{ID: "usr-alice"}, nil
	})

	tenantCap := capability.Read("read.tenant", capability.WithSpeculation(graph.PreAuthSafe)).
		WithProvides(kTenant.Any()).
		WithRun(func(nc *execution.NodeContext) error {
			execution.Publish(nc, kTenant, "tenant-globex")
			return nil
		})

	engine := ref.NewEngine(ref.WithCapability(authCap), ref.WithCapability(tenantCap))

	it := Flaw1ExampleIntent{kTenant: kTenant}
	_ = ref.Register(engine, it)
	_ = engine.Compile()

	inv := &invocation.Invocation{
		ID:        "inv-01",
		Intent:    "example.flaw1",
		Principal: invocation.PrincipalHint{BearerToken: "token-alice"},
	}

	res, _ := engine.Dispatch(context.Background(), inv)
	fmt.Printf("REF DAG Output: %v\n", res.Value)

	// Output:
	// REF DAG Output: user=usr-alice, tenant=tenant-globex
}

type Flaw1ExampleIntent struct {
	kTenant fact.Key[string]
}

func (Flaw1ExampleIntent) Name() intent.Name { return "example.flaw1" }
func (f Flaw1ExampleIntent) Spec() intent.Spec {
	return intent.Spec{
		Requires: []fact.AnyKey{capability.PrincipalKey.Any(), f.kTenant.Any()},
	}
}
func (f Flaw1ExampleIntent) Run(nc *execution.NodeContext, in BenchInput) (intent.Outcome[string], error) {
	p, _ := execution.Require(nc, capability.PrincipalKey)
	ten, _ := execution.Require(nc, f.kTenant)
	return intent.Outcome[string]{Value: fmt.Sprintf("user=%s, tenant=%s", p.ID, ten)}, nil
}

// Example_flaw2_ContextTypeSafety demonstrates compile-time typed fact keys
// vs untyped string-soup maps.
func Example_flaw2_ContextTypeSafety() {
	// Traditional Approach: String-keyed map with runtime type-assertion
	traditionalCtx := map[string]any{"tenant_id": "tenant-42"}
	traditionalTenant := traditionalCtx["tenant_id"].(string)

	// REF Approach: Strongly typed FactKey mapped to dense O(1) PlanSlot
	store := fact.AcquireStore(1)
	defer fact.ReleaseStore(store)

	fact.Put(store, 0, "tenant-42")
	refTenant, ok := fact.Get[string](store, 0)

	fmt.Printf("Traditional: %s\n", traditionalTenant)
	fmt.Printf("REF Typed:   %s (found=%v)\n", refTenant, ok)

	// Output:
	// Traditional: tenant-42
	// REF Typed:   tenant-42 (found=true)
}

// Example_flaw4_AlgebraicPolicyGate demonstrates REF's algebraic decision gate
// where DENY strictly dominates and constraints intersect.
func Example_flaw4_AlgebraicPolicyGate() {
	ds := execution.AcquireDecisionSet(3)
	defer execution.ReleaseDecisionSet(ds)

	// Three policies evaluate concurrently:
	// Policy 1: Authentication -> ALLOW
	ds.RecordAllow("policy.auth", nil)
	// Policy 2: Geolocation / IP check -> DENY
	ds.RecordDeny("policy.geo", "traffic from blocked region")
	// Policy 3: RBAC -> ALLOW
	ds.RecordAllow("policy.rbac", nil)

	// Even though 2 policies allowed, REF's algebraic composition guarantees DENY dominates:
	fmt.Printf("Composite Verdict: %s\n", ds.Verdict())

	// Output:
	// Composite Verdict: deny
}

// Example_flaw5_CrashSafeOutbox demonstrates REF's declarative EffectPlan
// providing atomic two-phase commit and outbox execution.
func Example_flaw5_CrashSafeOutbox() {
	plan := effect.EffectPlan{
		LocalTx: []effect.Effect{
			dummyEffect{name: "db.insert_invoice", kind: effect.LocalTransactional},
		},
		Durable: []effect.Effect{
			dummyEffect{name: "kafka.publish_invoice_created", kind: effect.DurableDelivery},
		},
		FireAndForget: []effect.Effect{
			dummyEffect{name: "statsd.increment_counter", kind: effect.FireAndForget},
		},
	}

	fmt.Printf("LocalTx Effects:       %d\n", len(plan.LocalTx))
	fmt.Printf("Durable Outbox Effects: %d\n", len(plan.Durable))
	fmt.Printf("FireAndForget Effects:  %d\n", len(plan.FireAndForget))

	// Output:
	// LocalTx Effects:       1
	// Durable Outbox Effects: 1
	// FireAndForget Effects:  1
}

// Example_flaw6_UniversalIntentTransport demonstrates dispatching a single
// universal Intent across multiple transports without protocol lock-in.
func Example_flaw6_UniversalIntentTransport() {
	engine := ref.NewEngine()
	_ = ref.Register(engine, UniversalEchoIntent{})
	_ = engine.Compile()

	// 1. Invocation from HTTP Transport
	resHTTP, _ := engine.Dispatch(context.Background(), &invocation.Invocation{
		ID:        "req-1",
		Intent:    "universal.echo",
		Input:     ref.NewInput([]byte(`{"id":"order-101"}`), "application/json"),
		Transport: invocation.Transport{Protocol: "http"},
	})

	// 2. Exact same Intent invoked from Background Queue Worker
	resQueue, _ := engine.Dispatch(context.Background(), &invocation.Invocation{
		ID:        "msg-1",
		Intent:    "universal.echo",
		Input:     ref.NewInput([]byte(`{"id":"order-101"}`), "application/json"),
		Transport: invocation.Transport{Protocol: "queue"},
	})

	fmt.Println(resHTTP.Value)
	fmt.Println(resQueue.Value)

	// Output:
	// [http] hello order-101
	// [queue] hello order-101
}
