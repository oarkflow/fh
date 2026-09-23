package ref_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
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

// ============================================================================
// FLAW 1: Pipeline Tyranny (Serial Middleware Latency vs DAG Overlap)
// ============================================================================
// In traditional HTTP pipelines, independent operations (Auth, Tenant lookup,
// Quota check, Profile load) execute serially, summing their latencies:
//   T_total = T_auth + T_tenant + T_quota + T_profile
// In REF, independent prerequisites execute concurrently in DAG waves:
//   T_total = max(T_auth, T_tenant, T_quota, T_profile)
// ============================================================================

const flawSimulatedDelay = 500 * time.Microsecond

var (
	flawTenantFact  = fact.NewKey[string]("flaw1.tenant")
	flawQuotaFact   = fact.NewKey[string]("flaw1.quota")
	flawProfileFact = fact.NewKey[string]("flaw1.profile")
)

type Flaw1Output struct {
	Result string
}

type Flaw1Intent struct{}

func (Flaw1Intent) Name() intent.Name { return "flaw1.overlap" }
func (Flaw1Intent) Spec() intent.Spec {
	return intent.Spec{
		Requires: []fact.AnyKey{
			capability.PrincipalKey.Any(),
			flawTenantFact.Any(),
			flawQuotaFact.Any(),
			flawProfileFact.Any(),
		},
	}
}
func (Flaw1Intent) Run(nc *execution.NodeContext, in BenchInput) (intent.Outcome[Flaw1Output], error) {
	p, _ := execution.Require(nc, capability.PrincipalKey)
	t, _ := execution.Require(nc, flawTenantFact)
	q, _ := execution.Require(nc, flawQuotaFact)
	pr, _ := execution.Require(nc, flawProfileFact)
	return intent.Outcome[Flaw1Output]{
		Value: Flaw1Output{Result: p.ID + ":" + in.ID + ":" + t + ":" + q + ":" + pr},
	}, nil
}

func simulateIODelay[T any](k fact.Key[T], val T) func(*execution.NodeContext) error {
	return func(nc *execution.NodeContext) error {
		time.Sleep(flawSimulatedDelay)
		execution.Publish(nc, k, val)
		return nil
	}
}

func BenchmarkFlaw1_PipelineTyranny(b *testing.B) {
	// Traditional Approach: 4 serial operations (Auth -> Tenant -> Quota -> Profile)
	b.Run("Traditional_SerialPipeline", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			// Step 1: Auth
			time.Sleep(flawSimulatedDelay)
			p := "usr-bench"

			// Step 2: Tenant lookup
			time.Sleep(flawSimulatedDelay)
			t := "acme"

			// Step 3: Quota lookup
			time.Sleep(flawSimulatedDelay)
			q := "allowed"

			// Step 4: Profile lookup
			time.Sleep(flawSimulatedDelay)
			pr := "standard"

			_ = p + ":" + "bench-123" + ":" + t + ":" + q + ":" + pr
		}
	})

	// REF Approach: DAG schedule overlaps independent reads concurrently
	b.Run("REF_DAGConcurrency", func(b *testing.B) {
		auth := capability.NewAuthCapability("flaw1.auth", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
			time.Sleep(flawSimulatedDelay)
			return capability.PrincipalFact{ID: "usr-bench"}, nil
		})
		pre := capability.WithSpeculation(graph.PreAuthSafe)
		engine := ref.NewEngine(
			ref.WithCapability(auth),
			ref.WithCapability(capability.Read("read.tenant", pre).WithProvides(flawTenantFact.Any()).WithRun(simulateIODelay(flawTenantFact, "acme"))),
			ref.WithCapability(capability.Read("read.quota", pre).WithProvides(flawQuotaFact.Any()).WithRun(simulateIODelay(flawQuotaFact, "allowed"))),
			ref.WithCapability(capability.Read("read.profile", pre).WithProvides(flawProfileFact.Any()).WithRun(simulateIODelay(flawProfileFact, "standard"))),
		)
		if err := ref.Register(engine, Flaw1Intent{}); err != nil {
			b.Fatal(err)
		}
		if err := engine.Compile(); err != nil {
			b.Fatal(err)
		}
		inv := &invocation.Invocation{
			ID:        "flaw1-inv",
			Intent:    "flaw1.overlap",
			Input:     ref.NewInput([]byte(`{"id":"bench-123"}`), "application/json"),
			Principal: invocation.PrincipalHint{BearerToken: "token"},
		}
		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			res, err := engine.Dispatch(ctx, inv)
			if err != nil {
				b.Fatal(err)
			}
			ref.ReleaseDispatchResult(res)
		}
	})
}

// ============================================================================
// FLAW 2: Mutable Context ("String Soup" vs Typed Fact Slots)
// ============================================================================
// Traditional frameworks pass request context through string maps
// (e.g. c.Locals("key", val) or ctx.WithValue(key, val)), which causes:
//   - String hashing and map collisions on every read/write
//   - Multiple heap allocations per request for map structures
//   - Untyped runtime type-assertions (val.(MyType))
// REF uses compile-time typed fact keys mapped to dense, flat array indices:
//   - O(1) direct slice index lookup
//   - Zero string hashing
//   - Type safety guaranteed at compile time
// ============================================================================

var (
	f2UserKey    = fact.NewKey[string]("f2.user")
	f2TenantKey  = fact.NewKey[string]("f2.tenant")
	f2RoleKey    = fact.NewKey[string]("f2.role")
	f2TraceKey   = fact.NewKey[string]("f2.trace")
	f2DeviceKey  = fact.NewKey[string]("f2.device")
	f2SessionKey = fact.NewKey[string]("f2.session")
)

func BenchmarkFlaw2_ContextPassing(b *testing.B) {
	// Traditional Approach: string map context with 6 entries
	b.Run("Traditional_StringMapContext", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			m := make(map[string]any, 6)
			// Middleware layer writes
			m["user_id"] = "usr-12345"
			m["tenant_id"] = "tenant-67890"
			m["role"] = "admin"
			m["trace_id"] = "tr-abcdef"
			m["device_id"] = "dev-102938"
			m["session_id"] = "sess-83921"

			// Handler layer reads with type assertion
			u, _ := m["user_id"].(string)
			t, _ := m["tenant_id"].(string)
			r, _ := m["role"].(string)
			tr, _ := m["trace_id"].(string)
			d, _ := m["device_id"].(string)
			s, _ := m["session_id"].(string)

			if u == "" || t == "" || r == "" || tr == "" || d == "" || s == "" {
				b.Fatal("missing context")
			}
		}
	})

	// Traditional Go HTTP Approach: chained context.WithValue (allocates N context nodes)
	b.Run("Traditional_ContextWithValueChain", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			ctx := context.Background()
			// Middlewares chain WithValue
			ctx = context.WithValue(ctx, "user_id", "usr-12345")
			ctx = context.WithValue(ctx, "tenant_id", "tenant-67890")
			ctx = context.WithValue(ctx, "role", "admin")
			ctx = context.WithValue(ctx, "trace_id", "tr-abcdef")
			ctx = context.WithValue(ctx, "device_id", "dev-102938")
			ctx = context.WithValue(ctx, "session_id", "sess-83921")

			// Handler traverses linked list with type assertions
			u, _ := ctx.Value("user_id").(string)
			t, _ := ctx.Value("tenant_id").(string)
			r, _ := ctx.Value("role").(string)
			tr, _ := ctx.Value("trace_id").(string)
			d, _ := ctx.Value("device_id").(string)
			s, _ := ctx.Value("session_id").(string)

			if u == "" || t == "" || r == "" || tr == "" || d == "" || s == "" {
				b.Fatal("missing context")
			}
		}
	})

	// Traditional Concurrent Approach: sync.Map for concurrent middleware access
	b.Run("Traditional_ConcurrentSyncMap", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			var sm sync.Map
			sm.Store("user_id", "usr-12345")
			sm.Store("tenant_id", "tenant-67890")
			sm.Store("role", "admin")
			sm.Store("trace_id", "tr-abcdef")
			sm.Store("device_id", "dev-102938")
			sm.Store("session_id", "sess-83921")

			u, _ := sm.Load("user_id")
			t, _ := sm.Load("tenant_id")
			r, _ := sm.Load("role")
			tr, _ := sm.Load("trace_id")
			d, _ := sm.Load("device_id")
			s, _ := sm.Load("session_id")

			if u == nil || t == nil || r == nil || tr == nil || d == nil || s == nil {
				b.Fatal("missing context")
			}
		}
	})

	// REF Approach: Dense Fact Store with pre-allocated PlanSlots
	b.Run("REF_TypedFactStore", func(b *testing.B) {
		store := fact.AcquireStore(6)
		defer fact.ReleaseStore(store)

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			s := fact.AcquireStore(6)
			// Capability layer writes to dense slots
			fact.Put(s, 0, "usr-12345")
			fact.Put(s, 1, "tenant-67890")
			fact.Put(s, 2, "admin")
			fact.Put(s, 3, "tr-abcdef")
			fact.Put(s, 4, "dev-102938")
			fact.Put(s, 5, "sess-83921")

			// Handler layer reads
			u, _ := fact.Get[string](s, 0)
			t, _ := fact.Get[string](s, 1)
			r, _ := fact.Get[string](s, 2)
			tr, _ := fact.Get[string](s, 3)
			d, _ := fact.Get[string](s, 4)
			sess, _ := fact.Get[string](s, 5)

			if u == "" || t == "" || r == "" || tr == "" || d == "" || sess == "" {
				b.Fatal("missing fact")
			}
			fact.ReleaseStore(s)
		}
	})
}

// ============================================================================
// FLAW 3: Speculation Dilemma (Late Security vs Speculative Pre-Fetching)
// ============================================================================
// In traditional pipelines, security policies run first; data pre-fetching
// cannot start until authentication completes, adding serial latency.
// In REF, PreAuthSafe nodes (e.g. public tenant config, body validation)
// run speculatively concurrent with token verification, safely fenced.
// ============================================================================

var f3TenantConfig = fact.NewKey[string]("f3.tenant_config")

type Flaw3Intent struct{}

func (Flaw3Intent) Name() intent.Name { return "flaw3.speculation" }
func (Flaw3Intent) Spec() intent.Spec {
	return intent.Spec{
		Requires: []fact.AnyKey{capability.PrincipalKey.Any(), f3TenantConfig.Any()},
	}
}
func (Flaw3Intent) Run(nc *execution.NodeContext, in BenchInput) (intent.Outcome[string], error) {
	p, _ := execution.Require(nc, capability.PrincipalKey)
	cfg, _ := execution.Require(nc, f3TenantConfig)
	return intent.Outcome[string]{Value: p.ID + ":" + cfg}, nil
}

func BenchmarkFlaw3_Speculation(b *testing.B) {
	const authDelay = 800 * time.Microsecond
	const configDelay = 800 * time.Microsecond

	// Traditional: Auth MUST complete before config read can start
	b.Run("Traditional_SerialAuthThenFetch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			// Step 1: Verify token
			time.Sleep(authDelay)
			p := "usr-bench"

			// Step 2: Fetch tenant config
			time.Sleep(configDelay)
			cfg := "theme=dark"

			_ = p + ":" + cfg
		}
	})

	// REF: PreAuthSafe speculation overlaps token auth with tenant config fetch
	b.Run("REF_SpeculativePreAuthSafe", func(b *testing.B) {
		auth := capability.NewAuthCapability("flaw3.auth", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
			time.Sleep(authDelay)
			return capability.PrincipalFact{ID: "usr-bench"}, nil
		})
		fetchConfig := capability.Read("read.config", capability.WithSpeculation(graph.PreAuthSafe)).
			WithProvides(f3TenantConfig.Any()).
			WithRun(func(nc *execution.NodeContext) error {
				time.Sleep(configDelay)
				execution.Publish(nc, f3TenantConfig, "theme=dark")
				return nil
			})

		engine := ref.NewEngine(ref.WithCapability(auth), ref.WithCapability(fetchConfig))
		if err := ref.Register(engine, Flaw3Intent{}); err != nil {
			b.Fatal(err)
		}
		if err := engine.Compile(); err != nil {
			b.Fatal(err)
		}

		inv := &invocation.Invocation{
			ID:        "flaw3-inv",
			Intent:    "flaw3.speculation",
			Input:     ref.NewInput([]byte(`{"id":"bench"}`), "application/json"),
			Principal: invocation.PrincipalHint{BearerToken: "token"},
		}
		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			res, err := engine.Dispatch(ctx, inv)
			if err != nil {
				b.Fatal(err)
			}
			ref.ReleaseDispatchResult(res)
		}
	})
}

// ============================================================================
// FLAW 4: Fragile Security & Multi-Policy Evaluation (Serial vs Algebraic)
// ============================================================================
// Traditional authorization stacks evaluate policies sequentially via middleware:
//   Policy1 (RateLimit) -> Policy2 (AuthN) -> Policy3 (RBAC Tenant)
// REF uses an algebraic DecisionSet where policies execute in parallel DAG waves
// and compose algebraically: DENY dominates, constraints intersect.
// ============================================================================

func BenchmarkFlaw4_MultiPolicyEvaluation(b *testing.B) {
	const checkDelay = 200 * time.Microsecond

	// Traditional: 3 sequential policy checks
	b.Run("Traditional_SerialPolicies", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			// Check 1: Token valid
			time.Sleep(checkDelay)
			authOK := true

			// Check 2: Rate limit
			time.Sleep(checkDelay)
			rateOK := true

			// Check 3: Tenant isolation
			time.Sleep(checkDelay)
			tenantOK := true

			if !authOK || !rateOK || !tenantOK {
				b.Fatal("denied")
			}
		}
	})

	// REF: Parallel DecisionNodes composed algebraically into DecisionSet
	b.Run("REF_ParallelDecisionNodes", func(b *testing.B) {
		p1 := fact.NewKey[bool]("p1.ok")
		p2 := fact.NewKey[bool]("p2.ok")
		p3 := fact.NewKey[bool]("p3.ok")

		c1 := capability.Decision("policy.auth").WithProvides(p1.Any()).WithRun(func(nc *execution.NodeContext) error {
			time.Sleep(checkDelay)
			nc.Decisions().RecordAllow("policy.auth", nil)
			execution.Publish(nc, p1, true)
			return nil
		})
		c2 := capability.Decision("policy.rate").WithProvides(p2.Any()).WithRun(func(nc *execution.NodeContext) error {
			time.Sleep(checkDelay)
			nc.Decisions().RecordAllow("policy.rate", nil)
			execution.Publish(nc, p2, true)
			return nil
		})
		c3 := capability.Decision("policy.tenant").WithProvides(p3.Any()).WithRun(func(nc *execution.NodeContext) error {
			time.Sleep(checkDelay)
			nc.Decisions().RecordAllow("policy.tenant", nil)
			execution.Publish(nc, p3, true)
			return nil
		})

		type PolicyIntent struct{}
		engine := ref.NewEngine(ref.WithCapability(c1), ref.WithCapability(c2), ref.WithCapability(c3))
		if err := ref.Register(engine, flaw4Intent{p1: p1, p2: p2, p3: p3}); err != nil {
			b.Fatal(err)
		}
		if err := engine.Compile(); err != nil {
			b.Fatal(err)
		}

		inv := &invocation.Invocation{
			ID:     "flaw4-inv",
			Intent: "flaw4.policy",
			Input:  ref.NewInput([]byte(`{}`), "application/json"),
		}
		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			res, err := engine.Dispatch(ctx, inv)
			if err != nil {
				b.Fatal(err)
			}
			ref.ReleaseDispatchResult(res)
		}
	})
}

type flaw4Intent struct {
	p1, p2, p3 fact.Key[bool]
}

func (flaw4Intent) Name() intent.Name { return "flaw4.policy" }
func (f flaw4Intent) Spec() intent.Spec {
	return intent.Spec{Requires: []fact.AnyKey{f.p1.Any(), f.p2.Any(), f.p3.Any()}}
}
func (flaw4Intent) Run(nc *execution.NodeContext, in BenchInput) (intent.Outcome[string], error) {
	return intent.Outcome[string]{Value: "authorized"}, nil
}

// ============================================================================
// FLAW 5: Crash-Safe Two-Phase Outbox vs Uncontrolled Mutation Trap
// ============================================================================
// In traditional handlers, database writes and external notifications happen
// sequentially mid-flight. A crash after DB write loses the notification or
// causes partial data corruption. Implementing manual outbox requires complex
// transaction management on every endpoint.
// In REF, intents return pure EffectPlan; the engine commits the two-phase outbox.
// ============================================================================

type dummyEffect struct {
	name  string
	kind  effect.EffectKind
	store effect.EffectStore
	txID  string
}

func (e dummyEffect) Name() string            { return e.name }
func (e dummyEffect) Kind() effect.EffectKind { return e.kind }
func (e dummyEffect) Commit(ctx context.Context) error {
	if e.kind == effect.LocalTransactional && e.store != nil {
		return e.store.Record(ctx, e.txID, effect.EffectRecord{Name: e.name, Kind: e.kind})
	}
	return nil
}

type dummyStore struct{ calls *atomic.Uint64 }

func (s dummyStore) hit() {
	if s.calls != nil {
		s.calls.Add(1)
	}
}
func (s dummyStore) Begin(context.Context, string) (string, error)             { s.hit(); return "tx-001", nil }
func (s dummyStore) Record(context.Context, string, effect.EffectRecord) error { s.hit(); return nil }
func (s dummyStore) Commit(context.Context, string) error                      { s.hit(); return nil }
func (s dummyStore) Abort(context.Context, string) error                       { s.hit(); return nil }
func (s dummyStore) ScheduleDelivery(context.Context, string) error            { s.hit(); return nil }
func (s dummyStore) Recover(context.Context) ([]effect.PendingTransaction, error) {
	s.hit()
	return nil, nil
}
func (s dummyStore) Close() error { s.hit(); return nil }

func BenchmarkFlaw5_TwoPhaseOutboxCommit(b *testing.B) {
	store := dummyStore{calls: &atomic.Uint64{}}
	var storeAPI effect.EffectStore = store
	ctx := context.Background()

	// Traditional Manual Outbox: application manually begins transaction,
	// writes domain row, writes outbox row, commits, and notifies worker.
	b.Run("Traditional_ManualOutboxTx", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			txID, _ := storeAPI.Begin(ctx, "exec-1")
			// Domain write
			_ = storeAPI.Record(ctx, txID, effect.EffectRecord{Name: "order.create", Kind: effect.LocalTransactional})
			// Outbox write
			_ = storeAPI.Record(ctx, txID, effect.EffectRecord{Name: "order.email_notification", Kind: effect.DurableDelivery})
			// Atomic commit
			_ = storeAPI.Commit(ctx, txID)
			// Trigger background worker
			_ = storeAPI.ScheduleDelivery(ctx, txID)
		}
	})

	// REF Declarative Two-Phase CommitPlan
	b.Run("REF_DeclarativeEffectPlanCommit", func(b *testing.B) {
		plan := effect.EffectPlan{
			LocalTx: []effect.Effect{dummyEffect{name: "order.create", kind: effect.LocalTransactional, store: storeAPI, txID: "tx-001"}},
			Durable: []effect.Effect{dummyEffect{name: "order.email_notification", kind: effect.DurableDelivery, store: storeAPI, txID: "tx-001"}},
		}

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if err := effect.CommitEffectPlan(ctx, storeAPI, "exec-1", plan); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ============================================================================
// FLAW 6: Protocol & Transport Entanglement (Lock-in vs Universal Intent)
// ============================================================================
// In traditional frameworks, handlers accept *fh.Ctx or http.ResponseWriter.
// Reusing that logic in a background worker, queue consumer, or CLI requires
// mocking a full HTTP request/response stack.
// In REF, intents are transport-neutral and dispatch directly.
// ============================================================================

func directApplication(in BenchInput) string {
	return "processed:" + in.ID
}

func traditionalQueueReuse() (string, error) {
	req, err := http.NewRequest(http.MethodPost, "http://localhost/order?retry=true", strings.NewReader("{\"id\":\"bench\"}"))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token-123")
	req.Header.Set("X-Queue-Topic", "orders.incoming")
	if req.Header.Get("Authorization") == "" || req.Header.Get("X-Queue-Topic") == "" {
		return "", fmt.Errorf("missing request metadata")
	}
	var input BenchInput
	if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
		return "", err
	}
	return directApplication(input), nil
}

func BenchmarkFlaw6_TransportAgnosticReuse(b *testing.B) {
	b.Run("Traditional_HTTPRequestAdapterReuse", func(b *testing.B) {
		got, err := traditionalQueueReuse()
		if err != nil || got != "processed:bench" {
			b.Fatalf("preflight result=%q err=%v", got, err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			got, err := traditionalQueueReuse()
			if err != nil || got != "processed:bench" {
				b.Fatalf("result=%q err=%v", got, err)
			}
		}
	})
	b.Run("REF_TransportNeutralDispatch", func(b *testing.B) {
		engine := ref.NewEngine()
		if err := ref.Register(engine, DirectBenchIntent{}); err != nil {
			b.Fatal(err)
		}
		if err := engine.Compile(); err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		data := []byte("{\"id\":\"bench\"}")
		newInvocation := func() *invocation.Invocation {
			return &invocation.Invocation{ID: "msg-001", Intent: "direct.intent", Input: ref.NewInput(data, "application/json"), Transport: invocation.Transport{Protocol: "queue"}}
		}
		preflight, err := engine.Dispatch(ctx, newInvocation())
		if err != nil {
			b.Fatal(err)
		}
		if preflight.Value != "processed:bench" {
			b.Fatalf("REF result=%v", preflight.Value)
		}
		ref.ReleaseDispatchResult(preflight)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			res, err := engine.Dispatch(ctx, newInvocation())
			if err != nil {
				b.Fatal(err)
			}
			if res.Value != "processed:bench" {
				b.Fatalf("REF result=%v", res.Value)
			}
			ref.ReleaseDispatchResult(res)
		}
	})
}

type DirectBenchIntent struct{}

func (DirectBenchIntent) Name() intent.Name { return "direct.intent" }
func (DirectBenchIntent) Spec() intent.Spec { return intent.Spec{} }
func (DirectBenchIntent) Run(nc *execution.NodeContext, in BenchInput) (intent.Outcome[string], error) {
	return intent.Outcome[string]{Value: directApplication(in)}, nil
}
