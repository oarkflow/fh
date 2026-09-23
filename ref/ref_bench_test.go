package ref_test

import (
	"context"
	"testing"

	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
)

type DispatchBenchInput struct {
	ID string `json:"id"`
}
type DispatchBenchOutput struct {
	Result string `json:"result"`
}
type DispatchBenchIntent struct{}

func (DispatchBenchIntent) Name() intent.Name { return "bench.direct" }
func (DispatchBenchIntent) Spec() intent.Spec {
	return intent.Spec{Requires: []fact.AnyKey{capability.PrincipalKey.Any()}}
}
func (DispatchBenchIntent) Run(nc *ref.NodeContext, in DispatchBenchInput) (ref.Outcome[DispatchBenchOutput], error) {
	p, err := ref.Require(nc, capability.PrincipalKey)
	if err != nil {
		return ref.Outcome[DispatchBenchOutput]{}, err
	}
	return ref.Outcome[DispatchBenchOutput]{Value: DispatchBenchOutput{Result: p.ID + ":" + in.ID}, Effects: effect.EffectPlan{}}, nil
}
func newDispatchBenchEngine(b *testing.B) *ref.Engine {
	b.Helper()
	engine := ref.NewEngine(ref.WithCapability(capability.NewAuthCapability("auth.direct", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
		return capability.PrincipalFact{ID: "usr-bench"}, nil
	})))
	if err := ref.Register(engine, DispatchBenchIntent{}); err != nil {
		b.Fatal(err)
	}
	if err := engine.Compile(); err != nil {
		b.Fatal(err)
	}
	return engine
}
func BenchmarkREFDispatch(b *testing.B) {
	engine := newDispatchBenchEngine(b)
	inv := &invocation.Invocation{ID: "bench", Intent: "bench.direct", Input: ref.NewInput([]byte("{\"id\":\"bench-123\"}"), "application/json"), Principal: invocation.PrincipalHint{BearerToken: "token"}}
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
}
func BenchmarkREFDispatchParallel(b *testing.B) {
	engine := newDispatchBenchEngine(b)
	inv := &invocation.Invocation{ID: "bench", Intent: "bench.direct", Input: ref.NewInput([]byte("{\"id\":\"bench-123\"}"), "application/json"), Principal: invocation.PrincipalHint{BearerToken: "token"}}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, err := engine.Dispatch(ctx, inv)
			if err != nil {
				b.Error(err)
				return
			}
			ref.ReleaseDispatchResult(res)
		}
	})
}
