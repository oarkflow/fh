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

type BenchInput struct {
	ID string `json:"id"`
}

type BenchOutput struct {
	Result string `json:"result"`
}

type BenchIntent struct{}

func (BenchIntent) Name() intent.Name { return "bench.intent" }
func (BenchIntent) Spec() intent.Spec {
	return intent.Spec{
		Requires: []fact.AnyKey{capability.PrincipalKey.Any()},
	}
}

func (BenchIntent) Run(nc *ref.NodeContext, in BenchInput) (ref.Outcome[BenchOutput], error) {
	p, err := ref.Require(nc, capability.PrincipalKey)
	if err != nil {
		return ref.Outcome[BenchOutput]{}, err
	}
	return ref.Outcome[BenchOutput]{
		Value: BenchOutput{Result: p.ID + ":" + in.ID},
		Effects: effect.EffectPlan{
			LocalTx: []effect.Effect{},
		},
	}, nil
}

func BenchmarkREFDispatch(b *testing.B) {
	engine := ref.NewEngine(
		ref.WithCapability(capability.NewAuthCapability("auth.bench", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
			return capability.PrincipalFact{ID: "usr-bench"}, nil
		})),
	)

	if err := ref.Register(engine, BenchIntent{}); err != nil {
		b.Fatalf("failed to register intent: %v", err)
	}
	if err := engine.Compile(); err != nil {
		b.Fatalf("failed to compile engine: %v", err)
	}

	inv := &invocation.Invocation{
		ID:     "bench-inv",
		Intent: "bench.intent",
		Input:  ref.NewInput([]byte(`{"id":"bench-123"}`), "application/json"),
		Principal: invocation.PrincipalHint{
			BearerToken: "token",
		},
	}

	ctx := context.Background()

	b.ReportAllocs()

	for b.Loop() {
		_, err := engine.Dispatch(ctx, inv)
		if err != nil {
			b.Fatalf("dispatch failed: %v", err)
		}
	}
}
