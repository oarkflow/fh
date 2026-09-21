package ref_test

import (
	"context"
	"testing"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
)

type PlaceOrderInput struct {
	Item     string  `json:"item"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

type PlaceOrderOutput struct {
	OrderID string  `json:"order_id"`
	Total   float64 `json:"total"`
}

type PlaceOrderIntent struct{}

func (PlaceOrderIntent) Name() intent.Name { return "order.place" }
func (PlaceOrderIntent) Spec() intent.Spec {
	return intent.Spec{
		Description: "Places an item order",
		Requires:    []fact.AnyKey{capability.PrincipalKey.Any()},
	}
}

func (PlaceOrderIntent) Run(nc *ref.NodeContext, in PlaceOrderInput) (ref.Outcome[PlaceOrderOutput], error) {
	principal, err := ref.Require(nc, capability.PrincipalKey)
	if err != nil {
		return ref.Outcome[PlaceOrderOutput]{}, err
	}

	total := float64(in.Quantity) * in.Price
	orderID := "ord-" + principal.ID + "-" + in.Item

	return ref.Outcome[PlaceOrderOutput]{
		Value: PlaceOrderOutput{
			OrderID: orderID,
			Total:   total,
		},
		Effects: effect.EffectPlan{
			LocalTx: []effect.Effect{},
		},
	}, nil
}

func TestEngineEndToEnd(t *testing.T) {
	engine := ref.NewEngine()

	// 1. Register auth capability
	authCap := capability.NewAuthCapability("auth.mock", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
		if hint.BearerToken == "valid-token" {
			return capability.PrincipalFact{ID: "usr-42", Username: "john"}, nil
		}
		return capability.PrincipalFact{}, ref.Failure{
			Code:     "UNAUTHENTICATED",
			Category: intent.CategoryAuth,
			Message:  "invalid token",
		}
	})
	if err := ref.RegisterCapability(engine, authCap); err != nil {
		t.Fatalf("failed to register capability: %v", err)
	}

	// 2. Register intent
	if err := ref.Register(engine, PlaceOrderIntent{}); err != nil {
		t.Fatalf("failed to register intent: %v", err)
	}

	// 3. Compile engine
	if err := engine.Compile(); err != nil {
		t.Fatalf("engine compile failed: %v", err)
	}

	// 4. Dispatch with valid auth
	inv := &invocation.Invocation{
		ID:     "inv-001",
		Intent: "order.place",
		Input:  ref.NewInput([]byte(`{"item":"laptop","quantity":2,"price":1200.50}`), "application/json"),
		Principal: invocation.PrincipalHint{
			BearerToken: "valid-token",
		},
	}

	res, err := engine.Dispatch(context.Background(), inv)
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	out, ok := res.Value.(PlaceOrderOutput)
	if !ok {
		t.Fatalf("expected PlaceOrderOutput, got %T", res.Value)
	}
	if out.OrderID != "ord-usr-42-laptop" || out.Total != 2401.00 {
		t.Errorf("unexpected output: %+v", out)
	}

	// 5. Dispatch with invalid auth -> should be denied
	invDenied := &invocation.Invocation{
		ID:     "inv-002",
		Intent: "order.place",
		Input:  ref.NewInput([]byte(`{"item":"laptop","quantity":1,"price":100}`), "application/json"),
		Principal: invocation.PrincipalHint{
			BearerToken: "invalid-token",
		},
	}

	_, err = engine.Dispatch(context.Background(), invDenied)
	if err == nil {
		t.Fatalf("expected dispatch error for invalid token, got nil")
	}

	// 6. Test App integration
	app := fh.New()
	refEng := app.EnableREF()
	if refEng == nil || app.REF() != refEng {
		t.Errorf("expected App.EnableREF to return non-nil engine and match App.REF()")
	}
}
