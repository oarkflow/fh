package intent_test

import (
	"context"
	"testing"

	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
)

type CreateUserInput struct {
	Username string `json:"username"`
	Email    string `json:"email"`
}

type CreateUserOutput struct {
	UserID string `json:"user_id"`
}

type CreateUserIntent struct{}

func (CreateUserIntent) Name() intent.Name { return "user.create" }
func (CreateUserIntent) Spec() intent.Spec {
	return intent.Spec{
		Description: "Create a new user account",
	}
}

func (CreateUserIntent) Run(nc *execution.NodeContext, input CreateUserInput) (intent.Outcome[CreateUserOutput], error) {
	if input.Username == "" {
		return intent.Outcome[CreateUserOutput]{}, intent.Failure{
			Code:     "USERNAME_REQUIRED",
			Category: intent.CategoryInvalidInput,
			Message:  "username must not be empty",
		}
	}

	return intent.Outcome[CreateUserOutput]{
		Value: CreateUserOutput{UserID: "usr-" + input.Username},
		Effects: effect.EffectPlan{
			LocalTx: []effect.Effect{},
		},
		Meta: intent.OutcomeMeta{
			CacheControl: "no-store",
		},
	}, nil
}

func TestIntentRegistrationAndRun(t *testing.T) {
	r := intent.NewRegistry()
	if err := intent.Register(r, CreateUserIntent{}); err != nil {
		t.Fatalf("failed to register intent: %v", err)
	}

	def, ok := r.Lookup("user.create")
	if !ok {
		t.Fatalf("intent not found in registry")
	}

	facts := fact.NewStore(1)
	slotMap := map[fact.DefinitionID]fact.PlanSlot{def.InputKey.DefID: 0}

	// Test successful run with JSON payload
	raw := []byte(`{"username":"bob","email":"bob@example.com"}`)
	inv := &invocation.Invocation{
		Intent: "user.create",
		Input:  invocation.NewInput(raw, "application/json"),
	}
	nc := execution.NewNodeContext(context.Background(), inv, facts, nil, nil, 0, slotMap, nil, 0)

	// Step 1: DecodeNode (compiler generates pure decode node)
	if err := def.DecodeNode(nc); err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	// Step 2: Run (operation receives already-decoded input)
	val, _, _, err := def.Run(nc)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	out, ok := val.(CreateUserOutput)
	if !ok || out.UserID != "usr-bob" {
		t.Errorf("expected user_id usr-bob, got %+v", out)
	}

	// Test validation error during decode
	invalidRaw := []byte(`{invalid-json}`)
	invBad := &invocation.Invocation{
		Intent: "user.create",
		Input:  invocation.NewInput(invalidRaw, "application/json"),
	}
	ncBad := execution.NewNodeContext(context.Background(), invBad, facts, nil, nil, 0, slotMap, nil, 0)
	err = def.DecodeNode(ncBad)
	if err == nil {
		t.Fatalf("expected decode failure, got nil")
	}

	fail, ok := err.(intent.Failure)
	if !ok || fail.Category != intent.CategoryInvalidInput {
		t.Errorf("expected CategoryInvalidInput failure, got %v", err)
	}
}
