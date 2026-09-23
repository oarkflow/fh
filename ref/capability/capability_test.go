package capability_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/invocation"
)

func TestCapabilityRegistry(t *testing.T) {
	r := capability.NewRegistry()

	authCap := capability.NewAuthCapability("auth.jwt", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
		if hint.BearerToken == "valid-token" {
			return capability.PrincipalFact{ID: "usr-1", Username: "alice"}, nil
		}
		return capability.PrincipalFact{}, errors.New("bad token")
	})

	if err := r.Register(authCap); err != nil {
		t.Fatalf("failed to register auth capability: %v", err)
	}

	// Duplicate registration error
	if err := r.Register(authCap); err == nil {
		t.Errorf("expected duplicate registration error")
	}

	// Lookup by fact
	prod, ok := r.ProducerOf(capability.PrincipalKey.DefinitionID())
	if !ok || prod.Name != "auth.jwt" {
		t.Errorf("expected ProducerOf to return auth.jwt, got %v", prod)
	}
}

func TestAuthAndTenantCapabilities(t *testing.T) {
	authCap := capability.NewAuthCapability("auth.static", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
		return capability.PrincipalFact{ID: "usr-10", Roles: []string{"admin"}}, nil
	})

	tenantCap := capability.NewTenantCapability("tenant.static", func(inv *invocation.Invocation, p *capability.PrincipalFact) (capability.TenantFact, error) {
		if p == nil || p.ID != "usr-10" {
			return capability.TenantFact{}, errors.New("unauthorized")
		}
		return capability.TenantFact{ID: "tenant-99", Tier: "enterprise"}, nil
	}, true)

	defToSlot := map[fact.DefinitionID]fact.PlanSlot{
		capability.PrincipalKey.DefinitionID(): 0,
		capability.TenantKey.DefinitionID():    1,
	}

	facts := fact.NewStore(2)
	decisions := execution.NewDecisionSet()
	inv := &invocation.Invocation{ID: "inv-test"}

	ncAuth := execution.NewNodeContext(context.Background(), inv, facts, nil, decisions, 0, defToSlot, nil, 0)
	if err := authCap.Run(ncAuth); err != nil {
		t.Fatalf("authCap run failed: %v", err)
	}

	p, err := execution.Require(ncAuth, capability.PrincipalKey)
	if err != nil || p.ID != "usr-10" {
		t.Fatalf("principal fact not published correctly: %v, %+v", err, p)
	}

	ncTenant := execution.NewNodeContext(context.Background(), inv, facts, nil, decisions, 1, defToSlot, nil, 0)
	if err := tenantCap.Run(ncTenant); err != nil {
		t.Fatalf("tenantCap run failed: %v", err)
	}

	tf, err := execution.Require(ncTenant, capability.TenantKey)
	if err != nil || tf.ID != "tenant-99" {
		t.Fatalf("tenant fact not published correctly: %v, %+v", err, tf)
	}

	cs := decisions.Constraints()
	if cs.TenantID != "tenant-99" {
		t.Errorf("expected tenant_id constraint tenant-99, got %s", cs.TenantID)
	}
}

func TestInMemoryCircuitBreakerTransitions(t *testing.T) {
	cb := capability.NewInMemoryCircuitBreaker(capability.InMemoryCircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OpenTimeout:      10 * time.Millisecond,
	})

	key := "payment-gateway"

	// Starts closed
	allowed, err := cb.Allow(key)
	if err != nil || !allowed {
		t.Fatalf("expected closed circuit to allow, got allowed=%v err=%v", allowed, err)
	}

	// Record failures up to threshold
	for i := 0; i < 3; i++ {
		if err := cb.Record(key, false); err != nil {
			t.Fatalf("record failure: %v", err)
		}
	}

	// Circuit should be open now
	allowed, _ = cb.Allow(key)
	if allowed {
		t.Fatal("expected circuit to be open after threshold failures")
	}
	if state := cb.State(key); state != capability.CircuitOpen {
		t.Fatalf("expected state open, got %v", state)
	}

	// After timeout, transitions to half-open
	time.Sleep(15 * time.Millisecond)
	allowed, _ = cb.Allow(key)
	if !allowed {
		t.Fatal("expected half-open circuit to allow a probe")
	}
	if state := cb.State(key); state != capability.CircuitHalfOpen {
		t.Fatalf("expected state half_open, got %v", state)
	}

	// Two successes close the circuit
	_ = cb.Record(key, true)
	_ = cb.Record(key, true)
	if state := cb.State(key); state != capability.CircuitClosed {
		t.Fatalf("expected state closed after successes, got %v", state)
	}
}

func TestInMemoryCircuitBreakerHalfOpenFailure(t *testing.T) {
	cb := capability.NewInMemoryCircuitBreaker(capability.InMemoryCircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenTimeout:      10 * time.Millisecond,
	})

	key := "flaky-service"

	// Trip open
	_ = cb.Record(key, false)
	_ = cb.Record(key, false)

	// Wait for half-open
	time.Sleep(15 * time.Millisecond)
	allowed, _ := cb.Allow(key)
	if !allowed {
		t.Fatal("expected half-open to allow probe")
	}

	// Probe fails → back to open
	_ = cb.Record(key, false)
	if state := cb.State(key); state != capability.CircuitOpen {
		t.Fatalf("expected open after half-open failure, got %v", state)
	}
}

func TestCircuitBreakerCapabilityRegistration(t *testing.T) {
	r := capability.NewRegistry()

	reg := capability.NewCircuitBreakerCapability(
		"cb.payment",
		func(nc *execution.NodeContext) string { return "payment" },
		func(key string) (bool, error) { return true, nil },
		func(key string, success bool) error { return nil },
		func(key string) capability.CircuitState { return capability.CircuitClosed },
	)

	if err := r.Register(reg); err != nil {
		t.Fatalf("register circuit breaker: %v", err)
	}

	prod, ok := r.Lookup("cb.payment")
	if !ok || prod.Name != "cb.payment" {
		t.Errorf("expected lookup to return cb.payment, got %v", prod)
	}
}

func TestCircuitBreakerCapabilityDeniesWhenOpen(t *testing.T) {
	cb := capability.NewInMemoryCircuitBreaker(capability.InMemoryCircuitBreakerConfig{
		FailureThreshold: 1,
		OpenTimeout:      time.Hour,
	})

	reg := capability.NewCircuitBreakerCapability(
		"cb.test",
		nil,
		cb.Allow,
		cb.Record,
		cb.State,
	)

	// Trip the circuit
	_ = cb.Record("test", false)

	defToSlot := map[fact.DefinitionID]fact.PlanSlot{}
	facts := fact.NewStore(0)
	decisions := execution.NewDecisionSet()
	inv := &invocation.Invocation{ID: "inv-cb", Intent: "test"}

	nc := execution.NewNodeContext(context.Background(), inv, facts, nil, decisions, 0, defToSlot, nil, 0)
	err := reg.Run(nc)
	if err == nil || !errors.Is(err, capability.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestInMemoryCircuitBreakerCapabilityConvenience(t *testing.T) {
	reg, cb := capability.NewInMemoryCircuitBreakerCapability(
		"cb.fast",
		capability.InMemoryCircuitBreakerConfig{
			FailureThreshold: 2,
			SuccessThreshold: 1,
			OpenTimeout:      5 * time.Millisecond,
		},
	)

	if reg.Name != "cb.fast" {
		t.Fatalf("expected name cb.fast, got %s", reg.Name)
	}

	// Should be closed initially
	allowed, _ := cb.Allow("anything")
	if !allowed {
		t.Fatal("expected initial allow")
	}
}
