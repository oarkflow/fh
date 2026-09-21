package capability_test

import (
	"context"
	"errors"
	"testing"

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

	ncAuth := execution.NewNodeContext(context.Background(), inv, facts, nil, decisions, 0, defToSlot)
	if err := authCap.Run(ncAuth); err != nil {
		t.Fatalf("authCap run failed: %v", err)
	}

	p, err := execution.Require(ncAuth, capability.PrincipalKey)
	if err != nil || p.ID != "usr-10" {
		t.Fatalf("principal fact not published correctly: %v, %+v", err, p)
	}

	ncTenant := execution.NewNodeContext(context.Background(), inv, facts, nil, decisions, 1, defToSlot)
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
