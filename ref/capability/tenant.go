package capability

import (
	"errors"
	"fmt"

	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/invocation"
)

var (
	// TenantKey is the typed fact key for the resolved tenant context.
	TenantKey = fact.NewKey[TenantFact]("tenant.context")

	ErrTenantResolution = errors.New("ref: tenant resolution failed")
)

// TenantFact represents a resolved multi-tenant context fact.
type TenantFact struct {
	ID             string
	Name           string
	Tier           string
	IsolationLevel string
	Settings       map[string]string
}

// TenantResolverFunc resolves tenant context using the invocation and optional principal.
type TenantResolverFunc func(inv *invocation.Invocation, p *PrincipalFact) (TenantFact, error)

// NewTenantCapability creates a capability registration that produces TenantKey.
func NewTenantCapability(name string, resolver TenantResolverFunc, requirePrincipal bool, opts ...Option) Registration {
	if name == "" {
		name = "capability.tenant"
	}
	reg := NewRegistration(name, graph.DecisionNode, opts...)
	if requirePrincipal {
		reg.Requires = append(reg.Requires, PrincipalKey.Any())
	}
	reg.Provides = []fact.AnyKey{TenantKey.Any()}

	reg.Run = func(nc *execution.NodeContext) error {
		if resolver == nil {
			nc.Decisions().RecordDeny(name, "no tenant resolver configured")
			return ErrTenantResolution
		}

		var p *PrincipalFact
		if requirePrincipal {
			principal, err := execution.Require(nc, PrincipalKey)
			if err != nil {
				nc.Decisions().RecordDeny(name, "missing required principal for tenant resolution")
				return err
			}
			p = &principal
		} else {
			if principal, err := execution.Require(nc, PrincipalKey); err == nil {
				p = &principal
			}
		}

		tenant, err := resolver(nc.Invocation(), p)
		if err != nil {
			nc.Decisions().RecordDeny(name, fmt.Sprintf("tenant resolution failed: %v", err))
			return err
		}

		// Record allow and bind tenant_id constraint
		nc.Decisions().RecordAllow(name, []execution.Constraint{
			{Field: "tenant_id", Values: []string{tenant.ID}},
		})

		execution.Publish(nc, TenantKey, tenant)
		return nil
	}

	return reg
}
