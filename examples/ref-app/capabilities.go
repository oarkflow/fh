package main

import (
	"errors"
	"fmt"

	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/invocation"
)

// TenantKey is a custom typed fact key for tenant context.
var TenantKey = fact.NewKey[string]("app.tenant_id")

// NewAppTenantCapability extracts and verifies the tenant ID from invocation metadata.
func NewAppTenantCapability() capability.Registration {
	return ref.Read("app.tenant",
		capability.WithSpeculation(ref.PreAuthSafe),
	).WithProvides(TenantKey.Any()).WithRun(func(nc *ref.NodeContext) error {
		inv := nc.Invocation()
		var tenantID string

		if hm, ok := inv.Metadata.(invocation.HTTPMeta); ok {
			if vals, exists := hm.Headers["X-Tenant-ID"]; exists && len(vals) > 0 {
				tenantID = vals[0]
			}
		}

		if tenantID == "" {
			tenantID = "default-org"
		}

		ref.Publish(nc, TenantKey, tenantID)
		return nil
	})
}

// NewAppAuthCapability authenticates callers using Bearer tokens.
func NewAppAuthCapability() capability.Registration {
	return capability.NewAuthCapability("app.auth", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
		if hint.BearerToken == "invalid-token" {
			return capability.PrincipalFact{}, errors.New("token revoked")
		}
		if hint.BearerToken == "" {
			// Demo fallback principal
			return capability.PrincipalFact{
				ID:       "usr-guest",
				Username: "guest",
				Roles:    []string{"shopper"},
			}, nil
		}
		return capability.PrincipalFact{
			ID:       "usr-alice",
			Username: "alice",
			Roles:    []string{"customer", "verified"},
			Claims: map[string]any{
				"vip": true,
			},
		}, nil
	})
}

// NewOrderPolicyCapability enforces tenant boundary constraints and audit obligations.
func NewOrderPolicyCapability() capability.Registration {
	return ref.Decision("app.policy.order",
		capability.WithSpeculation(ref.PreAuthSafe),
	).WithRequires(TenantKey.Any(), capability.PrincipalKey.Any()).WithRun(func(nc *ref.NodeContext) error {
		principal, err := ref.Require(nc, capability.PrincipalKey)
		if err != nil {
			nc.Decisions().RecordDeny("app.policy.order", "missing principal")
			return err
		}

		tenantID, err := ref.Require(nc, TenantKey)
		if err != nil {
			nc.Decisions().RecordDeny("app.policy.order", "missing tenant context")
			return err
		}

		// Prohibit banned tenants
		if tenantID == "banned-corp" {
			nc.Decisions().RecordDeny("app.policy.order", fmt.Sprintf("tenant %q is suspended", tenantID))
			return nil
		}

		// Apply constraint algebra: restrict tenant and geographic scope
		constraints := []ref.Constraint{
			{Field: "tenant_id", Values: []string{tenantID}},
			{Field: "region", Values: []string{"us-east-1", "eu-central-1"}},
		}

		obligations := []ref.Obligation{
			{Name: "audit_trail", Action: "log_access", Data: principal.ID},
		}

		nc.Decisions().RecordAllow("app.policy.order", constraints, obligations...)
		return nil
	})
}
