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
	// PrincipalKey is the typed fact key for the resolved principal.
	PrincipalKey = fact.NewKey[PrincipalFact]("auth.principal")

	ErrUnauthenticated = errors.New("ref: unauthenticated")
)

// PrincipalFact represents an authenticated identity fact.
type PrincipalFact struct {
	ID       string
	Username string
	Roles    []string
	Scopes   []string
	Claims   map[string]any
}

// AuthenticatorFunc verifies transport principal hints and produces a PrincipalFact.
type AuthenticatorFunc func(hint invocation.PrincipalHint) (PrincipalFact, error)

// NewAuthCapability creates a capability registration that produces PrincipalKey.
func NewAuthCapability(name string, auth AuthenticatorFunc, opts ...Option) Registration {
	if name == "" {
		name = "capability.auth"
	}
	reg := NewRegistration(name, graph.DecisionNode, opts...)
	reg.Provides = []fact.AnyKey{PrincipalKey.Any()}

	reg.Run = func(nc *execution.NodeContext) error {
		inv := nc.Invocation()
		if auth == nil {
			nc.Decisions().RecordDeny(name, "no authenticator configured")
			return ErrUnauthenticated
		}

		principal, err := auth(inv.Principal)
		if err != nil {
			nc.Decisions().RecordDeny(name, fmt.Sprintf("authentication failed: %v", err))
			return err
		}

		nc.Decisions().RecordAllow(name, nil)
		execution.Publish(nc, PrincipalKey, principal)
		return nil
	}

	return reg
}
