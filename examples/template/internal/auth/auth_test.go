package auth

import (
	"context"
	"testing"

	"github.com/oarkflow/fh"
)

func TestRoleAuthorizer(t *testing.T) {
	z := RoleAuthorizer{}
	if err := z.Authorize(nil, Principal{Roles: []string{"reader"}}, "developer", nil); err != ErrForbidden {
		t.Fatalf("expected forbidden, got %v", err)
	}
	if err := z.Authorize(nil, Principal{Roles: []string{"developer"}}, "developer", nil); err != nil {
		t.Fatal(err)
	}
}

func TestRBAC(t *testing.T) {
	authorizer := RoleAuthorizer{Engine: NewRBAC()}
	principal := Principal{Subject: "admin", Tenant: "default", Roles: []string{"developer"}}
	if err := authorizer.Authorize(context.Background(), principal, "route:protected", nil); err != nil {
		t.Fatalf("developer role should access protected route: %v", err)
	}
	principal.Subject = "reader-user"
	principal.Roles = []string{"reader"}
	if err := authorizer.Authorize(context.Background(), principal, "route:protected", nil); err != ErrForbidden {
		t.Fatalf("reader role should be denied, got %v", err)
	}
}

func TestCredentials(t *testing.T) {
	credentials, err := NewCredentials("admin", "correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := credentials.Verify("admin", "correct horse battery staple")
	if err != nil || principal.Subject != "admin" {
		t.Fatalf("verify: principal=%+v err=%v", principal, err)
	}
	if _, err := credentials.Verify("admin", "wrong"); err != ErrUnauthenticated {
		t.Fatalf("expected authentication failure, got %v", err)
	}
}

func TestStringsFromSession(t *testing.T) {
	roles := stringsFromSession([]any{"developer", "reader", 42})
	if len(roles) != 2 || roles[0] != "developer" || roles[1] != "reader" {
		t.Fatalf("unexpected roles: %#v", roles)
	}
}

var _ fh.HandlerFunc = Middleware(nil, nil, "")
