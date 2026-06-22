package main

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/oarkflow/tcpguard"
	"github.com/oarkflow/tcpguard/bcl"
)

func TestDemoAuthzAllowsRoutesBeforeTCPGuardRules(t *testing.T) {
	provider, err := tcpguard.NewOarkflowAuthzProviderFromFile(filepath.Join(exampleDir(), "tcpguard.authz"))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		action   string
		resource string
		tenant   string
		roles    []string
	}{
		{
			name:     "public endpoint",
			action:   "GET",
			resource: "route:GET:/public",
			tenant:   "demo-bank",
			roles:    []string{"member"},
		},
		{
			name:     "locked tenant reaches tenant-lockdown rule",
			action:   "GET",
			resource: "route:GET:/public",
			tenant:   "locked-bank",
			roles:    []string{"member"},
		},
		{
			name:     "dynamic order endpoint reaches route param rule",
			action:   "PUT",
			resource: "route:PUT:/api/users/user-2/order/order-9",
			tenant:   "demo-bank",
			roles:    []string{"member"},
		},
		{
			name:     "manager export endpoint reaches export rule",
			action:   "POST",
			resource: "route:POST:/api/v1/reports/export",
			tenant:   "demo-bank",
			roles:    []string{"manager"},
		},
		{
			name:     "admin endpoint reaches admin rules",
			action:   "POST",
			resource: "route:POST:/admin/users",
			tenant:   "demo-bank",
			roles:    []string{"admin"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, err := provider.Authorize(context.Background(), tcpguard.AuthzRequest{
				Action:   tt.action,
				Resource: tt.resource,
				Subject: map[string]any{
					"id":        "demo-user",
					"type":      "user",
					"tenant_id": tt.tenant,
					"roles":     tt.roles,
				},
				Attrs: map[string]any{
					"resource.tenant_id": tt.tenant,
					"env.tenant_id":      tt.tenant,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if !decision.Allowed {
				t.Fatalf("allowed=false reason=%q matched_by=%q trace=%v", decision.Evidence.Reason, decision.Evidence.MatchedBy, decision.Evidence.Trace)
			}
		})
	}
}

func TestDemoRateLimitDoesNotAlternateAfterThrottle(t *testing.T) {
	ctx := context.Background()
	dir := exampleDir()
	bundle, err := bcl.LoadTCPGuardBundleFile(ctx, filepath.Join(dir, "tcpguard.bcl"))
	if err != nil {
		t.Fatal(err)
	}
	store := tcpguard.NewMemoryStore()
	guard, err := tcpguard.New(
		tcpguard.WithBundle(bundle),
		tcpguard.WithStore(store),
		tcpguard.WithDataSource(tcpguard.MemoryDataSource{
			SourceID: "demo-cache",
			Values:   map[string]any{},
		}),
		tcpguard.WithSQLDataSource("account-db", openAccountDB()),
		tcpguard.WithContextBuilder(tcpguard.HTTPContextBuilder{
			TrustedProxyHeaders: true,
			DisableGeoIP:        true,
			IdentityExtractor:   extractIdentity,
			BusinessExtractor:   extractBusiness,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:18181/public", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.RemoteAddr = "192.0.2.200:12345"
		req.Header.Set("User-Agent", "demo")
		req.Header.Set("X-Forwarded-For", "192.0.2.200")

		result, err := guard.EvaluateHTTPRequest(req)
		if err != nil {
			t.Fatal(err)
		}
		if i <= 3 && result.Decision.Effect != tcpguard.DecisionAllow {
			t.Fatalf("request %d effect=%s matched=%v want allow", i, result.Decision.Effect, result.Decision.MatchedRules)
		}
		if i >= 4 {
			if result.Decision.Effect != tcpguard.DecisionThrottle {
				t.Fatalf("request %d effect=%s matched=%v want throttle", i, result.Decision.Effect, result.Decision.MatchedRules)
			}
			if len(result.Decision.MatchedRules) == 0 || result.Decision.MatchedRules[0] != "demo-rate-limit" {
				t.Fatalf("request %d matched=%v want demo-rate-limit", i, result.Decision.MatchedRules)
			}
		}
	}
}
