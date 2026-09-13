package privacy

import (
	"testing"
)

func TestNewDefaultFilter(t *testing.T) {
	f := New()
	if f == nil {
		t.Fatal("expected non-nil filter")
	}
}

func TestFilterHeadersAllowlisted(t *testing.T) {
	f := New()
	headers := map[string][]string{
		"Content-Type":  {"application/json"},
		"Authorization": {"Bearer secret"},
		"User-Agent":    {"test"},
	}
	filtered := f.FilterHeaders(headers)
	if _, ok := filtered["Content-Type"]; !ok {
		t.Error("expected Content-Type to be allowed")
	}
	if _, ok := filtered["User-Agent"]; !ok {
		t.Error("expected User-Agent to be allowed")
	}
	if _, ok := filtered["Authorization"]; ok {
		t.Error("expected Authorization to be filtered out")
	}
}

func TestFilterQueryRedactsSensitive(t *testing.T) {
	f := New(Config{
		QueryRedact: []string{"token"},
	})
	result := f.FilterQuery("name=alice&token=secret123&age=30")
	if result != "name=alice&token=[REDACTED]&age=30" {
		t.Errorf("unexpected result: %q", result)
	}
}

func TestFilterQueryRedactsDefaultSensitive(t *testing.T) {
	f := New()
	result := f.FilterQuery("password=hunter2&name=alice")
	if result != "password=[REDACTED]&name=alice" {
		t.Errorf("unexpected result: %q", result)
	}
}

func TestFilterQueryEmptyString(t *testing.T) {
	f := New()
	if got := f.FilterQuery(""); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestFilterBodyDisabled(t *testing.T) {
	f := New()
	got := f.FilterBody([]byte(`{"password":"secret"}`))
	if got != nil {
		t.Errorf("expected nil when BodyLogging is disabled, got %q", got)
	}
}

func TestFilterBodyEnabled(t *testing.T) {
	f := New(Config{BodyLogging: true})
	body := []byte(`{"name":"alice"}`)
	got := f.FilterBody(body)
	if string(got) != string(body) {
		t.Errorf("expected body passthrough, got %q", got)
	}
}

func TestHashField(t *testing.T) {
	f := New()
	h1 := f.HashField("test@example.com")
	h2 := f.HashField("test@example.com")
	if h1 != h2 {
		t.Error("expected deterministic hash")
	}
	if h1 == "test@example.com" {
		t.Error("expected hash, not plaintext")
	}
}

func TestShouldExport(t *testing.T) {
	f := New(Config{NeverExport: []string{"ssn", "credit_card"}})
	if f.ShouldExport("name") != true {
		t.Error("expected name to be exportable")
	}
	if f.ShouldExport("ssn") != false {
		t.Error("expected ssn to be blocked")
	}
	if f.ShouldExport("SSN") != false {
		t.Error("expected case-insensitive match")
	}
}

func TestTemplatePath(t *testing.T) {
	f := New()
	got := f.TemplatePath("/users/123/orders/456")
	want := "/users/:id/orders/:id"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestTemplatePathDisabled(t *testing.T) {
	f := New(Config{PathTemplate: true})
	got := f.TemplatePath("/users/123")
	want := "/users/:id"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestTemplatePathNonNumeric(t *testing.T) {
	f := New()
	got := f.TemplatePath("/users/abc/profile")
	if got != "/users/abc/profile" {
		t.Errorf("expected non-numeric segments unchanged, got %q", got)
	}
}

func TestTenantPolicy(t *testing.T) {
	f := New(Config{
		TenantLogPolicies: map[string]*TenantPolicy{
			"acme": {BodyLogging: true, SamplingRate: 0.5},
		},
	})
	p := f.TenantPolicy("acme")
	if p == nil {
		t.Fatal("expected non-nil policy for acme")
	}
	if !p.BodyLogging {
		t.Error("expected BodyLogging to be true")
	}
	if p.SamplingRate != 0.5 {
		t.Errorf("expected SamplingRate 0.5, got %f", p.SamplingRate)
	}
	if f.TenantPolicy("unknown") != nil {
		t.Error("expected nil policy for unknown tenant")
	}
}

func TestIsNumericID(t *testing.T) {
	tests := []struct {
		s    string
		want bool
	}{
		{"123", true},
		{"0", true},
		{"", false},
		{"abc", false},
		{"12a3", false},
	}
	for _, tt := range tests {
		got := isNumericID(tt.s)
		if got != tt.want {
			t.Errorf("isNumericID(%q) = %v, want %v", tt.s, got, tt.want)
		}
	}
}

func TestPrivacyMiddleware(t *testing.T) {
	f := New()
	handler := PrivacyMiddleware(f)
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestGetPrivacyFilterNil(t *testing.T) {
	f := New()
	got := f.TenantPolicy("nonexistent")
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestCustomHeaderAllowlist(t *testing.T) {
	f := New(Config{
		HeaderAllowlist: []string{"X-Custom"},
	})
	headers := map[string][]string{
		"X-Custom":     {"value"},
		"Content-Type": {"application/json"},
	}
	filtered := f.FilterHeaders(headers)
	if _, ok := filtered["X-Custom"]; !ok {
		t.Error("expected X-Custom to be allowed")
	}
	if _, ok := filtered["Content-Type"]; ok {
		t.Error("expected Content-Type to be filtered with custom allowlist")
	}
}
