package cors

import "testing"

func TestCORSRejectsWildcardCredentials(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = New(Config{AllowOrigins: []string{"*"}, AllowCredentials: true})
}

func TestValidOriginRejectsCRLF(t *testing.T) {
	if validOrigin("https://example.com\r\nX: y") {
		t.Fatal("origin with CRLF should be invalid")
	}
}

func TestMemoryOriginStoreWildcardSubdomain(t *testing.T) {
	s := NewMemoryOriginStore("https://*.example.com", "https://exact.test")
	cases := []struct {
		origin string
		want   bool
	}{
		{"https://api.example.com", true},           // subdomain matches
		{"https://a.b.example.com", true},           // nested subdomain matches
		{"https://api.example.com:8443", true},      // port is ignored
		{"https://example.com", false},              // bare domain must not match wildcard
		{"http://api.example.com", false},           // scheme must match
		{"https://api.evil.com", false},             // different domain
		{"https://evilexample.com", false},          // suffix without the dot boundary
		{"https://api.example.com.evil.com", false}, // suffix in the middle, not at the end
		{"https://exact.test", true},                // exact entry still works
	}
	for _, tc := range cases {
		if got := s.Allowed(tc.origin); got != tc.want {
			t.Errorf("Allowed(%q) = %v, want %v", tc.origin, got, tc.want)
		}
	}
}
