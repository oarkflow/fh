package config

import (
	"slices"
	"testing"
)

func TestLoopbackOriginPolicyAcceptsBothBrowserNames(t *testing.T) {
	for _, value := range []string{"localhost:8080", "127.0.0.1:8080", "http://localhost:8080"} {
		_, hosts, origins, err := parseOriginPolicy(value, false)
		if err != nil {
			t.Fatalf("parseOriginPolicy(%q): %v", value, err)
		}
		for _, host := range []string{"localhost", "127.0.0.1", "::1"} {
			if !slices.Contains(hosts, host) {
				t.Fatalf("parseOriginPolicy(%q) hosts %v do not contain %q", value, hosts, host)
			}
		}
		for _, origin := range []string{"http://localhost:8080", "http://127.0.0.1:8080", "http://[::1]:8080"} {
			if !slices.Contains(origins, origin) {
				t.Fatalf("parseOriginPolicy(%q) origins %v do not contain %q", value, origins, origin)
			}
		}
	}
}

func TestPortOnlyOriginPolicyEnablesLANDevelopment(t *testing.T) {
	origin, hosts, origins, err := parseOriginPolicy(":8080", false)
	if err != nil {
		t.Fatal(err)
	}
	if origin != "http://localhost:8080" {
		t.Fatalf("canonical origin=%q", origin)
	}
	if !slices.Contains(hosts, "0.0.0.0") || !slices.Contains(origins, "http://0.0.0.0:8080") {
		t.Fatalf("port-only policy does not include wildcard LAN development: hosts=%v origins=%v", hosts, origins)
	}
	if _, _, _, err := parseOriginPolicy(":8080", true); err == nil {
		t.Fatal("production accepted a port-only origin")
	}
}
