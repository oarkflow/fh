package realip

import (
	"net"
	"testing"
)

func TestNewDefaultDoesNotPanic(t *testing.T) { _ = New(Config{}) }

func TestClientFromChainStopsAtFirstUntrustedHop(t *testing.T) {
	_, proxyNet, _ := net.ParseCIDR("10.0.0.0/8")
	remote := net.ParseIP("10.0.0.3")
	chain := []net.IP{
		net.ParseIP("192.0.2.200"), // spoofed by the client
		net.ParseIP("203.0.113.9"), // actual untrusted client
	}
	got := clientFromChain(remote, chain, []*net.IPNet{proxyNet}, false)
	if got == nil || got.String() != "203.0.113.9" {
		t.Fatalf("got=%v", got)
	}
}

func TestClientFromChainTraversesTrustedProxyHops(t *testing.T) {
	_, proxyNet, _ := net.ParseCIDR("10.0.0.0/8")
	remote := net.ParseIP("10.0.0.3")
	chain := []net.IP{net.ParseIP("203.0.113.9"), net.ParseIP("10.0.0.2")}
	got := clientFromChain(remote, chain, []*net.IPNet{proxyNet}, false)
	if got == nil || got.String() != "203.0.113.9" {
		t.Fatalf("got=%v", got)
	}
}

func TestParseForwarded(t *testing.T) {
	chain := parseForwarded(`for=192.0.2.60;proto=http, for="[2001:db8::1]:4711"`)
	if len(chain) != 2 || chain[0].String() != "192.0.2.60" || chain[1].String() != "2001:db8::1" {
		t.Fatalf("chain=%v", chain)
	}
}
