package realip

import (
	"net"
	"strings"

	"github.com/oarkflow/fh"
)

type Config struct {
	Headers        []string
	TrustedProxies []*net.IPNet
	// TrustAll accepts forwarding headers from any peer. It is intended only
	// for tests or a listener that is unreachable except through a trusted edge.
	TrustAll bool
	LocalKey string
}

func New(cfg Config) fh.HandlerFunc {
	if len(cfg.Headers) == 0 {
		cfg.Headers = []string{"Forwarded", "X-Forwarded-For", "X-Real-IP", "CF-Connecting-IP", "True-Client-IP"}
	}
	if cfg.LocalKey == "" {
		cfg.LocalKey = "real_ip"
	}
	return func(c fh.Ctx) error {
		remote := net.ParseIP(c.IP())
		if remote == nil || (!cfg.TrustAll && !trusted(remote, cfg.TrustedProxies)) {
			// Secure default: an empty trust list trusts no forwarding peer.
			return c.Next()
		}
		for _, header := range cfg.Headers {
			value := c.Get(header)
			if value == "" {
				continue
			}
			var chain []net.IP
			if strings.EqualFold(header, "Forwarded") {
				chain = parseForwarded(value)
			} else {
				chain = parseIPChain(value)
			}
			ip := clientFromChain(remote, chain, cfg.TrustedProxies, cfg.TrustAll)
			if ip == nil {
				continue
			}
			canonical := ip.String()
			if fh.SetClientIP(c, canonical) {
				c.Locals(cfg.LocalKey, canonical)
			}
			return c.Next()
		}
		return c.Next()
	}
}

// clientFromChain walks right-to-left. Each hop may only assert the address to
// its left when that hop itself is trusted. This prevents a client-provided
// leftmost value from bypassing a trusted proxy appended to the chain.
func clientFromChain(remote net.IP, chain []net.IP, proxies []*net.IPNet, trustAll bool) net.IP {
	current := remote
	for i := len(chain) - 1; i >= 0; i-- {
		if !trustAll && !trusted(current, proxies) {
			break
		}
		current = chain[i]
	}
	if current.Equal(remote) {
		return nil
	}
	return current
}

func parseIPChain(value string) []net.IP {
	parts := strings.Split(value, ",")
	result := make([]net.IP, 0, len(parts))
	for _, part := range parts {
		if ip := parseNode(strings.TrimSpace(part)); ip != nil {
			result = append(result, ip)
		}
	}
	return result
}

func parseForwarded(value string) []net.IP {
	result := make([]net.IP, 0, 2)
	for _, element := range strings.Split(value, ",") {
		for _, param := range strings.Split(element, ";") {
			key, raw, ok := strings.Cut(strings.TrimSpace(param), "=")
			if !ok || !strings.EqualFold(key, "for") {
				continue
			}
			raw = strings.Trim(strings.TrimSpace(raw), "\"")
			if ip := parseNode(raw); ip != nil {
				result = append(result, ip)
			}
			break
		}
	}
	return result
}

func parseNode(value string) net.IP {
	if value == "" || value[0] == '_' || strings.EqualFold(value, "unknown") {
		return nil
	}
	if host, _, err := net.SplitHostPort(value); err == nil {
		value = host
	} else {
		value = strings.Trim(value, "[]")
	}
	return net.ParseIP(value)
}

func trusted(ip net.IP, nets []*net.IPNet) bool {
	for _, network := range nets {
		if network != nil && network.Contains(ip) {
			return true
		}
	}
	return false
}
