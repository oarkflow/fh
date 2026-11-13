package realip

import (
	"log/slog"
	"net"
	"strconv"
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
	} else {
		cfg.Headers = append([]string(nil), cfg.Headers...)
	}
	cfg.TrustedProxies = cloneNetworks(cfg.TrustedProxies)
	if cfg.LocalKey == "" {
		cfg.LocalKey = "real_ip"
	}
	if cfg.TrustAll {
		// TrustAll accepts forwarding headers from any peer, including the
		// public internet. Any downstream logic keyed on client IP
		// (rate limiting, IP allowlists, audit trails) becomes trivially
		// spoofable by sending a fake X-Forwarded-For/Forwarded header
		// unless this listener is genuinely unreachable except through a
		// trusted edge.
		slog.Warn("fh/mw/realip: TrustAll is enabled — any client can spoof its IP via forwarding headers; only use this when the listener is unreachable except through a trusted edge")
	}
	return func(c fh.Ctx) error {
		remote := net.ParseIP(c.IP())
		if remote == nil || (!cfg.TrustAll && !trusted(remote, cfg.TrustedProxies)) {
			// Secure default: an empty trust list trusts no forwarding peer.
			return c.Next()
		}
		for _, header := range cfg.Headers {
			value, count := forwardingHeaderValue(c, header)
			if count == 0 || value == "" {
				continue
			}
			if count > 1 && !strings.EqualFold(header, "Forwarded") && !strings.EqualFold(header, "X-Forwarded-For") {
				// Singleton identity headers do not define a chain. Reject
				// ambiguous duplicates instead of trusting whichever value the
				// request accessor happens to return first.
				return c.Next()
			}
			var chain []net.IP
			if strings.EqualFold(header, "Forwarded") {
				chain = parseForwarded(value)
			} else {
				chain = parseIPChain(value)
			}
			if len(chain) == 0 {
				// A malformed higher-priority forwarding header must not cause
				// fallback to a lower-priority value that may be client supplied.
				return c.Next()
			}
			ip := clientFromChain(remote, chain, cfg.TrustedProxies, cfg.TrustAll)
			if ip == nil {
				return c.Next()
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

func forwardingHeaderValue(c fh.Ctx, name string) (string, int) {
	var value strings.Builder
	count := 0
	c.RequestHeader().VisitAll(func(key, fieldValue []byte) {
		if !equalHeaderName(key, name) {
			return
		}
		if count > 0 {
			value.WriteByte(',')
		}
		_, _ = value.Write(fieldValue)
		count++
	})
	if count == 0 {
		return "", 0
	}
	return value.String(), count
}

func equalHeaderName(key []byte, name string) bool {
	if len(key) != len(name) {
		return false
	}
	for i := range key {
		b := key[i]
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		n := name[i]
		if n >= 'A' && n <= 'Z' {
			n += 'a' - 'A'
		}
		if b != n {
			return false
		}
	}
	return true
}

func cloneNetworks(networks []*net.IPNet) []*net.IPNet {
	if networks == nil {
		return nil
	}
	cloned := make([]*net.IPNet, len(networks))
	for i, network := range networks {
		if network == nil {
			continue
		}
		cloned[i] = &net.IPNet{
			IP:   append(net.IP(nil), network.IP...),
			Mask: append(net.IPMask(nil), network.Mask...),
		}
	}
	return cloned
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
		ip := parseNode(strings.TrimSpace(part))
		if ip == nil {
			return nil
		}
		result = append(result, ip)
	}
	return result
}

func parseForwarded(value string) []net.IP {
	result := make([]net.IP, 0, 2)
	for _, element := range strings.Split(value, ",") {
		var found bool
		for _, param := range strings.Split(element, ";") {
			key, raw, ok := strings.Cut(strings.TrimSpace(param), "=")
			if !ok || !strings.EqualFold(strings.TrimSpace(key), "for") {
				continue
			}
			if found {
				return nil
			}
			found = true
			raw = strings.TrimSpace(raw)
			if strings.HasPrefix(raw, "\"") || strings.HasSuffix(raw, "\"") {
				if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
					return nil
				}
				raw = raw[1 : len(raw)-1]
			}
			if strings.ContainsAny(raw, "\"\\") {
				return nil
			}
			ip := parseNode(raw)
			if ip == nil {
				return nil
			}
			result = append(result, ip)
		}
		if !found {
			return nil
		}
	}
	return result
}

func parseNode(value string) net.IP {
	if value == "" || value[0] == '_' || strings.EqualFold(value, "unknown") {
		return nil
	}
	if host, port, err := net.SplitHostPort(value); err == nil {
		if port == "" || !decimalPort(port) {
			return nil
		}
		value = host
	} else if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") && len(value) > 2 {
		value = value[1 : len(value)-1]
	} else {
		if strings.ContainsAny(value, "[]") || strings.Count(value, ":") == 1 {
			return nil
		}
	}
	return net.ParseIP(value)
}

func decimalPort(port string) bool {
	for i := 0; i < len(port); i++ {
		if port[i] < '0' || port[i] > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(port, 10, 16)
	return err == nil
}

func trusted(ip net.IP, nets []*net.IPNet) bool {
	for _, network := range nets {
		if network != nil && network.Contains(ip) {
			return true
		}
	}
	return false
}
