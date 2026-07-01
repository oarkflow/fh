package realip

import (
	"net"
	"strings"

	"github.com/oarkflow/fh"
)

type Config struct {
	Headers        []string
	TrustedProxies []*net.IPNet
	LocalKey       string
}

func New(cfg Config) fh.HandlerFunc {
	if len(cfg.Headers) == 0 {
		cfg.Headers = []string{"X-Forwarded-For", "X-Real-IP", "CF-Connecting-IP", "True-Client-IP"}
	}
	if cfg.LocalKey == "" {
		cfg.LocalKey = "real_ip"
	}
	return func(c fh.Ctx) error {
		remote := net.ParseIP(c.IP())
		if len(cfg.TrustedProxies) > 0 && remote != nil && !trusted(remote, cfg.TrustedProxies) {
			return c.Next()
		}
		for _, h := range cfg.Headers {
			v := c.Get(h)
			if v == "" {
				continue
			}
			for _, part := range strings.Split(v, ",") {
				ip := strings.TrimSpace(part)
				if parsed := net.ParseIP(ip); parsed != nil {
					c.Locals(cfg.LocalKey, ip)
					c.Set("X-Real-IP", ip)
					return c.Next()
				}
			}
		}
		return c.Next()
	}
}
func trusted(ip net.IP, nets []*net.IPNet) bool {
	for _, n := range nets {
		if n != nil && n.Contains(ip) {
			return true
		}
	}
	return false
}
