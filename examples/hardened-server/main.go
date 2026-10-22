// Run a public TLS origin or an HTTP origin reachable only from a trusted edge.
package main

import (
	"errors"
	"flag"
	"log"
	"strings"

	"github.com/oarkflow/fh"
)

type serverOptions struct {
	mode         fh.DeploymentMode
	addr         string
	host         string
	certFile     string
	keyFile      string
	trustedCIDRs []string
}

func buildApp(opt serverOptions) (*fh.App, error) {
	if opt.host == "" {
		return nil, errors.New("set -host to the public Host name")
	}
	switch opt.mode {
	case fh.DeploymentDirectOrigin:
		if opt.certFile == "" || opt.keyFile == "" {
			return nil, errors.New("direct-origin requires -cert and -key")
		}
	case fh.DeploymentTrustedEdge:
		if len(opt.trustedCIDRs) == 0 {
			return nil, errors.New("trusted-edge requires -trusted-proxies")
		}
	default:
		return nil, errors.New("set -mode to direct-origin or trusted-edge")
	}

	app := fh.New(
		fh.WithSecureByDefault(true),
		fh.WithAllowedHosts(opt.host),
		fh.WithMaxBufferedRequestBytes(64<<20),
		fh.WithDeploymentSecurityGate(fh.DeploymentSecurityConfig{
			Mode:              opt.mode,
			ExpectedReplicas:  1,
			TrustedProxyCIDRs: opt.trustedCIDRs,
		}),
	)
	// Public probes return only status. EnableHealth's detailed /ready response
	// should be protected because probe errors may include internal details.
	app.Get("/health/live", func(c fh.Ctx) error { return c.SendString("alive") })
	app.Get("/health/ready", func(c fh.Ctx) error {
		ready, _ := app.HealthStatus(c.Context())
		if app.IsDraining() || !ready {
			return c.Status(fh.StatusServiceUnavailable).SendString("unready")
		}
		return c.SendString("ready")
	})
	app.Get("/", func(c fh.Ctx) error { return c.SendString("fh is ready") })
	return app, nil
}

func main() {
	mode := flag.String("mode", "", "direct-origin or trusted-edge")
	addr := flag.String("listen", ":8443", "listen address")
	host := flag.String("host", "", "allowed public Host name")
	cert := flag.String("cert", "", "TLS certificate for direct-origin")
	key := flag.String("key", "", "TLS key for direct-origin")
	proxies := flag.String("trusted-proxies", "", "comma-separated edge CIDRs")
	flag.Parse()

	var cidrs []string
	for _, raw := range strings.Split(*proxies, ",") {
		if cidr := strings.TrimSpace(raw); cidr != "" {
			cidrs = append(cidrs, cidr)
		}
	}
	opt := serverOptions{mode: fh.DeploymentMode(*mode), addr: *addr, host: *host, certFile: *cert, keyFile: *key, trustedCIDRs: cidrs}
	app, err := buildApp(opt)
	if err != nil {
		log.Fatal(err)
	}
	if opt.mode == fh.DeploymentDirectOrigin {
		err = app.ListenTLSWithGracefulShutdown(opt.addr, opt.certFile, opt.keyFile)
	} else {
		err = app.ListenWithGracefulShutdown(opt.addr)
	}
	if err != nil {
		log.Fatal(err)
	}
}
