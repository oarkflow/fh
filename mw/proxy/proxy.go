package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

type Config struct {
	Target       string
	StripPrefix  string
	AddPrefix    string
	Timeout      time.Duration
	Director     func(*http.Request)
	ErrorHandler func(fh.Ctx, error) error

	// DisableSSRFGuard turns off the default block on proxying to well-known
	// cloud metadata endpoints (169.254.169.254, 169.254.170.2,
	// fd00:ec2::254). No legitimate reverse-proxy target is ever a metadata
	// endpoint, so this guard is on by default; disable only if this proxy
	// is intentionally used as a metadata sidecar.
	DisableSSRFGuard bool

	// DeniedCIDRs additionally blocks proxying to targets whose resolved IP
	// falls within any of these networks (e.g. "127.0.0.0/8", "10.0.0.0/8").
	// Opt-in: many legitimate proxy targets are private-network services, so
	// nothing beyond the metadata guard is blocked unless configured here.
	DeniedCIDRs []string
}

// defaultDeniedCIDRs are cloud metadata endpoints that should never be a
// legitimate reverse-proxy target regardless of deployment.
var defaultDeniedCIDRs = []string{
	"169.254.169.254/32", // AWS/GCP/Azure/DigitalOcean/Alibaba/Oracle IMDS
	"169.254.170.2/32",   // AWS ECS task metadata
	"fd00:ec2::254/128",  // AWS IMDSv2 IPv6
}

func New(cfg Config) fh.HandlerFunc {
	target, err := url.Parse(cfg.Target)
	if err != nil {
		panic(err)
	}
	denyNets, err := parseCIDRs(cfg)
	if err != nil {
		panic(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	dialer := &net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:       http.ProxyFromEnvironment,
		DialContext: guardedDialContext(dialer, denyNets),
	}
	if cfg.Timeout > 0 {
		transport.TLSHandshakeTimeout = cfg.Timeout
		transport.ResponseHeaderTimeout = cfg.Timeout
	}
	proxy.Transport = transport
	direct := proxy.Director
	proxy.Director = func(r *http.Request) {
		direct(r)
		if cfg.StripPrefix != "" {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, cfg.StripPrefix)
			if r.URL.Path == "" {
				r.URL.Path = "/"
			}
		}
		if cfg.AddPrefix != "" {
			r.URL.Path = cfg.AddPrefix + r.URL.Path
		}
		if cfg.Director != nil {
			cfg.Director(r)
		}
	}
	return func(c fh.Ctx) error {
		req, err := request(c, target)
		if err != nil {
			return err
		}
		writer := &responseWriter{ctx: c, header: http.Header{}, status: fh.StatusOK}
		proxy.ServeHTTP(writer, req)
		if writer.err != nil && cfg.ErrorHandler != nil {
			return cfg.ErrorHandler(c, writer.err)
		}
		if writer.err != nil {
			return fh.DependencyFailure(fmt.Sprintf("upstream error: %v", writer.err)).WithCause(writer.err)
		}
		if writer.wroteHeader && !c.Responded() {
			return c.SendStatus(writer.status)
		}
		return nil
	}
}

func parseCIDRs(cfg Config) ([]*net.IPNet, error) {
	var raw []string
	if !cfg.DisableSSRFGuard {
		raw = append(raw, defaultDeniedCIDRs...)
	}
	raw = append(raw, cfg.DeniedCIDRs...)
	nets := make([]*net.IPNet, 0, len(raw))
	for _, c := range raw {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			return nil, fmt.Errorf("proxy: invalid denied CIDR %q: %w", c, err)
		}
		nets = append(nets, n)
	}
	return nets, nil
}

// guardedDialContext resolves the target host once, rejects any resolved IP
// that falls within denyNets, and then dials the validated IP directly
// (rather than letting the transport re-resolve the hostname), which also
// defeats DNS-rebinding attacks where the name resolves to an allowed IP at
// validation time and a denied IP at connect time.
func guardedDialContext(base *net.Dialer, denyNets []*net.IPNet) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		var lastErr error
		for _, ip := range ips {
			for _, denied := range denyNets {
				if denied.Contains(ip) {
					return nil, fmt.Errorf("proxy: target %s resolves to denied address %s", host, ip)
				}
			}
		}
		for _, ip := range ips {
			conn, err := base.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("proxy: no addresses found for %s", host)
		}
		return nil, lastErr
	}
}

func Gateway(routes map[string]Config) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		var best string
		var cfg Config
		for prefix, candidate := range routes {
			if strings.HasPrefix(c.Path(), prefix) && len(prefix) > len(best) {
				best, cfg = prefix, candidate
			}
		}
		if best == "" {
			return fh.NewHTTPError(fh.StatusBadGateway, "UPSTREAM_NOT_FOUND", "no upstream matches the request path")
		}
		if cfg.StripPrefix == "" {
			cfg.StripPrefix = best
		}
		return New(cfg)(c)
	}
}

func request(c fh.Ctx, target *url.URL) (*http.Request, error) {
	req, err := http.NewRequestWithContext(c.Context(), c.Method(), target.String()+c.OriginalURL(), io.NopCloser(bytes.NewReader(c.Body())))
	if err != nil {
		return nil, err
	}
	skipHeaders := map[string]bool{
		"Authorization":   true,
		"Cookie":          true,
		"X-Forwarded-For": true,
		"X-Real-IP":       true,
		"X-Internal-Auth": true,
	}
	for key, values := range c.GetReqHeaders() {
		if skipHeaders[key] {
			continue
		}
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	req.RemoteAddr = c.IP()
	return req, nil
}

type responseWriter struct {
	ctx         fh.Ctx
	header      http.Header
	status      int
	err         error
	wroteHeader bool
}

func (w *responseWriter) Header() http.Header { return w.header }
func (w *responseWriter) WriteHeader(status int) {
	w.wroteHeader = true
	w.status = status
	for key, values := range w.header {
		for _, value := range values {
			w.ctx.Set(key, value)
		}
	}
	w.ctx.Status(status)
}
func (w *responseWriter) Write(body []byte) (int, error) {
	w.err = w.ctx.SendBytes(body)
	if w.err != nil {
		return 0, w.err
	}
	return len(body), nil
}
