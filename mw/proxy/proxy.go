package proxy

import (
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
	proxy := &httputil.ReverseProxy{}
	dialer := &net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		// The destination guard must validate the actual upstream address, not
		// an implicit proxy selected from the process environment.
		Proxy:       nil,
		DialContext: guardedDialContext(dialer, denyNets),
	}
	if cfg.Timeout > 0 {
		transport.TLSHandshakeTimeout = cfg.Timeout
		transport.ResponseHeaderTimeout = cfg.Timeout
	}
	proxy.Transport = transport
	proxy.Rewrite = func(r *httputil.ProxyRequest) {
		// Rewrite the inbound path before SetURL so a path configured on the
		// target URL is retained exactly once.
		path := r.In.URL.Path
		if cfg.StripPrefix != "" {
			path = strings.TrimPrefix(path, cfg.StripPrefix)
			if path == "" {
				path = "/"
			}
		}
		if cfg.AddPrefix != "" {
			path = cfg.AddPrefix + path
		}
		r.Out.URL.Path = path
		r.Out.URL.RawPath = ""
		r.SetURL(target)
		// Preserve NewSingleHostReverseProxy's historical Host behavior.
		r.Out.Host = r.In.Host
		if cfg.Director != nil {
			cfg.Director(r.Out)
		}
	}
	return func(c fh.Ctx) error {
		req, err := request(c)
		if err != nil {
			return err
		}
		out := req.Clone(c.Context())
		proxy.Rewrite(&httputil.ProxyRequest{In: req, Out: out})
		reader, writer := io.Pipe()
		out.Body = reader
		out.ContentLength = -1
		if h := c.RequestHeader(); h.HasContentLength {
			out.ContentLength = int64(h.ContentLength)
		}
		go func() {
			err := c.StreamBody(func(body io.Reader) error {
				_, err := io.Copy(writer, body)
				return err
			})
			_ = writer.CloseWithError(err)
		}()
		defer reader.Close()
		response, err := transport.RoundTrip(out)
		if err != nil {
			if cfg.ErrorHandler != nil {
				return cfg.ErrorHandler(c, err)
			}
			return fh.DependencyFailure(fmt.Sprintf("upstream error: %v", err)).WithCause(err)
		}
		defer response.Body.Close()
		connectionHeaders := connectionTokens(response.Header.Values("Connection"))
		for key, values := range response.Header {
			if hopByHopHeader(key) || strings.EqualFold(key, "Content-Length") || connectionHeaders[http.CanonicalHeaderKey(key)] {
				continue
			}
			for i, value := range values {
				if i == 0 {
					c.Set(key, value)
				} else {
					c.Append(key, value)
				}
			}
		}
		c.Status(response.StatusCode)
		if response.ContentLength >= 0 {
			return c.SendStreamLength(response.Body, response.ContentLength)
		}
		return c.SendStream(response.Body)
	}
}

func hopByHopHeader(name string) bool {
	switch http.CanonicalHeaderKey(name) {
	case "Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	}
	return false
}

func connectionTokens(values []string) map[string]bool {
	out := make(map[string]bool)
	for _, value := range values {
		for _, token := range strings.Split(value, ",") {
			if token = strings.TrimSpace(token); token != "" {
				out[http.CanonicalHeaderKey(token)] = true
			}
		}
	}
	return out
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
	// Build once so transports (and their connection pools) are reused.
	handlers := make(map[string]fh.HandlerFunc, len(routes))
	for prefix, cfg := range routes {
		if cfg.StripPrefix == "" {
			cfg.StripPrefix = prefix
		}
		handlers[prefix] = New(cfg)
	}
	return func(c fh.Ctx) error {
		var best string
		for prefix := range handlers {
			if strings.HasPrefix(c.Path(), prefix) && len(prefix) > len(best) {
				best = prefix
			}
		}
		if best == "" {
			return fh.NewHTTPError(fh.StatusBadGateway, "UPSTREAM_NOT_FOUND", "no upstream matches the request path")
		}
		return handlers[best](c)
	}
}

func request(c fh.Ctx) (*http.Request, error) {
	req, err := http.NewRequestWithContext(c.Context(), c.Method(), c.OriginalURL(), nil)
	if err != nil {
		return nil, err
	}
	skipHeaders := map[string]bool{
		"Authorization":   true,
		"Cookie":          true,
		"X-Forwarded-For": true,
		"X-Real-Ip":       true,
		"X-Internal-Auth": true,
	}
	for name := range connectionTokens(c.GetReqHeaders()["Connection"]) {
		skipHeaders[name] = true
	}
	for key, values := range c.GetReqHeaders() {
		if skipHeaders[http.CanonicalHeaderKey(key)] || hopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	req.Host = c.Get(fh.HeaderHostStr)
	req.RemoteAddr = net.JoinHostPort(c.IP(), "0")
	return req, nil
}
