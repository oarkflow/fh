package proxy

import (
	"bytes"
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
	ErrorHandler func(*fh.Ctx, error) error
}

func New(cfg Config) fh.HandlerFunc {
	target, err := url.Parse(cfg.Target)
	if err != nil {
		panic(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	if cfg.Timeout > 0 {
		proxy.Transport = &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: cfg.Timeout, ResponseHeaderTimeout: cfg.Timeout}
	}
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
	return func(c *fh.Ctx) error {
		req, err := request(c, target)
		if err != nil {
			return err
		}
		writer := &responseWriter{ctx: c, header: http.Header{}, status: fh.StatusOK}
		proxy.ServeHTTP(writer, req)
		if writer.err != nil && cfg.ErrorHandler != nil {
			return cfg.ErrorHandler(c, writer.err)
		}
		if writer.err == nil && writer.wroteHeader && !c.Responded() {
			return c.SendStatus(writer.status)
		}
		return writer.err
	}
}

func Gateway(routes map[string]Config) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
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

func request(c *fh.Ctx, target *url.URL) (*http.Request, error) {
	req, err := http.NewRequestWithContext(c.Context(), c.Method(), target.String()+c.OriginalURL(), io.NopCloser(bytes.NewReader(c.Body())))
	if err != nil {
		return nil, err
	}
	for key, values := range c.GetReqHeaders() {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	req.RemoteAddr = c.IP()
	return req, nil
}

type responseWriter struct {
	ctx         *fh.Ctx
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
