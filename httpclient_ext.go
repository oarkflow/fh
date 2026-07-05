package fh

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// TokenSource supplies bearer tokens without binding the client to any OAuth package.
type TokenSource interface {
	Token(context.Context) (string, error)
}
type TokenSourceFunc func(context.Context) (string, error)

func (f TokenSourceFunc) Token(ctx context.Context) (string, error) { return f(ctx) }

// ClientBearerTokenSource adds Authorization: Bearer <token> using a pluggable token source.
func ClientBearerTokenSource(src TokenSource) ClientMiddleware {
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if src == nil {
				return next.RoundTrip(r)
			}
			tok, err := src.Token(r.Context())
			if err != nil {
				return nil, err
			}
			if tok != "" && r.Header.Get("Authorization") == "" {
				r.Header.Set("Authorization", "Bearer "+tok)
			}
			return next.RoundTrip(r)
		})
	}
}

// ClientRequestID propagates an existing request id or creates a compact random id.
func ClientRequestID(header string) ClientMiddleware {
	if header == "" {
		header = "X-Request-ID"
	}
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get(header) == "" {
				r.Header.Set(header, newClientID())
			}
			return next.RoundTrip(r)
		})
	}
}

func newClientID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// ClientTraceContext injects W3C traceparent when the caller has one in context via ClientTraceParent.
type traceParentKey struct{}

func ClientTraceParent(ctx context.Context, traceparent string) context.Context {
	return context.WithValue(ctx, traceParentKey{}, traceparent)
}
func ClientTraceContext() ClientMiddleware {
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("traceparent") == "" {
				if v, _ := r.Context().Value(traceParentKey{}).(string); v != "" {
					r.Header.Set("traceparent", v)
				}
			}
			return next.RoundTrip(r)
		})
	}
}

// ClientRequireStatus converts unexpected HTTP status codes into structured ClientError.
func ClientRequireStatus(allow func(int) bool) ClientMiddleware {
	if allow == nil {
		allow = func(code int) bool { return code >= 200 && code < 300 }
	}
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			res, err := next.RoundTrip(r)
			if err != nil {
				return res, err
			}
			if res != nil && !allow(res.StatusCode) {
				return res, &ClientError{Kind: ClientErrStatus, Method: r.Method, URL: safeURL(r.URL), StatusCode: res.StatusCode, Err: ErrClientStatus}
			}
			return res, nil
		})
	}
}

// MemoryHTTPCache is a compact in-memory HTTP cache suitable for service clients and tests.
type MemoryHTTPCache struct {
	mu      sync.RWMutex
	items   map[string]cacheItem
	maxBody int64
	ttl     time.Duration
}
type cacheItem struct {
	status   int
	header   http.Header
	body     []byte
	expires  time.Time
	etag     string
	modified string
}

func NewMemoryHTTPCache(ttl time.Duration, maxBody int64) *MemoryHTTPCache {
	if ttl <= 0 {
		ttl = time.Minute
	}
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	return &MemoryHTTPCache{items: make(map[string]cacheItem), ttl: ttl, maxBody: maxBody}
}

func ClientCache(cache *MemoryHTTPCache) ClientMiddleware {
	if cache == nil {
		cache = NewMemoryHTTPCache(time.Minute, 1<<20)
	}
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				return next.RoundTrip(r)
			}
			key := r.Method + " " + r.URL.String()
			if it, ok := cache.getFresh(key); ok {
				return cachedResponse(r, it), nil
			}
			if it, ok := cache.get(key); ok {
				if it.etag != "" {
					r.Header.Set("If-None-Match", it.etag)
				}
				if it.modified != "" {
					r.Header.Set("If-Modified-Since", it.modified)
				}
			}
			res, err := next.RoundTrip(r)
			if err != nil {
				if it, ok := cache.getFresh(key); ok {
					return cachedResponse(r, it), nil
				}
				return res, err
			}
			if res == nil || res.Body == nil {
				return res, err
			}
			if res.StatusCode == http.StatusNotModified {
				if it, ok := cache.get(key); ok {
					_ = res.Body.Close()
					return cachedResponse(r, it), nil
				}
			}
			if res.StatusCode != http.StatusOK || res.ContentLength > cache.maxBody {
				return res, err
			}
			var buf bytes.Buffer
			lr := io.LimitReader(res.Body, cache.maxBody+1)
			_, readErr := io.Copy(&buf, lr)
			_ = res.Body.Close()
			if readErr != nil || int64(buf.Len()) > cache.maxBody {
				res.Body = io.NopCloser(bytes.NewReader(buf.Bytes()))
				return res, readErr
			}
			body := append([]byte(nil), buf.Bytes()...)
			it := cacheItem{status: res.StatusCode, header: cloneHeader(res.Header), body: body, expires: time.Now().Add(cache.ttl), etag: res.Header.Get("ETag"), modified: res.Header.Get("Last-Modified")}
			cache.set(key, it)
			res.Body = io.NopCloser(bytes.NewReader(body))
			res.ContentLength = int64(len(body))
			return res, nil
		})
	}
}
func (c *MemoryHTTPCache) get(key string) (cacheItem, bool) {
	c.mu.RLock()
	it, ok := c.items[key]
	c.mu.RUnlock()
	return it, ok
}
func (c *MemoryHTTPCache) getFresh(key string) (cacheItem, bool) {
	it, ok := c.get(key)
	return it, ok && time.Now().Before(it.expires)
}
func (c *MemoryHTTPCache) set(key string, it cacheItem) {
	c.mu.Lock()
	c.items[key] = it
	c.mu.Unlock()
}
func cachedResponse(req *http.Request, it cacheItem) *http.Response {
	return &http.Response{StatusCode: it.status, Status: fmt.Sprintf("%d %s", it.status, http.StatusText(it.status)), Header: cloneHeader(it.header), Body: io.NopCloser(bytes.NewReader(it.body)), ContentLength: int64(len(it.body)), Request: req}
}

// ServiceClient is a lightweight per-service view over Client with base path, shared headers and middleware.
type ServiceClient struct {
	client  *Client
	base    string
	headers map[string]string
	mw      []ClientMiddleware
}

func (c *Client) Service(base string) *ServiceClient {
	return &ServiceClient{client: c, base: strings.TrimRight(base, "/"), headers: make(map[string]string, 4)}
}
func (s *ServiceClient) Header(k, v string) *ServiceClient { s.headers[k] = v; return s }
func (s *ServiceClient) Use(m ...ClientMiddleware) *ServiceClient {
	s.mw = append(s.mw, m...)
	return s
}
func (s *ServiceClient) R() *Request {
	r := s.client.R()
	for k, v := range s.headers {
		r.Header(k, v)
	}
	if len(s.mw) > 0 {
		r.Use(s.mw...)
	}
	return r
}
func (s *ServiceClient) url(p string) string {
	if strings.HasPrefix(p, "http://") || strings.HasPrefix(p, "https://") {
		return p
	}
	if p == "" {
		return s.base
	}
	if strings.HasPrefix(p, "/") {
		return s.base + p
	}
	return s.base + "/" + p
}
func (s *ServiceClient) Do(ctx context.Context, method, p string, body ...any) (*Response, error) {
	r := s.R()
	if len(body) > 0 {
		r.Body(body[0])
	}
	return r.Do(ctx, method, s.url(p))
}
func (s *ServiceClient) Get(ctx context.Context, p string) (*Response, error) {
	return s.R().Get(ctx, s.url(p))
}
func (s *ServiceClient) Head(ctx context.Context, p string) (*Response, error) {
	return s.R().Head(ctx, s.url(p))
}
func (s *ServiceClient) Options(ctx context.Context, p string) (*Response, error) {
	return s.R().Options(ctx, s.url(p))
}
func (s *ServiceClient) Trace(ctx context.Context, p string) (*Response, error) {
	return s.R().Trace(ctx, s.url(p))
}
func (s *ServiceClient) Delete(ctx context.Context, p string) (*Response, error) {
	return s.R().Delete(ctx, s.url(p))
}
func (s *ServiceClient) Connect(ctx context.Context, p string) (*Response, error) {
	return s.R().Connect(ctx, s.url(p))
}
func (s *ServiceClient) Post(ctx context.Context, p string, body any) (*Response, error) {
	return s.R().Body(body).Post(ctx, s.url(p))
}
func (s *ServiceClient) Put(ctx context.Context, p string, body any) (*Response, error) {
	return s.R().Body(body).Put(ctx, s.url(p))
}
func (s *ServiceClient) Patch(ctx context.Context, p string, body any) (*Response, error) {
	return s.R().Body(body).Patch(ctx, s.url(p))
}
func (s *ServiceClient) Query(ctx context.Context, p string, body any) (*Response, error) {
	return s.R().Body(body).Do(ctx, MethodQuery, s.url(p))
}
func (s *ServiceClient) Search(ctx context.Context, p string, body any) (*Response, error) {
	return s.R().Body(body).Do(ctx, MethodSearch, s.url(p))
}

// EndpointSelector supports simple client-side service discovery / load balancing.
type EndpointSelector interface {
	Next(*http.Request) (*url.URL, error)
	Report(*url.URL, *http.Response, error)
}
type RoundRobinSelector struct {
	endpoints []*url.URL
	n         uint64
	mu        sync.Mutex
}

func NewRoundRobinSelector(raw ...string) (*RoundRobinSelector, error) {
	rr := &RoundRobinSelector{}
	for _, e := range raw {
		u, err := url.Parse(e)
		if err != nil {
			return nil, err
		}
		rr.endpoints = append(rr.endpoints, u)
	}
	if len(rr.endpoints) == 0 {
		return nil, errors.New("fh client: no endpoints")
	}
	return rr, nil
}
func (r *RoundRobinSelector) Next(*http.Request) (*url.URL, error) {
	r.mu.Lock()
	i := r.n
	r.n++
	u := r.endpoints[int(i)%len(r.endpoints)]
	cp := *u
	r.mu.Unlock()
	return &cp, nil
}
func (*RoundRobinSelector) Report(*url.URL, *http.Response, error) {}
func ClientLoadBalance(sel EndpointSelector) ClientMiddleware {
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if sel == nil {
				return next.RoundTrip(r)
			}
			base, err := sel.Next(r)
			if err != nil {
				return nil, err
			}
			old := r.URL
			cp := *old
			cp.Scheme = base.Scheme
			cp.Host = base.Host
			if base.Path != "" && base.Path != "/" {
				cp.Path = strings.TrimRight(base.Path, "/") + old.Path
			}
			r.URL = &cp
			res, err := next.RoundTrip(r)
			sel.Report(base, res, err)
			return res, err
		})
	}
}

// DownloadTo streams a response body to a writer without buffering it all in memory.
func (r *Response) DownloadTo(w io.Writer) (int64, error) {
	if r == nil || r.Raw == nil || r.Raw.Body == nil {
		return 0, nil
	}
	defer r.Raw.Body.Close()
	rd := io.Reader(r.Raw.Body)
	if r.limit > 0 {
		rd = io.LimitReader(rd, r.limit+1)
	}
	n, err := io.Copy(w, rd)
	if r.limit > 0 && n > r.limit {
		return n, ErrClientBodyTooLarge
	}
	return n, err
}

// SaveAtomic writes a response to path through a temporary file and atomic rename.
func (r *Response) SaveAtomic(path string, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".fh-download-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	n, copyErr := r.DownloadTo(tmp)
	syncErr := tmp.Sync()
	closeErr := tmp.Close()
	if copyErr != nil {
		_ = os.Remove(tmpName)
		return copyErr
	}
	if syncErr != nil {
		_ = os.Remove(tmpName)
		return syncErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpName)
		return closeErr
	}
	if n >= 0 {
		_ = os.Chmod(tmpName, perm)
	}
	return os.Rename(tmpName, path)
}

// Close closes the response body without draining.
func (r *Response) Close() error {
	if r == nil || r.Raw == nil || r.Raw.Body == nil {
		return nil
	}
	return r.Raw.Body.Close()
}

// ClientSecurityDialContext returns a DialContext wrapper enforcing the security policy after DNS resolution and before connect.
func ClientSecurityDialContext(sec ClientSecurity, base *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	if base == nil {
		base = &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			host = address
		}
		if sec.Enabled() {
			dialSec := sec
			dialSec.RequireHTTPS = false
			fake := &url.URL{Scheme: "tcp", Host: net.JoinHostPort(host, port)}
			if err := dialSec.Validate(fake); err != nil {
				return nil, err
			}
		}
		return base.DialContext(ctx, network, address)
	}
}
