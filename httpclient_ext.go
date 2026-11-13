package fh

import (
	"bytes"
	"container/list"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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
// It has a per-instance bound of 1,024 entries and an estimated 16 MiB for
// cached bodies, headers, and keys; larger entries are returned but not cached.
type MemoryHTTPCache struct {
	mu         sync.RWMutex
	items      map[string]memoryHTTPCacheEntry
	order      list.List
	cacheBytes int64
	maxBody    int64
	ttl        time.Duration
}

const (
	maxMemoryHTTPCacheEntries = 1024
	maxMemoryHTTPCacheBytes   = 16 << 20
)

type cacheItem struct {
	status   int
	header   http.Header
	body     []byte
	expires  time.Time
	etag     string
	modified string
}

type memoryHTTPCacheEntry struct {
	item    cacheItem
	orderEl *list.Element
	bytes   int64
}

func NewMemoryHTTPCache(ttl time.Duration, maxBody int64) *MemoryHTTPCache {
	if ttl <= 0 {
		ttl = time.Minute
	}
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	return &MemoryHTTPCache{items: make(map[string]memoryHTTPCacheEntry), ttl: ttl, maxBody: maxBody}
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
			if !memoryHTTPCacheRequestAllowed(r) {
				return next.RoundTrip(r)
			}
			key := memoryHTTPCacheKey(r.Method, r.URL)
			requestURL := r.URL.String()
			requestMethod := r.Method
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
				if it, ok := cache.get(key); ok && memoryHTTPCacheSensitiveRequestAllowed(r) && r.URL.String() == requestURL && r.Method == requestMethod && memoryHTTPCacheResponseAllowed(res) {
					_ = res.Body.Close()
					it.header = cloneHeader(it.header)
					for name, values := range res.Header {
						it.header[name] = append([]string(nil), values...)
					}
					if value := res.Header.Get("ETag"); value != "" {
						it.etag = value
					}
					if value := res.Header.Get("Last-Modified"); value != "" {
						it.modified = value
					}
					it.expires = time.Now().Add(memoryHTTPCacheFreshness(&http.Response{Header: it.header}, cache.ttl))
					cache.set(key, it)
					return cachedResponse(r, it), nil
				}
			}
			if res.StatusCode != http.StatusOK || !memoryHTTPCacheSensitiveRequestAllowed(r) || r.URL.String() != requestURL || r.Method != requestMethod || !memoryHTTPCacheResponseAllowed(res) {
				return res, err
			}
			bodyLimit := cache.maxBody
			if bodyLimit >= maxMemoryHTTPCacheBytes {
				bodyLimit = maxMemoryHTTPCacheBytes - 1
			}
			if res.ContentLength > bodyLimit {
				return res, err
			}
			var buf bytes.Buffer
			lr := io.LimitReader(res.Body, bodyLimit+1)
			_, readErr := io.Copy(&buf, lr)
			if readErr != nil || int64(buf.Len()) > bodyLimit {
				// Preserve the bytes consumed while probing the cache limit and keep
				// the upstream body open so oversized responses remain complete.
				res.Body = &prefixedReadCloser{
					Reader: io.MultiReader(bytes.NewReader(buf.Bytes()), res.Body),
					Closer: res.Body,
				}
				return res, readErr
			}
			_ = res.Body.Close()
			body := append([]byte(nil), buf.Bytes()...)
			it := cacheItem{status: res.StatusCode, header: cloneHeader(res.Header), body: body, expires: time.Now().Add(memoryHTTPCacheFreshness(res, cache.ttl)), etag: res.Header.Get("ETag"), modified: res.Header.Get("Last-Modified")}
			cache.set(key, it)
			res.Body = io.NopCloser(bytes.NewReader(body))
			res.ContentLength = int64(len(body))
			return res, nil
		})
	}
}

// memoryHTTPCacheKey keeps raw paths and query values out of the cache's
// retained map keys while maintaining a fixed-size key for capacity accounting.
func memoryHTTPCacheKey(method string, u *url.URL) string {
	sum := sha256.Sum256([]byte(method + " " + u.String()))
	return hex.EncodeToString(sum[:])
}

func memoryHTTPCacheRequestAllowed(r *http.Request) bool {
	if r == nil || r.URL == nil || r.URL.User != nil {
		return false
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		// A malformed query could disguise a credential key. Do not retain it
		// in a cache key when its fields cannot be parsed unambiguously.
		return false
	}
	for key := range query {
		if sensitiveClientURLKey(key) {
			return false
		}
	}
	if hasHeaderValues(r.Header, "Authorization") || hasHeaderValues(r.Header, "Cookie") ||
		hasHeaderValues(r.Header, "Range") || hasHeaderValues(r.Header, "If-Range") ||
		hasHeaderValues(r.Header, "If-None-Match") || hasHeaderValues(r.Header, "If-Modified-Since") ||
		hasHeaderValues(r.Header, "If-Match") || hasHeaderValues(r.Header, "If-Unmodified-Since") ||
		hasCacheDirectiveValue(r.Header.Values("Cache-Control"), "max-age", "0") {
		return false
	}
	return memoryHTTPCacheSensitiveRequestAllowed(r)
}

func memoryHTTPCacheSensitiveRequestAllowed(r *http.Request) bool {
	if hasHeaderValues(r.Header, "Authorization") || hasHeaderValues(r.Header, "Cookie") {
		return false
	}
	for name, values := range r.Header {
		if len(values) == 0 {
			continue
		}
		lower := strings.ToLower(name)
		for _, marker := range [...]string{"token", "secret", "password", "credential", "api-key", "apikey", "key", "auth", "bearer", "session", "tenant", "user", "account", "identity", "principal", "customer", "workspace", "organization", "org", "project", "subscription", "signature", "owner", "member", "role"} {
			if strings.Contains(lower, marker) {
				return false
			}
		}
	}
	return !hasCacheDirective(r.Header.Values("Cache-Control"), "no-store", "no-cache") &&
		!hasCacheDirective(r.Header.Values("Pragma"), "no-cache")
}

func memoryHTTPCacheResponseAllowed(res *http.Response) bool {
	if hasHeaderValues(res.Header, "Set-Cookie") || len(res.Header.Values("Vary")) != 0 {
		return false
	}
	return !hasCacheDirective(res.Header.Values("Cache-Control"), "no-store", "no-cache", "private") &&
		!hasCacheDirective(res.Header.Values("Pragma"), "no-cache")
}

func hasHeaderValues(h http.Header, name string) bool {
	return len(h.Values(name)) != 0
}

func memoryHTTPCacheFreshness(res *http.Response, fallback time.Duration) time.Duration {
	freshness := fallback
	maxAge, sharedMaxAge := time.Duration(0), time.Duration(0)
	hasMaxAge, hasSharedMaxAge := false, false
	for _, value := range res.Header.Values("Cache-Control") {
		for _, directive := range strings.Split(value, ",") {
			name, value, ok := strings.Cut(strings.TrimSpace(directive), "=")
			if !ok {
				continue
			}
			name = strings.TrimSpace(name)
			seconds, err := strconv.ParseInt(strings.Trim(strings.TrimSpace(value), `"`), 10, 64)
			parsed := time.Duration(0)
			if err == nil && seconds >= 0 {
				if seconds > int64(fallback/time.Second) {
					parsed = fallback
				} else {
					parsed = time.Duration(seconds) * time.Second
				}
			}
			switch {
			case strings.EqualFold(name, "s-maxage"):
				if !hasSharedMaxAge || parsed < sharedMaxAge {
					sharedMaxAge = parsed
				}
				hasSharedMaxAge = true
			case strings.EqualFold(name, "max-age"):
				if !hasMaxAge || parsed < maxAge {
					maxAge = parsed
				}
				hasMaxAge = true
			}
		}
	}
	if hasSharedMaxAge {
		freshness = sharedMaxAge
	} else if hasMaxAge {
		freshness = maxAge
	}
	if expires := res.Header.Get("Expires"); expires != "" {
		if t, err := http.ParseTime(expires); err == nil {
			until := time.Until(t)
			if until < freshness {
				freshness = until
			}
		}
	}
	if age := res.Header.Get("Age"); age != "" {
		if seconds, err := strconv.ParseInt(strings.TrimSpace(age), 10, 64); err == nil && seconds > 0 {
			if seconds > int64(freshness/time.Second) {
				return 0
			}
			ageDuration := time.Duration(seconds) * time.Second
			if ageDuration >= freshness {
				return 0
			}
			freshness -= ageDuration
		}
	}
	if freshness < 0 {
		return 0
	}
	return freshness
}

func hasCacheDirective(values []string, blocked ...string) bool {
	for _, value := range values {
		for _, directive := range strings.Split(value, ",") {
			name, _, _ := strings.Cut(strings.TrimSpace(directive), "=")
			name = strings.TrimSpace(name)
			for _, candidate := range blocked {
				if strings.EqualFold(name, candidate) {
					return true
				}
			}
		}
	}
	return false
}

func hasCacheDirectiveValue(values []string, name, want string) bool {
	for _, value := range values {
		for _, directive := range strings.Split(value, ",") {
			key, actual, ok := strings.Cut(strings.TrimSpace(directive), "=")
			if ok && strings.EqualFold(strings.TrimSpace(key), name) &&
				strings.EqualFold(strings.Trim(strings.TrimSpace(actual), `"`), want) {
				return true
			}
		}
	}
	return false
}

type prefixedReadCloser struct {
	io.Reader
	io.Closer
}

func (c *MemoryHTTPCache) get(key string) (cacheItem, bool) {
	c.mu.RLock()
	entry, ok := c.items[key]
	c.mu.RUnlock()
	return entry.item, ok
}
func (c *MemoryHTTPCache) getFresh(key string) (cacheItem, bool) {
	it, ok := c.get(key)
	return it, ok && time.Now().Before(it.expires)
}
func (c *MemoryHTTPCache) set(key string, it cacheItem) {
	itemBytes := memoryHTTPCacheItemBytes(key, it)
	if itemBytes > maxMemoryHTTPCacheBytes {
		return
	}
	c.mu.Lock()
	if entry, ok := c.items[key]; ok {
		c.cacheBytes -= entry.bytes
		entry.item = it
		entry.bytes = itemBytes
		c.cacheBytes += itemBytes
		c.order.MoveToBack(entry.orderEl)
	} else {
		entry := memoryHTTPCacheEntry{item: it, bytes: itemBytes}
		entry.orderEl = c.order.PushBack(key)
		c.items[key] = entry
		c.cacheBytes += itemBytes
	}
	for len(c.items) > maxMemoryHTTPCacheEntries || c.cacheBytes > maxMemoryHTTPCacheBytes {
		oldest := c.order.Front()
		if oldest == nil {
			break
		}
		oldKey := oldest.Value.(string)
		entry := c.items[oldKey]
		delete(c.items, oldKey)
		c.cacheBytes -= entry.bytes
		c.order.Remove(oldest)
	}
	c.mu.Unlock()
}

func memoryHTTPCacheItemBytes(key string, it cacheItem) int64 {
	// Include a conservative fixed charge for map, list, and entry overhead as
	// well as the retained request key, response headers, and body bytes.
	n := int64(256 + len(key) + len(it.body))
	for name, values := range it.header {
		n += int64(64 + len(name))
		for _, value := range values {
			n += int64(16 + len(value))
		}
	}
	return n
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
			reported := false
			defer func() {
				if !reported {
					sel.Report(base, nil, errors.New("fh client: round trip aborted"))
				}
			}()
			res, err := next.RoundTrip(r)
			reported = true
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
	reader := r.Reader()
	defer reader.Close()
	return io.Copy(w, reader)
}

// SaveAtomic writes a response to path through a temporary file and atomic rename.
func (r *Response) SaveAtomic(path string, perm os.FileMode) error {
	for _, segment := range strings.Split(filepath.ToSlash(path), "/") {
		if segment == ".." {
			return fmt.Errorf("fh client: invalid destination path: %s", path)
		}
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".fh-download-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()
	_, copyErr := r.DownloadTo(tmp)
	chmodErr := tmp.Chmod(perm)
	syncErr := tmp.Sync()
	closeErr := tmp.Close()
	if copyErr != nil {
		return copyErr
	}
	if chmodErr != nil {
		return chmodErr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	committed = true
	return nil
}

// Close closes the response body without draining.
func (r *Response) Close() error {
	if r == nil || r.Raw == nil || r.Raw.Body == nil {
		return nil
	}
	return r.Raw.Body.Close()
}

// ClientSecurityDialContext returns a DialContext wrapper enforcing the security policy after DNS resolution and before connect.
// It resolves DNS once, validates the resolved IPs, and connects directly to the
// resolved IP to prevent TOCTOU races where DNS could change between validation and connect.
func ClientSecurityDialContext(sec ClientSecurity, base *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	sec = sec.clone()
	if base == nil {
		base = &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			host = address
		}
		if sec.Enabled() {
			ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
			if err != nil {
				return nil, err
			}
			if len(ips) == 0 {
				return nil, ErrClientHostBlocked
			}
			var validIPs []net.IP
			for _, ip := range ips {
				if err := sec.validateResolvedIP(ip); err != nil {
					return nil, err
				}
				validIPs = append(validIPs, ip)
			}
			// Attempt to dial each validated IP until one succeeds (fallback)
			var dialErr error
			for _, ip := range validIPs {
				targetAddr := net.JoinHostPort(ip.String(), port)
				conn, err := base.DialContext(ctx, network, targetAddr)
				if err == nil {
					return conn, nil
				}
				dialErr = err
			}
			return nil, dialErr
		}
		return base.DialContext(ctx, network, address)
	}
}
