package fh

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptrace"
	"net/textproto"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// MethodQuery is the HTTP QUERY method used by APIs that need a safe, body-capable query operation.
	// Request already uses Query(...) for URL query parameters, so use Request.Do(ctx, MethodQuery, url)
	// or Client.Query(ctx, url, body) for this method.
	MethodQuery     = "QUERY"
	MethodSearch    = "SEARCH"
	MethodPropFind  = "PROPFIND"
	MethodPropPatch = "PROPPATCH"
	MethodMKCol     = "MKCOL"
	MethodCopy      = "COPY"
	MethodMove      = "MOVE"
	MethodLock      = "LOCK"
	MethodUnlock    = "UNLOCK"
	MethodReport    = "REPORT"
	MethodPurge     = "PURGE"
	MethodLink      = "LINK"
	MethodUnlink    = "UNLINK"
)

// ClientConfig configures the fh outbound HTTP client. The zero value is safe,
// but NewClient applies production-grade connection pooling and timeout defaults.
type ClientConfig struct {
	BaseURL               string
	UserAgent             string
	Timeout               time.Duration
	DialTimeout           time.Duration
	KeepAlive             time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	ExpectContinueTimeout time.Duration
	IdleConnTimeout       time.Duration
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxConnsPerHost       int
	DisableCompression    bool
	DisableKeepAlives     bool
	ForceAttemptHTTP2     bool
	TLSConfig             *tls.Config
	Proxy                 func(*http.Request) (*url.URL, error)
	Jar                   http.CookieJar
	Transport             http.RoundTripper
	Logger                Logger
	Metrics               ClientMetrics
	Hooks                 ClientHooks
	Security              ClientSecurity
	Retry                 RetryPolicy
	Redirect              RedirectPolicy
	BodyLimit             int64
	ResponseBodyLimit     int64
}

func (c ClientConfig) normalize() ClientConfig {
	if c.Timeout <= 0 {
		c.Timeout = 30 * time.Second
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.KeepAlive <= 0 {
		c.KeepAlive = 30 * time.Second
	}
	if c.TLSHandshakeTimeout <= 0 {
		c.TLSHandshakeTimeout = 5 * time.Second
	}
	if c.ResponseHeaderTimeout <= 0 {
		c.ResponseHeaderTimeout = 15 * time.Second
	}
	if c.ExpectContinueTimeout <= 0 {
		c.ExpectContinueTimeout = time.Second
	}
	if c.IdleConnTimeout <= 0 {
		c.IdleConnTimeout = 90 * time.Second
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = 1024
	}
	if c.MaxIdleConnsPerHost <= 0 {
		c.MaxIdleConnsPerHost = 256
	}
	if c.MaxConnsPerHost <= 0 {
		c.MaxConnsPerHost = 1024
	}
	if c.UserAgent == "" {
		c.UserAgent = "fh-client/1.0"
	}
	if c.Logger == nil {
		c.Logger = NewDefaultLogger()
	}
	if c.Metrics == nil {
		c.Metrics = NopClientMetrics{}
	}
	if c.Jar == nil {
		c.Jar, _ = cookiejar.New(nil)
	}
	if c.Retry.MaxAttempts == 0 {
		c.Retry = DefaultRetryPolicy()
	}
	if c.Redirect.Max == 0 {
		c.Redirect = RedirectPolicy{Max: 10, StripSensitiveHeaders: true}
	}
	return c
}

// Client is a high-performance, middleware-first outbound HTTP client for fh.
type Client struct {
	cfg    ClientConfig
	hc     *http.Client
	rt     http.RoundTripper
	mw     []ClientMiddleware
	pool   sync.Pool
	closed atomic.Bool
}

// NewClient creates a production-ready HTTP client. Middleware can be added via Use.
func NewClient(cfg ...ClientConfig) *Client {
	var c ClientConfig
	if len(cfg) > 0 {
		c = cfg[0]
	}
	c = c.normalize()
	transport := c.Transport
	if transport == nil {
		d := &net.Dialer{Timeout: c.DialTimeout, KeepAlive: c.KeepAlive}
		dialContext := d.DialContext
		if c.Security.Enabled() {
			dialContext = ClientSecurityDialContext(c.Security, d)
		}
		transport = &http.Transport{
			Proxy:                 c.Proxy,
			DialContext:           dialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          c.MaxIdleConns,
			MaxIdleConnsPerHost:   c.MaxIdleConnsPerHost,
			MaxConnsPerHost:       c.MaxConnsPerHost,
			IdleConnTimeout:       c.IdleConnTimeout,
			TLSHandshakeTimeout:   c.TLSHandshakeTimeout,
			ExpectContinueTimeout: c.ExpectContinueTimeout,
			ResponseHeaderTimeout: c.ResponseHeaderTimeout,
			TLSClientConfig:       c.TLSConfig,
			DisableCompression:    c.DisableCompression,
			DisableKeepAlives:     c.DisableKeepAlives,
		}
	}
	cl := &Client{cfg: c, rt: transport}
	cl.pool.New = func() any {
		return &Request{client: cl, headers: make(http.Header, 8), query: make(url.Values, 4), pathParams: make(map[string]string, 4)}
	}
	cl.hc = &http.Client{Transport: cl.wrap(transport), Timeout: c.Timeout, Jar: c.Jar, CheckRedirect: cl.checkRedirect}
	callClientHook(c.Hooks.OnClientStart, ClientEvent{At: time.Now()})
	return cl
}

// Use appends outbound middlewares. It is intended to be called during startup.
func (c *Client) Use(m ...ClientMiddleware) *Client {
	c.mw = append(c.mw, m...)
	c.hc.Transport = c.wrap(c.rt)
	return c
}

func (c *Client) wrap(base http.RoundTripper) http.RoundTripper {
	var rt http.RoundTripper = roundTripperFunc(func(r *http.Request) (*http.Response, error) { return base.RoundTrip(r) })
	for i := len(c.mw) - 1; i >= 0; i-- {
		rt = c.mw[i](rt)
	}
	rt = builtinClientTransport{next: rt, client: c}
	return rt
}

// Close releases idle connections and runs lifecycle hooks.
func (c *Client) Close() error {
	if c == nil || c.closed.Swap(true) {
		return nil
	}
	if tr, ok := c.rt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	callClientHook(c.cfg.Hooks.OnClientClose, ClientEvent{At: time.Now()})
	return nil
}

// R returns a reusable fluent request builder. Do not use the returned Request concurrently.
func (c *Client) R() *Request {
	r := c.pool.Get().(*Request)
	r.reset(c)
	return r
}

func (c *Client) releaseRequest(r *Request) {
	if r == nil {
		return
	}
	if r.bodyBytes != nil && cap(r.bodyBytes) > 1<<20 {
		r.bodyBytes = nil
	}
	c.pool.Put(r)
}

func (c *Client) Do(ctx context.Context, method, u string, body ...any) (*Response, error) {
	r := c.R()
	if len(body) > 0 {
		r.Body(body[0])
	}
	return r.Do(ctx, method, u)
}
func (c *Client) Get(ctx context.Context, u string) (*Response, error)  { return c.R().Get(ctx, u) }
func (c *Client) Head(ctx context.Context, u string) (*Response, error) { return c.R().Head(ctx, u) }
func (c *Client) Options(ctx context.Context, u string) (*Response, error) {
	return c.R().Options(ctx, u)
}
func (c *Client) Trace(ctx context.Context, u string) (*Response, error) { return c.R().Trace(ctx, u) }
func (c *Client) Delete(ctx context.Context, u string) (*Response, error) {
	return c.R().Delete(ctx, u)
}
func (c *Client) Connect(ctx context.Context, u string) (*Response, error) {
	return c.R().Connect(ctx, u)
}
func (c *Client) Post(ctx context.Context, u string, body any) (*Response, error) {
	return c.R().Body(body).Post(ctx, u)
}
func (c *Client) Put(ctx context.Context, u string, body any) (*Response, error) {
	return c.R().Body(body).Put(ctx, u)
}
func (c *Client) Patch(ctx context.Context, u string, body any) (*Response, error) {
	return c.R().Body(body).Patch(ctx, u)
}
func (c *Client) Query(ctx context.Context, u string, body any) (*Response, error) {
	return c.R().Body(body).Do(ctx, MethodQuery, u)
}
func (c *Client) Search(ctx context.Context, u string, body any) (*Response, error) {
	return c.R().Body(body).Do(ctx, MethodSearch, u)
}

func (c *Client) checkRedirect(req *http.Request, via []*http.Request) error {
	p := c.cfg.Redirect
	if p.Max <= 0 {
		p.Max = 10
	}
	if len(via) >= p.Max {
		return ErrClientRedirectLimit
	}
	if p.SameHostOnly && len(via) > 0 && !strings.EqualFold(req.URL.Host, via[0].URL.Host) {
		return ErrClientRedirectBlocked
	}
	if p.HTTPSOnly && req.URL.Scheme != "https" {
		return ErrClientRedirectBlocked
	}
	if p.StripSensitiveHeaders && len(via) > 0 && !strings.EqualFold(req.URL.Host, via[0].URL.Host) {
		stripSensitive(req.Header)
	}
	callClientHook(c.cfg.Hooks.OnRedirect, ClientEvent{At: time.Now(), Method: req.Method, URL: safeURL(req.URL), Attempt: len(via) + 1})
	return nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// ClientMiddleware wraps outbound HTTP round trips.
type ClientMiddleware func(http.RoundTripper) http.RoundTripper

type builtinClientTransport struct {
	next   http.RoundTripper
	client *Client
}

func (b builtinClientTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c := b.client
	start := time.Now()
	event := ClientEvent{At: start, Method: req.Method, URL: safeURL(req.URL), Host: req.URL.Host}
	callClientHook(c.cfg.Hooks.OnBeforeRequest, event)
	c.cfg.Metrics.Inflight(1)
	defer c.cfg.Metrics.Inflight(-1)
	if c.cfg.Security.Enabled() {
		if err := c.cfg.Security.Validate(req.URL); err != nil {
			c.cfg.Metrics.Error(req.Method, req.URL.Host, "security")
			callClientHook(c.cfg.Hooks.OnError, event.withErr(err))
			return nil, &ClientError{Kind: ClientErrSecurity, Method: req.Method, URL: safeURL(req.URL), Err: err, Retryable: false}
		}
	}
	trace := &httptrace.ClientTrace{
		DNSStart:     func(i httptrace.DNSStartInfo) { callClientHook(c.cfg.Hooks.OnDNSStart, event.withHost(i.Host)) },
		DNSDone:      func(i httptrace.DNSDoneInfo) { callClientHook(c.cfg.Hooks.OnDNSDone, event.withErr(i.Err)) },
		ConnectStart: func(network, addr string) { callClientHook(c.cfg.Hooks.OnConnectStart, event.withHost(addr)) },
		ConnectDone: func(network, addr string, err error) {
			callClientHook(c.cfg.Hooks.OnConnectDone, event.withHost(addr).withErr(err))
		},
		TLSHandshakeStart: func() { callClientHook(c.cfg.Hooks.OnTLSHandshakeStart, event) },
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			callClientHook(c.cfg.Hooks.OnTLSHandshakeDone, event.withErr(err))
		},
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				callClientHook(c.cfg.Hooks.OnConnectionReuse, event)
			} else {
				callClientHook(c.cfg.Hooks.OnConnectionOpen, event)
			}
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			callClientHook(c.cfg.Hooks.OnRequestSent, event.withErr(info.Err))
		},
		GotFirstResponseByte: func() { callClientHook(c.cfg.Hooks.OnResponseHeaders, event) },
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	res, err := b.next.RoundTrip(req)
	dur := time.Since(start)
	code := 0
	if res != nil {
		code = res.StatusCode
	}
	c.cfg.Metrics.Observe(req.Method, req.URL.Host, code, dur)
	if err != nil {
		c.cfg.Metrics.Error(req.Method, req.URL.Host, classifyNetErr(err))
		callClientHook(c.cfg.Hooks.OnError, event.withErr(err).withDuration(dur))
		return res, wrapClientErr(req, err, dur)
	}
	callClientHook(c.cfg.Hooks.OnAfterResponse, event.withStatus(code).withDuration(dur))
	return res, nil
}

// Request is a fluent, allocation-conscious request builder.
type Request struct {
	client           *Client
	method           string
	rawURL           string
	headers          http.Header
	query            url.Values
	pathParams       map[string]string
	cookies          []*http.Cookie
	body             any
	bodyBytes        []byte
	bodyReader       io.Reader
	bodyCloser       io.Closer
	contentType      string
	timeout          time.Duration
	retry            *RetryPolicy
	decode           any
	errDecode        any
	meta             map[string]any
	middleware       []ClientMiddleware
	noDefaultHeaders bool
	bodyLimit        int64
	responseLimit    int64
	stream           bool
	idempotencyKey   string
}

func (r *Request) reset(c *Client) {
	r.client = c
	r.method = ""
	r.rawURL = ""
	r.body = nil
	r.bodyReader = nil
	r.bodyCloser = nil
	r.contentType = ""
	r.timeout = 0
	r.retry = nil
	r.decode = nil
	r.errDecode = nil
	r.noDefaultHeaders = false
	r.bodyLimit = 0
	r.responseLimit = 0
	r.stream = false
	r.idempotencyKey = ""
	for k := range r.headers {
		delete(r.headers, k)
	}
	for k := range r.query {
		delete(r.query, k)
	}
	for k := range r.pathParams {
		delete(r.pathParams, k)
	}
	r.cookies = r.cookies[:0]
	r.middleware = r.middleware[:0]
	if r.meta != nil {
		for k := range r.meta {
			delete(r.meta, k)
		}
	}
}

func (r *Request) Header(k, v string) *Request { r.headers.Set(k, v); return r }
func (r *Request) Headers(h map[string]string) *Request {
	for k, v := range h {
		r.headers.Set(k, v)
	}
	return r
}
func (r *Request) AddHeader(k, v string) *Request { r.headers.Add(k, v); return r }
func (r *Request) Query(k, v string) *Request     { r.query.Add(k, v); return r }
func (r *Request) QuerySet(k, v string) *Request  { r.query.Set(k, v); return r }
func (r *Request) QueryValues(values url.Values) *Request {
	for k, vv := range values {
		for _, v := range vv {
			r.query.Add(k, v)
		}
	}
	return r
}
func (r *Request) QueryMap(values map[string]string) *Request {
	for k, v := range values {
		r.query.Set(k, v)
	}
	return r
}
func (r *Request) QueryRaw(raw string) *Request {
	if raw == "" {
		return r
	}
	values, err := url.ParseQuery(strings.TrimPrefix(raw, "?"))
	if err != nil {
		return r
	}
	return r.QueryValues(values)
}
func (r *Request) Param(k, v string) *Request       { r.pathParams[k] = v; return r }
func (r *Request) Cookie(c *http.Cookie) *Request   { r.cookies = append(r.cookies, c); return r }
func (r *Request) Timeout(d time.Duration) *Request { r.timeout = d; return r }
func (r *Request) Retry(p RetryPolicy) *Request     { r.retry = &p; return r }
func (r *Request) Use(m ...ClientMiddleware) *Request {
	r.middleware = append(r.middleware, m...)
	return r
}
func (r *Request) Decode(v any) *Request            { r.decode = v; return r }
func (r *Request) DecodeError(v any) *Request       { r.errDecode = v; return r }
func (r *Request) BodyLimit(n int64) *Request       { r.bodyLimit = n; return r }
func (r *Request) ResponseLimit(n int64) *Request   { r.responseLimit = n; return r }
func (r *Request) Stream() *Request                 { r.stream = true; return r }
func (r *Request) IdempotencyKey(k string) *Request { r.idempotencyKey = k; return r }
func (r *Request) Meta(k string, v any) *Request {
	if r.meta == nil {
		r.meta = make(map[string]any, 4)
	}
	r.meta[k] = v
	return r
}
func (r *Request) Body(v any) *Request { r.body = v; return r }
func (r *Request) Bytes(b []byte, ct ...string) *Request {
	r.bodyBytes = b
	if len(ct) > 0 {
		r.contentType = ct[0]
	}
	return r
}
func (r *Request) String(s string, ct ...string) *Request {
	r.bodyReader = strings.NewReader(s)
	if len(ct) > 0 {
		r.contentType = ct[0]
	} else {
		r.contentType = "text/plain; charset=utf-8"
	}
	return r
}
func (r *Request) Reader(rd io.Reader, ct ...string) *Request {
	r.bodyReader = rd
	if c, ok := rd.(io.Closer); ok {
		r.bodyCloser = c
	}
	if len(ct) > 0 {
		r.contentType = ct[0]
	}
	return r
}
func (r *Request) JSON(v any) *Request { r.body = v; r.contentType = "application/json"; return r }
func (r *Request) Form(values url.Values) *Request {
	r.bodyBytes = []byte(values.Encode())
	r.contentType = "application/x-www-form-urlencoded"
	return r
}

func (r *Request) Multipart(fields map[string]string, files ...UploadFile) *Request {
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	for k, v := range fields {
		_ = mw.WriteField(k, v)
	}
	for _, f := range files {
		_ = f.writeTo(mw)
	}
	_ = mw.Close()
	r.bodyBytes = b.Bytes()
	r.contentType = mw.FormDataContentType()
	return r
}

type UploadFile struct {
	FieldName, FileName, ContentType string
	Reader                           io.Reader
	Path                             string
}

func File(field, filename string, r io.Reader) UploadFile {
	return UploadFile{FieldName: field, FileName: filename, Reader: r}
}
func FilePath(field, p string) UploadFile {
	return UploadFile{FieldName: field, FileName: path.Base(p), Path: p}
}
func (f UploadFile) writeTo(w *multipart.Writer) error {
	var part io.Writer
	var err error
	if f.ContentType != "" {
		h := make(textproto.MIMEHeader)
		h.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, f.FieldName, f.FileName))
		h.Set("Content-Type", f.ContentType)
		part, err = w.CreatePart(h)
	} else {
		part, err = w.CreateFormFile(f.FieldName, f.FileName)
	}
	if err != nil {
		return err
	}
	if f.Path != "" {
		fh, err := os.Open(f.Path)
		if err != nil {
			return err
		}
		defer fh.Close()
		_, err = io.Copy(part, fh)
		return err
	}
	_, err = io.Copy(part, f.Reader)
	return err
}

func (r *Request) Method(ctx context.Context, method, u string) (*Response, error) {
	return r.Do(ctx, method, u)
}
func (r *Request) Send(ctx context.Context, method, u string) (*Response, error) {
	return r.Do(ctx, method, u)
}
func (r *Request) Get(ctx context.Context, u string) (*Response, error) {
	return r.Do(ctx, http.MethodGet, u)
}
func (r *Request) Head(ctx context.Context, u string) (*Response, error) {
	return r.Do(ctx, http.MethodHead, u)
}
func (r *Request) Options(ctx context.Context, u string) (*Response, error) {
	return r.Do(ctx, http.MethodOptions, u)
}
func (r *Request) Trace(ctx context.Context, u string) (*Response, error) {
	return r.Do(ctx, http.MethodTrace, u)
}
func (r *Request) Delete(ctx context.Context, u string) (*Response, error) {
	return r.Do(ctx, http.MethodDelete, u)
}
func (r *Request) Connect(ctx context.Context, u string) (*Response, error) {
	return r.Do(ctx, http.MethodConnect, u)
}
func (r *Request) Post(ctx context.Context, u string) (*Response, error) {
	return r.Do(ctx, http.MethodPost, u)
}
func (r *Request) Put(ctx context.Context, u string) (*Response, error) {
	return r.Do(ctx, http.MethodPut, u)
}
func (r *Request) Patch(ctx context.Context, u string) (*Response, error) {
	return r.Do(ctx, http.MethodPatch, u)
}

func (r *Request) Do(ctx context.Context, method, rawurl string) (*Response, error) {
	c := r.client
	defer c.releaseRequest(r)
	if ctx == nil {
		ctx = context.Background()
	}
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}
	finalURL, err := r.buildURL(rawurl)
	if err != nil {
		return nil, err
	}
	body, err := r.makeBody()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, finalURL, body)
	if err != nil {
		return nil, err
	}
	if r.bodyBytes != nil {
		b := r.bodyBytes
		req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
		req.ContentLength = int64(len(b))
	}
	if r.bodyLimit > 0 && req.ContentLength > r.bodyLimit {
		return nil, ErrClientBodyTooLarge
	}
	req.Header = cloneHeader(r.headers)
	if !r.noDefaultHeaders {
		if req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", c.cfg.UserAgent)
		}
		if r.contentType != "" && req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", r.contentType)
		}
		if req.Header.Get("Accept") == "" {
			req.Header.Set("Accept", "application/json, text/plain, */*")
		}
	}
	if r.idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", r.idempotencyKey)
	}
	for _, ck := range r.cookies {
		req.AddCookie(ck)
	}
	rt := c.hc.Transport
	if len(r.middleware) > 0 {
		for i := len(r.middleware) - 1; i >= 0; i-- {
			rt = r.middleware[i](rt)
		}
	}
	hc := *c.hc
	hc.Transport = rt
	policy := c.cfg.Retry
	if r.retry != nil {
		policy = *r.retry
	}
	res, err := doWithRetry(ctx, &hc, req, policy, c.cfg.Hooks)
	if err != nil {
		return nil, err
	}
	out := &Response{Raw: res, Request: req, started: time.Now(), limit: r.responseLimit, stream: r.stream}
	if out.limit == 0 {
		out.limit = c.cfg.ResponseBodyLimit
	}
	if res != nil && res.StatusCode >= 400 && r.errDecode != nil && res.Body != nil {
		_ = out.Decode(r.errDecode)
		return out, &ClientError{Kind: ClientErrStatus, Method: req.Method, URL: safeURL(req.URL), StatusCode: res.StatusCode, Err: ErrClientStatus}
	}
	if r.decode != nil && res != nil && res.Body != nil {
		if err := out.Decode(r.decode); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (r *Request) buildURL(raw string) (string, error) {
	if raw == "" {
		raw = r.rawURL
	}
	base := r.client.cfg.BaseURL
	var u *url.URL
	var err error
	if base != "" && !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		bu, err := url.Parse(base)
		if err != nil {
			return "", err
		}
		ref, err := url.Parse(raw)
		if err != nil {
			return "", err
		}
		u = bu.ResolveReference(ref)
	} else {
		u, err = url.Parse(raw)
		if err != nil {
			return "", err
		}
	}
	p := u.Path
	for k, v := range r.pathParams {
		p = strings.ReplaceAll(p, "{"+k+"}", url.PathEscape(v))
	}
	u.Path = p
	q := u.Query()
	for k, vs := range r.query {
		for _, v := range vs {
			q.Add(k, v)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (r *Request) makeBody() (io.Reader, error) {
	var rd io.Reader
	switch {
	case r.bodyReader != nil:
		rd = r.bodyReader
	case r.bodyBytes != nil:
		rd = bytes.NewReader(r.bodyBytes)
	case r.body != nil:
		if r.contentType == "" {
			r.contentType = "application/json"
		}
		if strings.Contains(r.contentType, "json") {
			b, err := CurrentJSONEngine().Marshal(r.body)
			if err != nil {
				return nil, err
			}
			r.bodyBytes = b
			rd = bytes.NewReader(b)
		} else if b, ok := r.body.([]byte); ok {
			rd = bytes.NewReader(b)
		} else if s, ok := r.body.(string); ok {
			rd = strings.NewReader(s)
		} else {
			return nil, fmt.Errorf("fh client: unsupported body type %T", r.body)
		}
	}
	limit := r.bodyLimit
	if limit == 0 {
		limit = r.client.cfg.BodyLimit
	}
	if limit > 0 && rd != nil {
		rd = io.LimitReader(rd, limit)
	}
	return rd, nil
}

// Response wraps http.Response with safe helpers.
type Response struct {
	Raw     *http.Response
	Request *http.Request
	body    []byte
	read    bool
	started time.Time
	limit   int64
	stream  bool
}

func (r *Response) StatusCode() int {
	if r == nil || r.Raw == nil {
		return 0
	}
	return r.Raw.StatusCode
}
func (r *Response) Header() http.Header {
	if r == nil || r.Raw == nil {
		return nil
	}
	return r.Raw.Header
}
func (r *Response) Cookies() []*http.Cookie {
	if r == nil || r.Raw == nil {
		return nil
	}
	return r.Raw.Cookies()
}
func (r *Response) IsSuccess() bool { c := r.StatusCode(); return c >= 200 && c < 300 }
func (r *Response) IsError() bool   { return r.StatusCode() >= 400 }
func (r *Response) Reader() io.ReadCloser {
	if r == nil || r.Raw == nil {
		return io.NopCloser(bytes.NewReader(nil))
	}
	return r.Raw.Body
}
func (r *Response) Bytes() ([]byte, error) {
	if r == nil || r.Raw == nil || r.Raw.Body == nil {
		return nil, nil
	}
	if r.read {
		return r.body, nil
	}
	if r.stream {
		return nil, ErrClientBodyStreamed
	}
	defer r.Raw.Body.Close()
	var rd io.Reader = r.Raw.Body
	if r.limit > 0 {
		rd = io.LimitReader(rd, r.limit+1)
	}
	b, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	if r.limit > 0 && int64(len(b)) > r.limit {
		return nil, ErrClientBodyTooLarge
	}
	r.body = b
	r.read = true
	return b, nil
}
func (r *Response) String() (string, error) { b, err := r.Bytes(); return string(b), err }
func (r *Response) Decode(v any) error {
	b, err := r.Bytes()
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	return CurrentJSONEngine().Unmarshal(b, v)
}
func (r *Response) Save(path string, perm os.FileMode) error {
	b, err := r.Bytes()
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, perm)
}
func (r *Response) DrainAndClose() error {
	if r == nil || r.Raw == nil || r.Raw.Body == nil {
		return nil
	}
	_, _ = io.CopyN(io.Discard, r.Raw.Body, 64<<10)
	return r.Raw.Body.Close()
}

// RetryPolicy controls safe retries. By default only idempotent methods are retried.
type RetryPolicy struct {
	MaxAttempts   int
	MaxElapsed    time.Duration
	BaseDelay     time.Duration
	MaxDelay      time.Duration
	Jitter        bool
	RetryStatuses map[int]bool
	RetryMethods  map[string]bool
	RetryAfter    bool
	Predicate     func(*http.Response, error) bool
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, MaxElapsed: 15 * time.Second, BaseDelay: 50 * time.Millisecond, MaxDelay: 2 * time.Second, Jitter: true, RetryAfter: true, RetryStatuses: map[int]bool{408: true, 425: true, 429: true, 500: true, 502: true, 503: true, 504: true}, RetryMethods: map[string]bool{http.MethodGet: true, http.MethodHead: true, http.MethodPut: true, http.MethodDelete: true, http.MethodOptions: true, http.MethodTrace: true, MethodQuery: true, MethodSearch: true}}
}
func NoRetry() RetryPolicy { return RetryPolicy{MaxAttempts: 1} }

func doWithRetry(ctx context.Context, hc *http.Client, req *http.Request, p RetryPolicy, hooks ClientHooks) (*http.Response, error) {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	started := time.Now()
	var lastErr error
	var res *http.Response
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		clone := req.Clone(ctx)
		if req.GetBody != nil {
			b, err := req.GetBody()
			if err == nil {
				clone.Body = b
			}
		}
		res, lastErr = hc.Do(clone)
		if !shouldRetry(req, res, lastErr, p) || attempt == p.MaxAttempts {
			return res, lastErr
		}
		if res != nil && res.Body != nil {
			_, _ = io.CopyN(io.Discard, res.Body, 64<<10)
			_ = res.Body.Close()
		}
		delay := retryDelay(res, attempt, p)
		if p.MaxElapsed > 0 && time.Since(started)+delay > p.MaxElapsed {
			return res, lastErr
		}
		callClientHook(hooks.OnRetry, ClientEvent{At: time.Now(), Method: req.Method, URL: safeURL(req.URL), Attempt: attempt, Err: lastErr, StatusCode: statusOf(res), Duration: delay})
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return res, ctx.Err()
		case <-t.C:
		}
	}
	return res, lastErr
}
func shouldRetry(req *http.Request, res *http.Response, err error, p RetryPolicy) bool {
	if p.Predicate != nil {
		return p.Predicate(res, err)
	}
	if !p.RetryMethods[req.Method] && req.Header.Get("Idempotency-Key") == "" {
		return false
	}
	if err != nil {
		return true
	}
	return res != nil && p.RetryStatuses[res.StatusCode]
}
func retryDelay(res *http.Response, attempt int, p RetryPolicy) time.Duration {
	if p.RetryAfter && res != nil {
		if v := res.Header.Get("Retry-After"); v != "" {
			if sec, err := strconv.Atoi(v); err == nil {
				return time.Duration(sec) * time.Second
			}
			if tm, err := http.ParseTime(v); err == nil {
				if d := time.Until(tm); d > 0 {
					return d
				}
			}
		}
	}
	base := p.BaseDelay
	if base <= 0 {
		base = 50 * time.Millisecond
	}
	max := p.MaxDelay
	if max <= 0 {
		max = 2 * time.Second
	}
	d := time.Duration(float64(base) * math.Pow(2, float64(attempt-1)))
	if d > max {
		d = max
	}
	if p.Jitter && d > 0 {
		d = time.Duration(rand.Int63n(int64(d)))
	}
	return d
}

// RedirectPolicy controls redirects.
type RedirectPolicy struct {
	Max                   int
	SameHostOnly          bool
	HTTPSOnly             bool
	StripSensitiveHeaders bool
}

// ClientSecurity protects outbound calls against SSRF and unsafe redirects.
type ClientSecurity struct {
	Strict          bool
	AllowPrivateIPs bool
	AllowLocalhost  bool
	AllowedHosts    map[string]bool
	BlockedHosts    map[string]bool
	RequireHTTPS    bool
}

func (s ClientSecurity) Enabled() bool {
	return s.Strict || s.RequireHTTPS || len(s.AllowedHosts) > 0 || len(s.BlockedHosts) > 0
}
func (s ClientSecurity) Validate(u *url.URL) error {
	if u == nil {
		return ErrClientInvalidURL
	}
	host := strings.ToLower(u.Hostname())
	if s.RequireHTTPS && u.Scheme != "https" {
		return ErrClientHTTPSRequired
	}
	if len(s.AllowedHosts) > 0 && !s.AllowedHosts[host] {
		return ErrClientHostBlocked
	}
	if s.BlockedHosts[host] {
		return ErrClientHostBlocked
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return err
	}
	for _, ip := range ips {
		if s.Strict && !s.AllowLocalhost && ip.IsLoopback() {
			return ErrClientHostBlocked
		}
		if s.Strict && !s.AllowPrivateIPs && isPrivateIP(ip) {
			return ErrClientHostBlocked
		}
	}
	return nil
}
func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate() {
		return true
	}
	return false
}

// ClientHooks exposes modern lifecycle extension points.
type ClientHooks struct{ OnClientStart, OnClientClose, OnBeforeRequest, OnRequestSent, OnResponseHeaders, OnAfterResponse, OnRetry, OnRedirect, OnError, OnDNSStart, OnDNSDone, OnConnectStart, OnConnectDone, OnTLSHandshakeStart, OnTLSHandshakeDone, OnConnectionOpen, OnConnectionReuse func(ClientEvent) }

type ClientEvent struct {
	At                time.Time
	Method, URL, Host string
	StatusCode        int
	Attempt           int
	Duration          time.Duration
	Err               error
}

func callClientHook(f func(ClientEvent), e ClientEvent) {
	if f != nil {
		f(e)
	}
}

func (e ClientEvent) withErr(err error) ClientEvent            { e.Err = err; return e }
func (e ClientEvent) withHost(host string) ClientEvent         { e.Host = host; return e }
func (e ClientEvent) withStatus(s int) ClientEvent             { e.StatusCode = s; return e }
func (e ClientEvent) withDuration(d time.Duration) ClientEvent { e.Duration = d; return e }

// ClientMetrics is intentionally small and allocation-free for adapters.
type ClientMetrics interface {
	Inflight(delta int64)
	Observe(method, host string, status int, dur time.Duration)
	Error(method, host, kind string)
}
type NopClientMetrics struct{}

func (NopClientMetrics) Inflight(int64)                             {}
func (NopClientMetrics) Observe(string, string, int, time.Duration) {}
func (NopClientMetrics) Error(string, string, string)               {}

// ClientStatsMetrics is an in-memory low-overhead metrics collector.
type ClientStatsMetrics struct {
	InFlight atomic.Int64
	Requests atomic.Uint64
	Errors   atomic.Uint64
	BytesIn  atomic.Uint64
}

func (m *ClientStatsMetrics) Inflight(d int64)                           { m.InFlight.Add(d) }
func (m *ClientStatsMetrics) Observe(string, string, int, time.Duration) { m.Requests.Add(1) }
func (m *ClientStatsMetrics) Error(string, string, string)               { m.Errors.Add(1) }

// Built-in middlewares.
func ClientLogger(l Logger) ClientMiddleware {
	if l == nil {
		l = NewDefaultLogger()
	}
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			start := time.Now()
			res, err := next.RoundTrip(r)
			status := statusOf(res)
			if err != nil {
				l.Error("fh client request failed", "method", r.Method, "url", safeURL(r.URL), "status", status, "duration", time.Since(start), "error", err)
			} else {
				l.Info("fh client request", "method", r.Method, "url", safeURL(r.URL), "status", status, "duration", time.Since(start))
			}
			return res, err
		})
	}
}
func ClientHeader(k, v string) ClientMiddleware {
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get(k) == "" {
				r.Header.Set(k, v)
			}
			return next.RoundTrip(r)
		})
	}
}
func ClientBearer(token string) ClientMiddleware {
	return ClientHeader("Authorization", "Bearer "+token)
}
func ClientBasicAuth(user, pass string) ClientMiddleware {
	enc := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	return ClientHeader("Authorization", "Basic "+enc)
}
func ClientAPIKey(header, key string) ClientMiddleware { return ClientHeader(header, key) }
func ClientGzipRequest(min int) ClientMiddleware {
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if r.Body == nil || r.Header.Get("Content-Encoding") != "" {
				return next.RoundTrip(r)
			}
			var b bytes.Buffer
			gz := gzip.NewWriter(&b)
			_, err := io.Copy(gz, r.Body)
			if closeErr := gz.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
			_ = r.Body.Close()
			if err != nil {
				return nil, err
			}
			if b.Len() >= min {
				r.Body = io.NopCloser(bytes.NewReader(b.Bytes()))
				r.ContentLength = int64(b.Len())
				r.Header.Set("Content-Encoding", "gzip")
			}
			return next.RoundTrip(r)
		})
	}
}
func ClientHMACSigner(header, secret string) ClientMiddleware {
	if header == "" {
		header = "X-Signature"
	}
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			sig, err := signRequestHMAC(r, []byte(secret), sha256.New)
			if err != nil {
				return nil, err
			}
			r.Header.Set(header, sig)
			return next.RoundTrip(r)
		})
	}
}
func ClientIdempotency(provider func(*http.Request) string) ClientMiddleware {
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Idempotency-Key") == "" {
				if provider != nil {
					r.Header.Set("Idempotency-Key", provider(r))
				} else {
					r.Header.Set("Idempotency-Key", strconv.FormatInt(time.Now().UnixNano(), 36))
				}
			}
			return next.RoundTrip(r)
		})
	}
}

func ClientBulkhead(max int, wait time.Duration) ClientMiddleware {
	if max <= 0 {
		max = 1024
	}
	sem := make(chan struct{}, max)
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			timer := time.NewTimer(wait)
			if wait <= 0 {
				timer.Stop()
			}
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
				return next.RoundTrip(r)
			case <-r.Context().Done():
				if !timer.Stop() {
					<-timer.C
				}
				return nil, r.Context().Err()
			case <-timer.C:
				return nil, ErrClientBulkheadFull
			}
		})
	}
}
func ClientRateLimit(ratePerSecond int) ClientMiddleware {
	if ratePerSecond <= 0 {
		ratePerSecond = 1
	}
	interval := time.Second / time.Duration(ratePerSecond)
	var mu sync.Mutex
	last := time.Now()
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			mu.Lock()
			now := time.Now()
			wait := last.Add(interval).Sub(now)
			if wait > 0 {
				last = last.Add(interval)
			} else {
				last = now
			}
			mu.Unlock()
			if wait > 0 {
				t := time.NewTimer(wait)
				select {
				case <-r.Context().Done():
					t.Stop()
					return nil, r.Context().Err()
				case <-t.C:
				}
			}
			return next.RoundTrip(r)
		})
	}
}
func ClientBodyLimit(n int64) ClientMiddleware {
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if n > 0 && r.Body != nil && r.ContentLength > n {
				return nil, ErrClientBodyTooLarge
			}
			return next.RoundTrip(r)
		})
	}
}
func ClientRecover() ClientMiddleware {
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (res *http.Response, err error) {
			defer func() {
				if rec := recover(); rec != nil {
					err = &ClientError{Kind: ClientErrPanic, Method: r.Method, URL: safeURL(r.URL), Err: fmt.Errorf("panic: %v", rec)}
				}
			}()
			return next.RoundTrip(r)
		})
	}
}

// Circuit breaker middleware.
type CircuitConfig struct {
	FailureThreshold uint64
	RecoveryTimeout  time.Duration
	HalfOpenMax      uint64
}
type circuitState uint32

const (
	circuitClosed circuitState = iota
	circuitOpen
	circuitHalfOpen
)

type CircuitBreaker struct {
	cfg      CircuitConfig
	state    atomic.Uint32
	failures atomic.Uint64
	openedAt atomic.Int64
	probes   atomic.Uint64
}

func NewCircuitBreaker(cfg CircuitConfig) *CircuitBreaker {
	if cfg.FailureThreshold == 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.RecoveryTimeout <= 0 {
		cfg.RecoveryTimeout = 30 * time.Second
	}
	if cfg.HalfOpenMax == 0 {
		cfg.HalfOpenMax = 1
	}
	return &CircuitBreaker{cfg: cfg}
}
func (b *CircuitBreaker) Middleware() ClientMiddleware {
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if !b.allow() {
				return nil, ErrClientCircuitOpen
			}
			res, err := next.RoundTrip(r)
			if err != nil || (res != nil && res.StatusCode >= 500) {
				b.fail()
			} else {
				b.success()
			}
			return res, err
		})
	}
}
func (b *CircuitBreaker) allow() bool {
	st := circuitState(b.state.Load())
	if st == circuitClosed {
		return true
	}
	if st == circuitOpen {
		if time.Since(time.Unix(0, b.openedAt.Load())) >= b.cfg.RecoveryTimeout {
			b.state.Store(uint32(circuitHalfOpen))
			b.probes.Store(0)
		} else {
			return false
		}
	}
	return b.probes.Add(1) <= b.cfg.HalfOpenMax
}
func (b *CircuitBreaker) fail() {
	if b.failures.Add(1) >= b.cfg.FailureThreshold {
		b.state.Store(uint32(circuitOpen))
		b.openedAt.Store(time.Now().UnixNano())
	}
}
func (b *CircuitBreaker) success() {
	b.failures.Store(0)
	b.state.Store(uint32(circuitClosed))
	b.probes.Store(0)
}
func ClientCircuitBreaker(cfg CircuitConfig) ClientMiddleware {
	return NewCircuitBreaker(cfg).Middleware()
}

// Async and batch helpers.
type FutureResponse struct {
	done chan struct{}
	res  *Response
	err  error
}

func (f *FutureResponse) Await(ctx context.Context) (*Response, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.done:
		return f.res, f.err
	}
}
func (c *Client) Async(fn func(*Request) (*Response, error)) *FutureResponse {
	f := &FutureResponse{done: make(chan struct{})}
	go func() {
		defer close(f.done)
		defer func() {
			if r := recover(); r != nil {
				f.err = fmt.Errorf("httpclient: async panic: %v", r)
			}
		}()
		f.res, f.err = fn(c.R())
	}()
	return f
}

type BatchResult struct {
	Response *Response
	Error    error
	Index    int
}

func (c *Client) Batch(ctx context.Context, maxConcurrency int, fns ...func(*Request) (*Response, error)) []BatchResult {
	if maxConcurrency <= 0 {
		maxConcurrency = 8
	}
	sem := make(chan struct{}, maxConcurrency)
	out := make([]BatchResult, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		i, fn := i, fn
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					out[i] = BatchResult{Index: i, Error: fmt.Errorf("httpclient: batch panic: %v", r)}
				}
			}()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				out[i] = BatchResult{Index: i, Error: ctx.Err()}
				return
			}
			res, err := fn(c.R())
			out[i] = BatchResult{Index: i, Response: res, Error: err}
		}()
	}
	wg.Wait()
	return out
}

// Generic typed helpers.
func GetJSON[T any](ctx context.Context, c *Client, u string) (T, error) {
	var out T
	_, err := c.R().Decode(&out).Get(ctx, u)
	return out, err
}
func PostJSON[Req any, Res any](ctx context.Context, c *Client, u string, body Req) (Res, error) {
	var out Res
	_, err := c.R().JSON(body).Decode(&out).Post(ctx, u)
	return out, err
}
func PutJSON[Req any, Res any](ctx context.Context, c *Client, u string, body Req) (Res, error) {
	var out Res
	_, err := c.R().JSON(body).Decode(&out).Put(ctx, u)
	return out, err
}

// Errors.
type ClientErrorKind string

const (
	ClientErrNetwork  ClientErrorKind = "network"
	ClientErrTimeout  ClientErrorKind = "timeout"
	ClientErrTLS      ClientErrorKind = "tls"
	ClientErrRedirect ClientErrorKind = "redirect"
	ClientErrSecurity ClientErrorKind = "security"
	ClientErrProtocol ClientErrorKind = "protocol"
	ClientErrPanic    ClientErrorKind = "panic"
	ClientErrStatus   ClientErrorKind = "status"
)

type ClientError struct {
	Kind        ClientErrorKind
	Method, URL string
	StatusCode  int
	Duration    time.Duration
	Attempt     int
	Retryable   bool
	Err         error
}

func (e *ClientError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return "fh client: " + string(e.Kind) + ": " + e.Err.Error()
	}
	return "fh client: " + string(e.Kind)
}
func (e *ClientError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

var (
	ErrClientRedirectLimit   = errors.New("redirect limit reached")
	ErrClientRedirectBlocked = errors.New("redirect blocked by policy")
	ErrClientBodyTooLarge    = errors.New("body too large")
	ErrClientBodyStreamed    = errors.New("response is streaming")
	ErrClientInvalidURL      = errors.New("invalid url")
	ErrClientHTTPSRequired   = errors.New("https required")
	ErrClientHostBlocked     = errors.New("host blocked by security policy")
	ErrClientCircuitOpen     = errors.New("circuit breaker open")
	ErrClientBulkheadFull    = errors.New("bulkhead capacity exhausted")
	ErrClientStatus          = errors.New("unexpected http status")
)

func wrapClientErr(req *http.Request, err error, d time.Duration) error {
	kind := ClientErrNetwork
	if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
		kind = ClientErrTimeout
	}
	if strings.Contains(strings.ToLower(err.Error()), "tls") {
		kind = ClientErrTLS
	}
	if errors.Is(err, ErrClientRedirectLimit) || errors.Is(err, ErrClientRedirectBlocked) {
		kind = ClientErrRedirect
	}
	return &ClientError{Kind: kind, Method: req.Method, URL: safeURL(req.URL), Duration: d, Err: err, Retryable: kind == ClientErrNetwork || kind == ClientErrTimeout}
}
func classifyNetErr(err error) string {
	if err == nil {
		return ""
	}
	if os.IsTimeout(err) {
		return "timeout"
	}
	if strings.Contains(strings.ToLower(err.Error()), "tls") {
		return "tls"
	}
	return "network"
}
func statusOf(r *http.Response) int {
	if r == nil {
		return 0
	}
	return r.StatusCode
}
func cloneHeader(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, vs := range h {
		cp := make([]string, len(vs))
		copy(cp, vs)
		out[k] = cp
	}
	return out
}
func stripSensitive(h http.Header) {
	for _, k := range []string{"Authorization", "Cookie", "Proxy-Authorization", "X-API-Key", "X-Signature"} {
		h.Del(k)
	}
}
func safeURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	cp := *u
	if cp.User != nil {
		cp.User = url.UserPassword(cp.User.Username(), "xxxxx")
	}
	q := cp.Query()
	for k := range q {
		lk := strings.ToLower(k)
		if strings.Contains(lk, "token") || strings.Contains(lk, "secret") || strings.Contains(lk, "password") || strings.Contains(lk, "key") {
			q.Set(k, "xxxxx")
		}
	}
	cp.RawQuery = q.Encode()
	return cp.String()
}
func signRequestHMAC(r *http.Request, secret []byte, newHash func() hash.Hash) (string, error) {
	h := hmac.New(newHash, secret)
	h.Write([]byte(r.Method))
	h.Write([]byte("\n"))
	h.Write([]byte(r.URL.RequestURI()))
	h.Write([]byte("\n"))
	if r.Body != nil {
		bodyReader := io.TeeReader(r.Body, h)
		b, err := io.ReadAll(bodyReader)
		if err != nil {
			return "", err
		}
		r.Body = io.NopCloser(bytes.NewReader(b))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
