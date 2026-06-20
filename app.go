package fasthttp

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ── Lifecycle events ───────────────────────────────────────────────────────

// HookFunc is a lifecycle hook with optional error propagation.
type HookFunc func() error

// Hooks groups all application lifecycle hooks.
type Hooks struct {
	onListen   []HookFunc
	onShutdown []HookFunc
	onConnect  []func(net.Conn)
	onClose    []func(net.Conn)
	onError    []func(error)
}

// ── Config ─────────────────────────────────────────────────────────────────

// Config holds server configuration.
type Config struct {
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	IdleTimeout          time.Duration
	MaxConnections       int
	ReadBufferSize       int
	MaxRequestBodySize   int
	MaxHeaderListSize    int
	MaxConcurrentStreams uint32
	DisableKeepAlive     bool
	DisableHTTP2         bool
	ErrorHandler         func(*Ctx, error)
	Logger               *log.Logger
	TemplateEngine       TemplateEngine
}

var defaultConfig = Config{
	ReadTimeout:          10 * time.Second,
	WriteTimeout:         10 * time.Second,
	IdleTimeout:          60 * time.Second,
	ReadBufferSize:       16384,
	MaxRequestBodySize:   4 << 20,
	MaxHeaderListSize:    64 << 10,
	MaxConcurrentStreams: 128,
}

var ErrAppAlreadyStarted = errors.New("fasthttp: app has already been started")

// ── App ────────────────────────────────────────────────────────────────────

// App is the top-level application object. Create with New().
type App struct {
	cfg          Config
	router       *Router
	hooks        Hooks
	logger       *log.Logger
	middleware   []HandlerFunc
	sem          chan struct{}
	listener     net.Listener
	activeConn   sync.WaitGroup
	closed       atomic.Bool
	draining     atomic.Bool
	connMu       sync.Mutex
	conns        map[net.Conn]*connState
	shutdownOnce sync.Once
	started      atomic.Bool
	buildMu      sync.Mutex
	groups       []*Group
}

type connState struct {
	active bool
	h2     *h2Conn
}

// New creates a new App with optional config.
func New(config ...Config) *App {
	cfg := defaultConfig
	if len(config) > 0 {
		c := config[0]
		if c.ReadTimeout > 0 {
			cfg.ReadTimeout = c.ReadTimeout
		}
		if c.WriteTimeout > 0 {
			cfg.WriteTimeout = c.WriteTimeout
		}
		if c.IdleTimeout > 0 {
			cfg.IdleTimeout = c.IdleTimeout
		}
		if c.ReadBufferSize > 0 {
			cfg.ReadBufferSize = c.ReadBufferSize
		}
		if c.MaxConnections > 0 {
			cfg.MaxConnections = c.MaxConnections
		}
		if c.MaxRequestBodySize > 0 {
			cfg.MaxRequestBodySize = c.MaxRequestBodySize
		}
		if c.MaxHeaderListSize > 0 {
			cfg.MaxHeaderListSize = c.MaxHeaderListSize
		}
		if c.MaxConcurrentStreams > 0 {
			cfg.MaxConcurrentStreams = c.MaxConcurrentStreams
		}
		cfg.DisableKeepAlive = c.DisableKeepAlive
		cfg.DisableHTTP2 = c.DisableHTTP2
		cfg.ErrorHandler = c.ErrorHandler
		cfg.Logger = c.Logger
		cfg.TemplateEngine = c.TemplateEngine
	}
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = defaultErrorHandler
	}

	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}

	app := &App{
		cfg:    cfg,
		router: newRouter(),
		logger: logger,
		conns:  make(map[net.Conn]*connState),
	}

	if cfg.MaxConnections > 0 {
		app.sem = make(chan struct{}, cfg.MaxConnections)
	}

	return app
}

// ── Routing methods ────────────────────────────────────────────────────────

func (a *App) Add(method, path string, handlers ...HandlerFunc) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	if len(handlers) == 0 {
		panic("fasthttp: route requires at least one handler")
	}
	for _, handler := range handlers {
		if handler == nil {
			panic("fasthttp: nil route handler")
		}
	}
	a.router.Add(method, path, a.chain(handlers))
	return a
}

func (a *App) Get(path string, handlers ...HandlerFunc) *App {
	return a.Add("GET", path, handlers...)
}

func (a *App) Post(path string, handlers ...HandlerFunc) *App {
	return a.Add("POST", path, handlers...)
}

func (a *App) Put(path string, handlers ...HandlerFunc) *App {
	return a.Add("PUT", path, handlers...)
}

func (a *App) Delete(path string, handlers ...HandlerFunc) *App {
	return a.Add("DELETE", path, handlers...)
}

func (a *App) Patch(path string, handlers ...HandlerFunc) *App {
	return a.Add("PATCH", path, handlers...)
}

func (a *App) Head(path string, handlers ...HandlerFunc) *App {
	return a.Add("HEAD", path, handlers...)
}

func (a *App) Options(path string, handlers ...HandlerFunc) *App {
	return a.Add("OPTIONS", path, handlers...)
}

func (a *App) Connect(path string, handlers ...HandlerFunc) *App {
	return a.Add("CONNECT", path, handlers...)
}
func (a *App) Trace(path string, handlers ...HandlerFunc) *App {
	return a.Add("TRACE", path, handlers...)
}

func (a *App) All(path string, handlers ...HandlerFunc) *App {
	for _, m := range []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE"} {
		a.Add(m, path, handlers...)
	}
	return a
}

// Use registers global middleware (applied to all routes).
func (a *App) Use(handlers ...HandlerFunc) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.middleware = append(a.middleware, handlers...)
	return a
}

// Group creates a route group with a shared prefix and optional middleware.
func (a *App) Group(prefix string, handlers ...HandlerFunc) *Group {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	g := &Group{app: a, prefix: prefix, middleware: handlers}
	a.groups = append(a.groups, g)
	return g
}

// ── Lifecycle hooks ────────────────────────────────────────────────────────

func (a *App) OnListen(fn HookFunc) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.hooks.onListen = append(a.hooks.onListen, fn)
	return a
}

func (a *App) OnShutdown(fn HookFunc) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.hooks.onShutdown = append(a.hooks.onShutdown, fn)
	return a
}

func (a *App) OnConnect(fn func(net.Conn)) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.hooks.onConnect = append(a.hooks.onConnect, fn)
	return a
}

func (a *App) OnClose(fn func(net.Conn)) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.hooks.onClose = append(a.hooks.onClose, fn)
	return a
}

func (a *App) OnError(fn func(error)) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	a.hooks.onError = append(a.hooks.onError, fn)
	return a
}

// ── Listen ─────────────────────────────────────────────────────────────────

func (a *App) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return a.Serve(ln)
}

// ListenTLS serves HTTPS using the standard library TLS stack.
func (a *App) ListenTLS(addr, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return a.ServeTLS(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
}

// ServeTLS wraps ln with TLS and advertises HTTP/2 through ALPN when enabled.
func (a *App) ServeTLS(ln net.Listener, config *tls.Config) error {
	if config == nil {
		return errors.New("nil TLS config")
	}
	cfg := config.Clone()
	if len(cfg.NextProtos) == 0 {
		if a.cfg.DisableHTTP2 {
			cfg.NextProtos = []string{"http/1.1"}
		} else {
			cfg.NextProtos = []string{"h2", "http/1.1"}
		}
	}
	return a.Serve(tls.NewListener(ln, cfg))
}

func (a *App) Serve(ln net.Listener) error {
	a.buildMu.Lock()
	if !a.started.CompareAndSwap(false, true) {
		a.buildMu.Unlock()
		return ErrAppAlreadyStarted
	}
	a.buildMu.Unlock()
	a.connMu.Lock()
	a.listener = ln
	a.connMu.Unlock()
	a.closed.Store(false)
	a.draining.Store(false)

	for _, fn := range a.hooks.onListen {
		if err := fn(); err != nil {
			_ = ln.Close()
			return err
		}
	}

	a.logger.Printf("[fasthttp] Listening on %s", ln.Addr())

	var acceptErr error
	for {
		conn, err := ln.Accept()
		if err != nil {
			if a.closed.Load() {
				break
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				a.logger.Printf("[fasthttp] accept timeout: %v", err)
				continue
			}
			acceptErr = err
			_ = a.beginShutdown()
			break
		}

		if a.sem != nil {
			select {
			case a.sem <- struct{}{}:
			default:
				_ = writeAll(conn, serverError503)
				_ = conn.Close()
				continue
			}
		}

		a.connMu.Lock()
		if a.draining.Load() {
			a.connMu.Unlock()
			_ = conn.Close()
			if a.sem != nil {
				<-a.sem
			}
			continue
		}
		a.activeConn.Add(1)
		a.conns[conn] = &connState{}
		a.connMu.Unlock()
		go a.serveConn(conn)
	}

	a.activeConn.Wait()

	a.runShutdownHooks()

	return acceptErr
}

func (a *App) assertMutable() {
	if a.started.Load() {
		panic(ErrAppAlreadyStarted)
	}
}

func (a *App) Shutdown() error {
	err := a.beginShutdown()
	a.activeConn.Wait()
	a.runShutdownHooks()
	return err
}

func (a *App) ShutdownWithTimeout(d time.Duration) error {
	if err := a.beginShutdown(); err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		a.activeConn.Wait()
		close(done)
	}()
	select {
	case <-done:
		a.runShutdownHooks()
		return nil
	case <-time.After(d):
		a.closeAllConnections()
		return errors.New("shutdown timed out")
	}
}

func (a *App) beginShutdown() error {
	a.closed.Store(true)
	a.draining.Store(true)
	var err error
	a.connMu.Lock()
	listener := a.listener
	a.connMu.Unlock()
	if listener != nil {
		err = listener.Close()
	}
	var h2conns []*h2Conn
	a.connMu.Lock()
	for conn, state := range a.conns {
		if state.h2 != nil {
			h2conns = append(h2conns, state.h2)
		} else if !state.active {
			_ = conn.Close()
		}
	}
	a.connMu.Unlock()
	for _, h2c := range h2conns {
		h2c.startDrain()
	}
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (a *App) closeAllConnections() {
	a.connMu.Lock()
	for conn := range a.conns {
		_ = conn.Close()
	}
	a.connMu.Unlock()
}

func (a *App) runShutdownHooks() {
	a.shutdownOnce.Do(func() {
		for _, fn := range a.hooks.onShutdown {
			if err := fn(); err != nil {
				a.logger.Printf("[fasthttp] shutdown hook error: %v", err)
			}
		}
	})
}

func (a *App) setConnActive(conn net.Conn, active bool) {
	a.connMu.Lock()
	if state := a.conns[conn]; state != nil {
		state.active = active
	}
	a.connMu.Unlock()
}

func (a *App) setH2Conn(conn net.Conn, h2c *h2Conn) {
	a.connMu.Lock()
	if state := a.conns[conn]; state != nil {
		state.active, state.h2 = true, h2c
	}
	a.connMu.Unlock()
}

// ── Connection handler ─────────────────────────────────────────────────────

func (a *App) serveConn(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			a.logger.Printf("[fasthttp] connection hook panic: %v\n%s", r, debug.Stack())
		}
		conn.Close()
		a.connMu.Lock()
		delete(a.conns, conn)
		a.connMu.Unlock()
		for _, fn := range a.hooks.onClose {
			func() {
				defer func() {
					if r := recover(); r != nil {
						a.logger.Printf("[fasthttp] close hook panic: %v", r)
					}
				}()
				fn(conn)
			}()
		}
		if a.sem != nil {
			<-a.sem
		}
		a.activeConn.Done()
	}()

	for _, fn := range a.hooks.onConnect {
		fn(conn)
	}
	if tc, ok := conn.(*tls.Conn); ok && !a.cfg.DisableHTTP2 {
		_ = tc.SetDeadline(time.Now().Add(a.cfg.ReadTimeout))
		if err := tc.Handshake(); err != nil {
			if !isExpectedConnErr(err) {
				a.emitError(err)
			}
			return
		}
		_ = tc.SetDeadline(time.Time{})
		if tc.ConnectionState().NegotiatedProtocol == "h2" {
			h2c := newH2Conn(a, conn)
			a.setH2Conn(conn, h2c)
			h2c.serve(nil, false)
			return
		}
	}

	rawBuf := getBuf(a.cfg.ReadBufferSize)
	defer putBuf(rawBuf)
	buf := *rawBuf

	accumulated := buf[:0]

	for {
		if err := conn.SetReadDeadline(time.Now().Add(a.cfg.IdleTimeout)); err != nil {
			return
		}

		headEnd := findHeaderEnd(accumulated)
		for headEnd < 0 {
			if len(accumulated) == cap(buf) {
				conn.Write([]byte("HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
				return
			}

			if err := conn.SetReadDeadline(time.Now().Add(a.cfg.ReadTimeout)); err != nil {
				return
			}

			n, err := conn.Read(buf[len(accumulated):cap(buf)])
			if n > 0 {
				accumulated = buf[:len(accumulated)+n]
			}
			if err != nil {
				if err != io.EOF && !isExpectedConnErr(err) {
					a.emitError(err)
				}
				return
			}

			if !a.cfg.DisableHTTP2 && len(accumulated) <= len(h2ClientPreface) && bytes.Equal(accumulated, h2ClientPreface[:len(accumulated)]) {
				if len(accumulated) < len(h2ClientPreface) {
					continue
				}
				h2c := newH2Conn(a, conn)
				a.setH2Conn(conn, h2c)
				h2c.serve(nil, true)
				return
			}
			if !a.cfg.DisableHTTP2 && len(accumulated) > len(h2ClientPreface) && bytes.Equal(accumulated[:len(h2ClientPreface)], h2ClientPreface) {
				h2c := newH2Conn(a, conn)
				a.setH2Conn(conn, h2c)
				h2c.serve(accumulated[len(h2ClientPreface):], true)
				return
			}
			headEnd = findHeaderEnd(accumulated)
		}
		a.setConnActive(conn, true)

		// ── Parse request head ────────────────────────────────────────
		ctx := acquireCtx(conn, a)

		consumed, err := parseRequestLine(accumulated, &ctx.Header)
		if err != nil {
			releaseCtx(ctx)
			conn.Write(serverError400)
			return
		}

		_, err = parseHeaders(accumulated[consumed:headEnd+4], &ctx.Header)
		if err != nil {
			releaseCtx(ctx)
			conn.Write(serverError400)
			return
		}
		if ctx.Header.UnsupportedTransferEncoding {
			releaseCtx(ctx)
			_ = writeAll(conn, serverError501)
			return
		}
		if ctx.Header.ContentLength > a.cfg.MaxRequestBodySize {
			releaseCtx(ctx)
			_ = writeAll(conn, serverError413)
			return
		}
		expect := trimOWS(ctx.Header.Peek([]byte("Expect")))
		if len(expect) > 0 {
			if !strEqFold(expect, "100-continue") || (!ctx.Header.Chunked && ctx.Header.ContentLength == 0) {
				releaseCtx(ctx)
				_ = writeAll(conn, serverError417)
				return
			}
			if err := writeAll(conn, []byte("HTTP/1.1 100 Continue\r\n\r\n")); err != nil {
				releaseCtx(ctx)
				return
			}
		}
		bodyStart := headEnd + 4
		bodyLen := ctx.Header.ContentLength
		var nextData []byte
		chunkedBody := ctx.Header.Chunked

		if chunkedBody {
			body, leftover, trailers, readErr := readChunkedBody(conn, accumulated[bodyStart:], a.cfg.MaxRequestBodySize, a.cfg.ReadTimeout)
			if readErr != nil {
				releaseCtx(ctx)
				if errors.Is(readErr, ErrBodyTooLarge) {
					_ = writeAll(conn, serverError413)
				} else {
					_ = writeAll(conn, serverError400)
				}
				return
			}
			ctx.body, ctx.trailers, nextData = body, trailers, leftover
		} else if bodyLen > 0 {
			messageEnd := bodyStart + bodyLen
			if messageEnd <= cap(buf) {
				for len(accumulated) < messageEnd {
					if err := conn.SetReadDeadline(time.Now().Add(a.cfg.ReadTimeout)); err != nil {
						releaseCtx(ctx)
						return
					}
					n, err := conn.Read(buf[len(accumulated):cap(buf)])
					if n > 0 {
						accumulated = buf[:len(accumulated)+n]
					}
					if err != nil {
						if len(accumulated) >= messageEnd {
							break
						}
						if err != io.EOF && !isExpectedConnErr(err) {
							a.emitError(err)
						}
						releaseCtx(ctx)
						return
					}
				}
				ctx.body = accumulated[bodyStart:messageEnd]
			} else {
				grown := make([]byte, messageEnd)
				copy(grown, accumulated)
				if len(accumulated) < messageEnd {
					if _, err := io.ReadFull(conn, grown[len(accumulated):]); err != nil {
						releaseCtx(ctx)
						return
					}
				}
				buf = grown
				accumulated = grown
				ctx.body = grown[bodyStart:messageEnd]
			}
		}
		if chunkedBody {
			ctx.upgradeBuffered = nextData
		} else {
			nextStart := bodyStart + bodyLen
			if nextStart < len(accumulated) {
				ctx.upgradeBuffered = accumulated[nextStart:]
			}
		}
		if a.draining.Load() {
			ctx.Header.KeepAlive = false
		}

		if err := conn.SetWriteDeadline(time.Now().Add(a.cfg.WriteTimeout)); err != nil {
			releaseCtx(ctx)
			return
		}

		a.dispatch(ctx)
		keepAlive := ctx.Header.KeepAlive && !ctx.forceClose && !ctx.upgraded && !a.cfg.DisableKeepAlive && !a.draining.Load()
		upgraded := ctx.upgraded

		releaseCtx(ctx)

		if upgraded || !keepAlive {
			return
		}

		if chunkedBody {
			if len(nextData) > cap(buf) {
				buf = make([]byte, len(nextData))
			}
			copy(buf, nextData)
			accumulated = buf[:len(nextData)]
		} else {
			nextStart := bodyStart + bodyLen
			if nextStart < len(accumulated) {
				copy(buf, accumulated[nextStart:])
				accumulated = buf[:len(accumulated)-nextStart]
			} else {
				accumulated = buf[:0]
			}
		}
		a.setConnActive(conn, false)
	}
}

func (a *App) dispatch(ctx *Ctx) {
	defer func() {
		if r := recover(); r != nil {
			a.logger.Printf("[fasthttp] panic: %v\n%s", r, debug.Stack())
			if !ctx.responded {
				_ = ctx.Status(500).SendString("Internal Server Error")
			}
		}
	}()

	path := ctx.path()
	handler := a.router.FindBytes(ctx.Header.Method, path, &ctx.params)
	if handler == nil && bytesEqualFold(ctx.Header.Method, MethodHEAD) {
		ctx.params = ctx.params[:0]
		handler = a.router.FindBytes(MethodGET, path, &ctx.params)
	}

	if handler == nil {
		ctx.params = ctx.params[:0]
		allowed := a.router.Allowed(path)
		if bytesEqualFold(ctx.Header.Method, MethodOPTIONS) && len(path) == 1 && path[0] == '*' {
			allowed = a.router.Methods()
		}
		fallback := func(ctx *Ctx) error {
			if len(allowed) == 0 {
				return ctx.Status(404).SendString("404 Not Found")
			}
			ctx.Set("Allow", strings.Join(allowed, ", "))
			if bytesEqualFold(ctx.Header.Method, MethodOPTIONS) {
				return ctx.SendStatus(204)
			}
			return ctx.Status(405).SendString("405 Method Not Allowed")
		}
		if len(a.middleware) > 0 {
			handler = a.chain([]HandlerFunc{fallback})
		} else {
			handler = fallback
		}
	}

	if err := handler(ctx); err != nil {
		if !ctx.responded {
			a.cfg.ErrorHandler(ctx, err)
		}
	} else if !ctx.responded {
		_ = ctx.SendStatus(200)
	}
}

// chain combines global middleware + route-specific handlers into one HandlerFunc.
// For the common case (no middleware, single handler), returns handler directly — zero alloc.
func (a *App) chain(handlers []HandlerFunc) HandlerFunc {
	if len(a.middleware) == 0 && len(handlers) == 1 {
		return handlers[0]
	}

	all := make([]HandlerFunc, 0, len(a.middleware)+len(handlers))
	all = append(all, a.middleware...)
	all = append(all, handlers...)

	return func(ctx *Ctx) error {
		ctx.handlers = all
		ctx.handlerIndex = 0
		return ctx.Next()
	}
}

// ── Helpers ────────────────────────────────────────────────────────────────

func findHeaderEnd(b []byte) int {
	for i := 0; i < len(b)-3; i++ {
		if b[i] == '\r' && b[i+1] == '\n' && b[i+2] == '\r' && b[i+3] == '\n' {
			return i
		}
	}
	return -1
}

func (a *App) emitError(err error) {
	for _, fn := range a.hooks.onError {
		fn(err)
	}
}

func isExpectedConnErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout() || errors.Is(err, net.ErrClosed)
}

func defaultErrorHandler(ctx *Ctx, err error) {
	ctx.Status(500).SendString("Internal Server Error: " + err.Error())
}

// Pre-allocated 400 error response
var serverError400 = []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var serverError413 = []byte("HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var serverError417 = []byte("HTTP/1.1 417 Expectation Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var serverError501 = []byte("HTTP/1.1 501 Not Implemented\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var serverError503 = []byte("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
var plainTextCT = []byte("text/plain; charset=utf-8")
