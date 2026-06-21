package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type HTTPApp struct {
	engine      *Engine
	routes      []compiledRoute
	middlewares map[string]func(http.Handler) http.Handler
	global      []string
}
type compiledRoute struct {
	cfg      RouteConfig
	segments []pathSegment
}
type pathSegment struct {
	literal  string
	param    string
	wildcard string
}

func NewHTTPApp(engine *Engine, cfg *Config) (*HTTPApp, error) {
	app := &HTTPApp{engine: engine, middlewares: map[string]func(http.Handler) http.Handler{}, global: append([]string(nil), cfg.GlobalMiddlewares...)}
	for _, mc := range cfg.Middlewares {
		mw, err := buildMiddleware(engine, mc)
		if err != nil {
			return nil, err
		}
		if mc.When != "" || mc.Condition != "" {
			mw = conditionalMiddleware(engine, mc, mw)
		}
		app.middlewares[mc.ID] = mw
	}
	for _, gc := range app.global {
		if app.middlewares[gc] == nil {
			return nil, fmt.Errorf("global middleware %s not found", gc)
		}
	}
	for _, rc := range FlattenRoutes(cfg) {
		cr, err := compileRoute(rc, engine)
		if err != nil {
			return nil, err
		}
		app.routes = append(app.routes, cr)
	}
	return app, nil
}

func conditionalMiddleware(e *Engine, cfg MiddlewareConfig, mw func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		wrapped := mw(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ok, err := e.evalMiddlewareCondition(cfg, r)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "middleware condition failed", "middleware": cfg.ID, "detail": err.Error()})
				return
			}
			if !ok {
				next.ServeHTTP(w, r)
				return
			}
			wrapped.ServeHTTP(w, r)
		})
	}
}

func compileRoute(rc RouteConfig, e *Engine) (compiledRoute, error) {
	if rc.ID == "" {
		return compiledRoute{}, errors.New("route id is required")
	}
	if rc.Method == "" || rc.Path == "" {
		return compiledRoute{}, fmt.Errorf("route %s requires method and path", rc.ID)
	}
	rc.Method = strings.ToUpper(rc.Method)
	if rc.Mode == "" {
		rc.Mode = RouteSync
	}
	if rc.Mode != RouteSync && rc.Mode != RouteAsync && rc.Mode != RouteDetached && rc.Mode != RouteStream && rc.Mode != RouteWebhook {
		return compiledRoute{}, fmt.Errorf("route %s unsupported mode %s", rc.ID, rc.Mode)
	}
	targets := 0
	if rc.Workflow != "" {
		targets++
	}
	if rc.Chain != "" {
		targets++
	}
	if len(rc.Workflows) > 0 {
		targets++
	}
	if targets != 1 {
		return compiledRoute{}, fmt.Errorf("route %s must define exactly one of workflow, chain, workflows", rc.ID)
	}
	if rc.Workflow != "" {
		if _, err := e.workflow(rc.Workflow); err != nil {
			return compiledRoute{}, err
		}
	}
	if rc.Chain != "" {
		if _, err := e.chain(rc.Chain); err != nil {
			return compiledRoute{}, err
		}
	}
	if len(rc.Workflows) > 0 {
		for _, id := range rc.Workflows {
			if _, err := e.workflow(id); err != nil {
				return compiledRoute{}, err
			}
		}
	}
	return compiledRoute{cfg: rc, segments: parsePath(rc.Path)}, nil
}

func (a *HTTPApp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, cr := range a.routes {
		if r.Method != cr.cfg.Method {
			continue
		}
		params, ok := matchPath(cr.segments, r.URL.Path)
		if !ok {
			continue
		}
		if cr.cfg.When != "" || cr.cfg.Condition != "" {
			facts := httpConditionFacts(r, params, nil, cr.cfg)
			ok, err := a.engine.evalRouteCondition(cr.cfg, facts)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "route condition failed", "detail": err.Error()})
				return
			}
			if !ok {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": "route not found"})
				return
			}
		}
		var h http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { a.handleWorkflowRoute(w, r, cr.cfg, params) })
		chainNames := append([]string(nil), a.global...)
		chainNames = append(chainNames, cr.cfg.Middlewares...)
		for i := len(chainNames) - 1; i >= 0; i-- {
			mw := a.middlewares[chainNames[i]]
			if mw == nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "middleware not found", "name": chainNames[i]})
				return
			}
			h = mw(h)
		}
		h.ServeHTTP(w, r)
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]any{"error": "route not found"})
}

func (a *HTTPApp) handleWorkflowRoute(w http.ResponseWriter, r *http.Request, rc RouteConfig, params map[string]string) {
	input, err := readInput(r, params, rc.Envelope)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := a.engine.ValidateAgainstSchema(rc.InputSchema, input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "schema validation failed", "detail": err.Error()})
		return
	}
	if rc.Workflow != "" {
		if key := r.Header.Get("Idempotency-Key"); key != "" {
			if existing, ok, err := a.engine.handleIdempotency(r.Context(), key, rc.Workflow, input); err != nil {
				writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
				return
			} else if ok {
				writeJSON(w, http.StatusOK, existing)
				return
			}
		}
		if rc.Mode == RouteAsync || rc.Mode == RouteDetached {
			task, err := a.engine.RunAsync(r.Context(), rc.Workflow, input)
			a.engine.recordIdempotencyFromRequest(r, rc.Workflow, input, task)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusAccepted, task)
			return
		}
		task, err := a.engine.RunSync(r.Context(), rc.Workflow, input)
		a.engine.recordIdempotencyFromRequest(r, rc.Workflow, input, task)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, taskOrError(task, err))
			return
		}
		if task.Status == TaskWaiting || task.Status == TaskPaused {
			writeJSON(w, http.StatusAccepted, task)
			return
		}
		writeJSON(w, http.StatusOK, task)
		return
	}
	if rc.Chain != "" {
		if rc.Mode == RouteAsync || rc.Mode == RouteDetached {
			run, err := a.engine.RunChainAsync(r.Context(), rc.Chain, input)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusAccepted, run)
			return
		}
		run, err := a.engine.RunChainSync(r.Context(), rc.Chain, input)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, chainOrError(run, err))
			return
		}
		writeJSON(w, http.StatusOK, run)
		return
	}
	if rc.Mode == RouteAsync || rc.Mode == RouteDetached {
		run := newChainRun("adhoc", rc.Workflows, input)
		_ = a.engine.chainStore.Create(run)
		go func() {
			current := input
			var runErr error
			for _, id := range rc.Workflows {
				task, err := a.engine.RunSync(context.Background(), id, current)
				run.Tasks = append(run.Tasks, task)
				run.UpdatedAt = time.Now()
				_ = a.engine.chainStore.Save(run)
				if err != nil {
					runErr = err
					break
				}
				current = task.Result
			}
			if runErr == nil {
				run.Result = current
			}
			a.engine.finishChain(run, runErr)
		}()
		writeJSON(w, http.StatusAccepted, run)
		return
	}
	run, err := a.engine.RunWorkflowIDsSync(r.Context(), rc.Workflows, input)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, chainOrError(run, err))
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func parsePath(path string) []pathSegment {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	parts := strings.Split(path, "/")
	out := make([]pathSegment, 0, len(parts))
	for _, p := range parts {
		seg := pathSegment{}
		switch {
		case p == "*":
			seg.wildcard = "*"
		case strings.HasPrefix(p, "*"):
			seg.wildcard = strings.TrimPrefix(p, "*")
			if seg.wildcard == "" {
				seg.wildcard = "*"
			}
		case strings.HasPrefix(p, ":"):
			seg.param = strings.TrimPrefix(p, ":")
		case strings.HasPrefix(p, "{") && strings.HasSuffix(p, "}"):
			seg.param = strings.TrimSuffix(strings.TrimPrefix(p, "{"), "}")
		default:
			seg.literal = p
		}
		out = append(out, seg)
	}
	return out
}

func matchPath(pattern []pathSegment, actual string) (map[string]string, bool) {
	actual = strings.Trim(actual, "/")
	var parts []string
	if actual != "" {
		parts = strings.Split(actual, "/")
	}
	params := map[string]string{}
	pi := 0
	for si, seg := range pattern {
		if seg.wildcard != "" {
			name := seg.wildcard
			if name == "*" {
				name = "wildcard"
			}
			if pi <= len(parts) {
				params[name] = strings.Join(parts[pi:], "/")
			}
			return trueMap(params), true
		}
		if pi >= len(parts) {
			return nil, false
		}
		if seg.param != "" {
			params[seg.param] = parts[pi]
			pi++
			continue
		}
		if seg.literal != parts[pi] {
			return nil, false
		}
		pi++
		if si == len(pattern)-1 && pi != len(parts) {
			return nil, false
		}
	}
	if pi != len(parts) {
		return nil, false
	}
	return params, true
}

func trueMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

func readInput(r *http.Request, params map[string]string, envelope bool) (any, error) {
	query := map[string]any{}
	for k, v := range r.URL.Query() {
		if len(v) == 1 {
			query[k] = v[0]
		} else {
			query[k] = v
		}
	}
	var body any = map[string]any{}
	if r.Body != nil && r.Body != http.NoBody {
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		if err := dec.Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
	}
	if envelope {
		return map[string]any{"body": body, "path": params, "query": query, "method": r.Method}, nil
	}
	if len(params) > 0 || len(query) > 0 {
		if m, ok := body.(map[string]any); ok {
			for k, v := range params {
				m[k] = v
			}
			for k, v := range query {
				if _, exists := m[k]; !exists {
					m[k] = v
				}
			}
			return m, nil
		}
	}
	return body, nil
}

func buildMiddleware(e *Engine, c MiddlewareConfig) (func(http.Handler) http.Handler, error) {
	if c.ID == "" || c.Type == "" {
		return nil, errors.New("middleware requires id and type")
	}
	switch c.Type {
	case "recover":
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer func() {
					if rec := recover(); rec != nil {
						writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "panic recovered", "detail": fmt.Sprint(rec)})
					}
				}()
				next.ServeHTTP(w, r)
			})
		}, nil
	case "logger":
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				start := time.Now()
				next.ServeHTTP(w, r)
				log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
			})
		}, nil
	case "request_id":
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				id := r.Header.Get("X-Request-ID")
				if id == "" {
					id = newID("req")
				}
				w.Header().Set("X-Request-ID", id)
				next.ServeHTTP(w, r)
			})
		}, nil
	case "api_key":
		header := c.Header
		if header == "" {
			header = "X-API-Key"
		}
		status := c.Status
		if status == 0 {
			status = http.StatusUnauthorized
		}
		msg := c.Message
		if msg == "" {
			msg = "unauthorized"
		}
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got := r.Header.Get(header)
				if subtle.ConstantTimeCompare([]byte(got), []byte(c.Value)) != 1 {
					writeJSON(w, status, map[string]any{"error": msg})
					return
				}
				next.ServeHTTP(w, r)
			})
		}, nil
	case "max_body":
		maxBytes := c.MaxBytes
		if maxBytes <= 0 {
			maxBytes = 1 << 20
		}
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
				next.ServeHTTP(w, r)
			})
		}, nil
	case "cors":
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, X-Request-ID")
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
				if r.Method == http.MethodOptions {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				next.ServeHTTP(w, r)
			})
		}, nil
	case "rate_limit":
		limit := c.Limit
		if limit <= 0 {
			limit = 60
		}
		window, err := parseDuration(c.Window)
		if err != nil {
			return nil, err
		}
		if window <= 0 {
			window = time.Minute
		}
		rl := newRateLimiter(limit, window)
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ip := clientIP(r)
				if !rl.allow(ip) {
					w.Header().Set("Retry-After", fmt.Sprintf("%.0f", window.Seconds()))
					writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "rate limit exceeded"})
					return
				}
				next.ServeHTTP(w, r)
			})
		}, nil
	default:
		return nil, fmt.Errorf("unsupported middleware type %s", c.Type)
	}
}

type rateLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	buckets map[string]*rateBucket
}
type rateBucket struct {
	count int
	reset time.Time
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{limit: limit, window: window, buckets: map[string]*rateBucket{}}
}
func (r *rateLimiter) allow(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	b := r.buckets[key]
	if b == nil || now.After(b.reset) {
		r.buckets[key] = &rateBucket{count: 1, reset: now.Add(r.window)}
		return true
	}
	if b.count >= r.limit {
		return false
	}
	b.count++
	return true
}

func taskOrError(task *Task, err error) any {
	if task != nil {
		return task
	}
	return map[string]any{"error": err.Error()}
}
func chainOrError(run *ChainRun, err error) any {
	if run != nil {
		return run
	}
	return map[string]any{"error": err.Error()}
}
