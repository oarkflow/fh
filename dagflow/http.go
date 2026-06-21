package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

type HTTPApp struct {
	engine      *Engine
	routes      []compiledRoute
	middlewares map[string]fh.HandlerFunc
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
	app := &HTTPApp{engine: engine, middlewares: map[string]fh.HandlerFunc{}, global: append([]string(nil), cfg.GlobalMiddlewares...)}
	for _, mc := range cfg.Middlewares {
		mw, err := buildMiddleware(mc)
		if err != nil {
			return nil, err
		}
		if mc.When != "" || mc.Condition != "" {
			mw = conditionalMiddleware(engine, mc, mw)
		}
		app.middlewares[mc.ID] = mw
	}
	for _, name := range app.global {
		if app.middlewares[name] == nil {
			return nil, fmt.Errorf("global middleware %s not found", name)
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

// Register installs every BCL-defined route directly in fh's router.
func (a *HTTPApp) Register(app *fh.App) error {
	for i := range a.routes {
		cr := a.routes[i]
		handlers := make([]fh.HandlerFunc, 0, len(a.global)+len(cr.cfg.Middlewares)+1)
		for _, name := range append(append([]string{}, a.global...), cr.cfg.Middlewares...) {
			mw := a.middlewares[name]
			if mw == nil {
				return fmt.Errorf("route %s middleware %s not found", cr.cfg.ID, name)
			}
			handlers = append(handlers, mw)
		}
		handlers = append(handlers, func(c *fh.Ctx) error {
			params, ok := matchPath(cr.segments, c.Path())
			if !ok {
				return writeJSON(c, fh.StatusNotFound, map[string]any{"error": "route not found"})
			}
			if cr.cfg.When != "" || cr.cfg.Condition != "" {
				ok, err := a.engine.evalRouteCondition(cr.cfg, httpConditionFacts(c, params, nil, cr.cfg))
				if err != nil {
					return writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": "route condition failed", "detail": err.Error()})
				}
				if !ok {
					return writeJSON(c, fh.StatusNotFound, map[string]any{"error": "route not found"})
				}
			}
			return a.handleWorkflowRoute(c, cr.cfg, params)
		})
		app.Add(cr.cfg.Method, fhRoutePath(cr.cfg.Path), handlers...)
	}
	return nil
}

func fhRoutePath(path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}") {
			parts[i] = ":" + strings.TrimSuffix(strings.TrimPrefix(part, "{"), "}")
		}
	}
	return strings.Join(parts, "/")
}

func conditionalMiddleware(e *Engine, cfg MiddlewareConfig, mw fh.HandlerFunc) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		ok, err := e.evalMiddlewareCondition(cfg, c)
		if err != nil {
			return writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": "middleware condition failed", "middleware": cfg.ID, "detail": err.Error()})
		}
		if !ok {
			return c.Next()
		}
		return mw(c)
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
	for _, id := range rc.Workflows {
		if _, err := e.workflow(id); err != nil {
			return compiledRoute{}, err
		}
	}
	return compiledRoute{cfg: rc, segments: parsePath(rc.Path)}, nil
}

func (a *HTTPApp) handleWorkflowRoute(c *fh.Ctx, rc RouteConfig, params map[string]string) error {
	input, err := readInput(c, params, rc.Envelope)
	if err != nil {
		return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
	}
	if err := a.engine.ValidateAgainstSchema(rc.InputSchema, input); err != nil {
		return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": "schema validation failed", "detail": err.Error()})
	}
	ctx := c.Context()
	if rc.Workflow != "" {
		if key := c.Get("Idempotency-Key"); key != "" {
			if existing, ok, err := a.engine.handleIdempotency(ctx, key, rc.Workflow, input); err != nil {
				return writeJSON(c, fh.StatusConflict, map[string]any{"error": err.Error()})
			} else if ok {
				return writeJSON(c, fh.StatusOK, existing)
			}
		}
		if rc.Mode == RouteAsync || rc.Mode == RouteDetached {
			task, err := a.engine.RunAsync(ctx, rc.Workflow, input)
			a.engine.recordIdempotencyFromRequest(c, rc.Workflow, input, task)
			if err != nil {
				return writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusAccepted, task)
		}
		task, err := a.engine.RunSync(ctx, rc.Workflow, input)
		a.engine.recordIdempotencyFromRequest(c, rc.Workflow, input, task)
		if err != nil {
			return writeJSON(c, fh.StatusInternalServerError, taskOrError(task, err))
		}
		if task.Status == TaskWaiting || task.Status == TaskPaused {
			return writeJSON(c, fh.StatusAccepted, task)
		}
		return writeJSON(c, fh.StatusOK, task)
	}
	if rc.Chain != "" {
		if rc.Mode == RouteAsync || rc.Mode == RouteDetached {
			run, err := a.engine.RunChainAsync(ctx, rc.Chain, input)
			if err != nil {
				return writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": err.Error()})
			}
			return writeJSON(c, fh.StatusAccepted, run)
		}
		run, err := a.engine.RunChainSync(ctx, rc.Chain, input)
		if err != nil {
			return writeJSON(c, fh.StatusInternalServerError, chainOrError(run, err))
		}
		return writeJSON(c, fh.StatusOK, run)
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
		return writeJSON(c, fh.StatusAccepted, run)
	}
	run, err := a.engine.RunWorkflowIDsSync(ctx, rc.Workflows, input)
	if err != nil {
		return writeJSON(c, fh.StatusInternalServerError, chainOrError(run, err))
	}
	return writeJSON(c, fh.StatusOK, run)
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
	for _, seg := range pattern {
		if seg.wildcard != "" {
			name := seg.wildcard
			if name == "*" {
				name = "wildcard"
			}
			params[name] = strings.Join(parts[pi:], "/")
			return params, true
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
	}
	return params, pi == len(parts)
}

func readInput(c *fh.Ctx, params map[string]string, envelope bool) (any, error) {
	query := map[string]any{}
	if err := c.QueryParser(&query); err != nil {
		return nil, err
	}
	var body any = map[string]any{}
	if len(c.Body()) > 0 {
		dec := json.NewDecoder(strings.NewReader(string(c.Body())))
		dec.UseNumber()
		if err := dec.Decode(&body); err != nil {
			return nil, err
		}
	}
	if envelope {
		return map[string]any{"body": body, "path": params, "query": query, "method": c.Method()}, nil
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

func buildMiddleware(cfg MiddlewareConfig) (fh.HandlerFunc, error) {
	if cfg.ID == "" || cfg.Type == "" {
		return nil, errors.New("middleware requires id and type")
	}
	switch cfg.Type {
	case "recover":
		return func(c *fh.Ctx) (err error) {
			defer func() {
				if rec := recover(); rec != nil {
					err = writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": "panic recovered", "detail": fmt.Sprint(rec)})
				}
			}()
			return c.Next()
		}, nil
	case "logger":
		return func(c *fh.Ctx) error {
			start := time.Now()
			err := c.Next()
			log.Printf("%s %s %s", c.Method(), c.Path(), time.Since(start))
			return err
		}, nil
	case "request_id":
		return func(c *fh.Ctx) error {
			id := c.Get("X-Request-ID")
			if id == "" {
				id = newID("req")
			}
			c.Set("X-Request-ID", id)
			return c.Next()
		}, nil
	case "api_key":
		header, status, msg := cfg.Header, cfg.Status, cfg.Message
		if header == "" {
			header = "X-API-Key"
		}
		if status == 0 {
			status = fh.StatusUnauthorized
		}
		if msg == "" {
			msg = "unauthorized"
		}
		return func(c *fh.Ctx) error {
			if subtle.ConstantTimeCompare([]byte(c.Get(header)), []byte(cfg.Value)) != 1 {
				return writeJSON(c, status, map[string]any{"error": msg})
			}
			return c.Next()
		}, nil
	case "max_body":
		max := cfg.MaxBytes
		if max <= 0 {
			max = 1 << 20
		}
		return func(c *fh.Ctx) error {
			if int64(len(c.Body())) > max {
				return writeJSON(c, fh.StatusPayloadTooLarge, map[string]any{"error": "request body too large"})
			}
			return c.Next()
		}, nil
	case "cors":
		return func(c *fh.Ctx) error {
			c.Set("Access-Control-Allow-Origin", "*")
			c.Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, X-Request-ID")
			c.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			if c.Method() == "OPTIONS" {
				return c.SendStatus(fh.StatusNoContent)
			}
			return c.Next()
		}, nil
	case "rate_limit":
		limit := cfg.Limit
		if limit <= 0 {
			limit = 60
		}
		window, err := parseDuration(cfg.Window)
		if err != nil {
			return nil, err
		}
		if window <= 0 {
			window = time.Minute
		}
		rl := newRateLimiter(limit, window)
		return func(c *fh.Ctx) error {
			if !rl.allow(clientIP(c)) {
				c.Set("Retry-After", fmt.Sprintf("%.0f", window.Seconds()))
				return writeJSON(c, fh.StatusTooManyRequests, map[string]any{"error": "rate limit exceeded"})
			}
			return c.Next()
		}, nil
	default:
		return nil, fmt.Errorf("unsupported middleware type %s", cfg.Type)
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
