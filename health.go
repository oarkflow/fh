package fh

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type HealthCheckResult struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // "ok" or "error"
	Latency string `json:"latency,omitempty"`
	Error   string `json:"error,omitempty"`
}

type registeredHealthCheck struct {
	name    string
	timeout time.Duration
	fn      func(context.Context) error
	mu      sync.Mutex
	run     *healthRun
}

type healthRun struct {
	done chan struct{}
	err  error // published by closing done
}

type HealthProbe func() bool

type HealthConfig struct {
	Probes map[string]HealthProbe
	// Timeout bounds each legacy probe. Zero uses two seconds.
	Timeout time.Duration
}

type HealthResponse struct {
	Status string          `json:"status"`
	Probes map[string]bool `json:"probes,omitempty"`
}

// AddHealthCheck adds a named health check function with a timeout to App.
func (a *App) AddHealthCheck(name string, timeout time.Duration, fn func(context.Context) error) *App {
	a.healthMu.Lock()
	defer a.healthMu.Unlock()
	for _, hc := range a.healthChecks {
		if hc.name == name {
			hc.mu.Lock()
			hc.timeout, hc.fn = timeout, fn
			hc.mu.Unlock()
			return a
		}
	}
	a.healthChecks = append(a.healthChecks, &registeredHealthCheck{name: name, timeout: timeout, fn: fn})
	return a
}

// HealthStatus runs all registered health checks and returns overall readiness and individual results.
func (a *App) HealthStatus(ctx context.Context) (bool, []HealthCheckResult) {
	if ctx == nil {
		ctx = context.Background()
	}
	a.healthMu.RLock()
	checks := append([]*registeredHealthCheck(nil), a.healthChecks...)
	a.healthMu.RUnlock()

	if len(checks) == 0 {
		return true, nil
	}

	results := make([]HealthCheckResult, len(checks))
	// Each check writes a distinct result slot, but the failure state is shared. Keep the
	// aggregation atomic so concurrent probes remain race-free without adding
	// a lock to the (common) result writes.
	var wg sync.WaitGroup

	for i, hc := range checks {
		wg.Add(1)
		go func(idx int, c *registeredHealthCheck) {
			defer wg.Done()
			start := time.Now()
			err := a.runHealthCheck(ctx, c)
			dur := time.Since(start).String()
			if err != nil {
				results[idx] = HealthCheckResult{
					Name:    c.name,
					Status:  "error",
					Latency: dur,
					Error:   err.Error(),
				}
			} else {
				results[idx] = HealthCheckResult{
					Name:    c.name,
					Status:  "ok",
					Latency: dur,
				}
			}
		}(i, hc)
	}
	wg.Wait()
	for _, result := range results {
		if result.Status != "ok" {
			return false, results
		}
	}
	return true, results
}

// runHealthCheck shares an outstanding run between callers. A callback that
// ignores cancellation can occupy one slot, but repeated polls cannot create
// additional stuck callbacks for the same check.
func (a *App) runHealthCheck(ctx context.Context, check *registeredHealthCheck) error {
	if check == nil {
		return errors.New("health check is nil")
	}
	check.mu.Lock()
	fn := check.fn
	if fn == nil {
		check.mu.Unlock()
		return errors.New("health check is nil")
	}
	timeout := check.timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	run := check.run
	if run == nil {
		select {
		case a.healthSlots <- struct{}{}:
			run = &healthRun{done: make(chan struct{})}
			check.run = run
			go func() {
				defer func() {
					if p := recover(); p != nil {
						run.err = fmt.Errorf("health check panic: %v", p)
					}
					<-a.healthSlots
					check.mu.Lock()
					check.run = nil
					close(run.done)
					check.mu.Unlock()
				}()
				run.err = fn(ctx)
			}()
		default:
			check.mu.Unlock()
			return errors.New("health check capacity exhausted")
		}
	}
	check.mu.Unlock()
	select {
	case <-run.done:
		return run.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HealthCheck registers a health check endpoint at path with configured probes.
func (a *App) HealthCheck(path string, config HealthConfig) *App {
	checks := make(map[string]*registeredHealthCheck, len(config.Probes))
	for name, probe := range config.Probes {
		p := probe
		checks[name] = &registeredHealthCheck{name: name, timeout: config.Timeout, fn: func(context.Context) error {
			if p == nil || !p() {
				return errors.New("probe failed")
			}
			return nil
		}}
	}
	a.Get(path, func(c Ctx) error {
		results := make(map[string]bool, len(checks))
		allPass := true

		if len(checks) > 0 {
			var wg sync.WaitGroup
			var mu sync.Mutex
			for name, check := range checks {
				wg.Add(1)
				go func(n string, hc *registeredHealthCheck) {
					defer wg.Done()
					pass := a.runHealthCheck(c.Context(), hc) == nil
					mu.Lock()
					results[n] = pass
					if !pass {
						allPass = false
					}
					mu.Unlock()
				}(name, check)
			}
			wg.Wait()
		}

		resp := HealthResponse{
			Probes: results,
		}
		if allPass {
			resp.Status = "UP"
			c.Status(StatusOK)
		} else {
			resp.Status = "DOWN"
			c.Status(StatusServiceUnavailable)
		}

		return c.JSON(resp)
	})
	return a
}
