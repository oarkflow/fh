package fh

import (
	"context"
	"sync"
	"sync/atomic"
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
}

type HealthProbe func() bool

type HealthConfig struct {
	Probes map[string]HealthProbe
}

type HealthResponse struct {
	Status string          `json:"status"`
	Probes map[string]bool `json:"probes,omitempty"`
}

// AddHealthCheck adds a named health check function with a timeout to App.
func (a *App) AddHealthCheck(name string, timeout time.Duration, fn func(context.Context) error) *App {
	a.healthMu.Lock()
	defer a.healthMu.Unlock()
	for i, hc := range a.healthChecks {
		if hc.name == name {
			a.healthChecks[i] = registeredHealthCheck{name: name, timeout: timeout, fn: fn}
			return a
		}
	}
	a.healthChecks = append(a.healthChecks, registeredHealthCheck{name: name, timeout: timeout, fn: fn})
	return a
}

// HealthStatus runs all registered health checks and returns overall readiness and individual results.
func (a *App) HealthStatus(ctx context.Context) (bool, []HealthCheckResult) {
	if ctx == nil {
		ctx = context.Background()
	}
	a.healthMu.RLock()
	checks := append([]registeredHealthCheck(nil), a.healthChecks...)
	a.healthMu.RUnlock()

	if len(checks) == 0 {
		return true, nil
	}

	results := make([]HealthCheckResult, len(checks))
	// Each check writes a distinct result slot, but the failure state is shared. Keep the
	// aggregation atomic so concurrent probes remain race-free without adding
	// a lock to the (common) result writes.
	var failed atomic.Bool
	var wg sync.WaitGroup

	for i, hc := range checks {
		wg.Add(1)
		go func(idx int, c registeredHealthCheck) {
			defer wg.Done()
			start := time.Now()
			tCtx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()
			err := c.fn(tCtx)
			dur := time.Since(start).String()
			if err != nil {
				results[idx] = HealthCheckResult{
					Name:    c.name,
					Status:  "error",
					Latency: dur,
					Error:   err.Error(),
				}
				failed.Store(true)
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
	return !failed.Load(), results
}

// HealthCheck registers a health check endpoint at path with configured probes.
func (a *App) HealthCheck(path string, config HealthConfig) *App {
	a.Get(path, func(c Ctx) error {
		results := make(map[string]bool, len(config.Probes))
		allPass := true

		if len(config.Probes) > 0 {
			var wg sync.WaitGroup
			var mu sync.Mutex
			for name, probe := range config.Probes {
				if probe == nil {
					continue
				}
				wg.Add(1)
				go func(n string, p HealthProbe) {
					defer wg.Done()
					pass := p()
					mu.Lock()
					results[n] = pass
					if !pass {
						allPass = false
					}
					mu.Unlock()
				}(name, probe)
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
