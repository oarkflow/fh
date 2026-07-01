package fh

import (
	"context"
	"time"
)

// HealthCheck validates one dependency or invariant needed for readiness. Checks
// should be fast and respect ctx deadlines.
type HealthCheck func(context.Context) error

type registeredHealthCheck struct {
	Name    string
	Timeout time.Duration
	Check   HealthCheck
}

// HealthCheckResult is returned by /ready and RuntimeInfo dependency checks.
type HealthCheckResult struct {
	Name      string        `json:"name"`
	Status    string        `json:"status"`
	Latency   time.Duration `json:"latency"`
	Error     string        `json:"error,omitempty"`
	CheckedAt time.Time     `json:"checked_at"`
}

// AddHealthCheck registers a readiness dependency check. It is safe to call
// before or after startup. Duplicate names replace the previous check.
func (a *App) AddHealthCheck(name string, timeout time.Duration, check HealthCheck) *App {
	if a == nil || name == "" || check == nil {
		return a
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	a.healthMu.Lock()
	defer a.healthMu.Unlock()
	for i := range a.healthChecks {
		if a.healthChecks[i].Name == name {
			a.healthChecks[i] = registeredHealthCheck{Name: name, Timeout: timeout, Check: check}
			return a
		}
	}
	a.healthChecks = append(a.healthChecks, registeredHealthCheck{Name: name, Timeout: timeout, Check: check})
	return a
}

// HealthStatus runs registered checks and returns readiness status.
func (a *App) HealthStatus(ctx context.Context) (bool, []HealthCheckResult) {
	if a == nil {
		return false, nil
	}
	a.healthMu.RLock()
	checks := make([]registeredHealthCheck, len(a.healthChecks))
	copy(checks, a.healthChecks)
	a.healthMu.RUnlock()
	ready := !a.IsDraining()
	results := make([]HealthCheckResult, 0, len(checks))
	for _, chk := range checks {
		started := time.Now()
		checkCtx, cancel := context.WithTimeout(ctx, chk.Timeout)
		err := chk.Check(checkCtx)
		cancel()
		res := HealthCheckResult{Name: chk.Name, Status: "ok", Latency: time.Since(started), CheckedAt: time.Now().UTC()}
		if err != nil {
			res.Status = "error"
			res.Error = err.Error()
			ready = false
		}
		results = append(results, res)
	}
	return ready, results
}
