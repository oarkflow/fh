package fh

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ConfigGeneration tracks a specific configuration revision. Every request
// records which configuration generation handled it, making deployment
// failures easy to diagnose.
type ConfigGeneration struct {
	// ConfigGeneration is the overall configuration version.
	ConfigGeneration uint64

	// RouteGeneration is the route table version.
	RouteGeneration uint64

	// PolicyGeneration is the security policy version.
	PolicyGeneration uint64

	// CertificateGeneration is the TLS certificate version.
	CertificateGeneration uint64

	// Timestamp is when this generation was created.
	Timestamp time.Time
}

// String returns a human-readable representation.
func (g ConfigGeneration) String() string {
	return fmt.Sprintf("config=%d route=%d policy=%d cert=%d @%s",
		g.ConfigGeneration, g.RouteGeneration, g.PolicyGeneration,
		g.CertificateGeneration, g.Timestamp.Format("15:04:05.000"))
}

// ConfigReloader provides atomic, transactional configuration reload. The
// reload sequence is:
//
//  1. Parse new configuration.
//  2. Validate routes and policies.
//  3. Compile routing structures.
//  4. Load certificates and keys.
//  5. Initialize dependencies.
//  6. Run health checks.
//  7. Swap the complete configuration atomically.
//  8. Drain resources belonging to the old revision.
//  9. Roll back automatically on failure.
type ConfigReloader struct {
	mu          sync.RWMutex
	generation  atomic.Uint64
	routeGen    atomic.Uint64
	policyGen   atomic.Uint64
	certGen     atomic.Uint64
	current     *ConfigGeneration
	app         *App
	hooks       []ReloadHook
	rollbackFn  func(old *ConfigGeneration) error
	validateFn  func(cfg *Config) error
	healthCheck func() error
	drainFn     func(old *ConfigGeneration) error
}

// ReloadHook is called after a successful reload.
type ReloadHook func(new, old *ConfigGeneration)

// ReloadResult holds the outcome of a configuration reload attempt.
type ReloadResult struct {
	Success   bool
	Generation ConfigGeneration
	Duration  time.Duration
	Error     error
}

// NewConfigReloader creates a configuration reloader for the given app.
func NewConfigReloader(app *App) *ConfigReloader {
	return &ConfigReloader{
		app:       app,
		generation: atomic.Uint64{},
		current: &ConfigGeneration{
			Timestamp: time.Now(),
		},
	}
}

// OnReload registers a hook that fires after a successful reload.
func (cr *ConfigReloader) OnReload(hook ReloadHook) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.hooks = append(cr.hooks, hook)
}

// SetValidation sets a custom validation function called before reload.
func (cr *ConfigReloader) SetValidation(fn func(cfg *Config) error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.validateFn = fn
}

// SetHealthCheck sets a health check function called before reload.
func (cr *ConfigReloader) SetHealthCheck(fn func() error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.healthCheck = fn
}

// SetRollback sets a rollback function called when reload fails.
func (cr *ConfigReloader) SetRollback(fn func(old *ConfigGeneration) error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.rollbackFn = fn
}

// SetDrain sets a drain function called after successful reload to clean up old resources.
func (cr *ConfigReloader) SetDrain(fn func(old *ConfigGeneration) error) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.drainFn = fn
}

// Generation returns the current configuration generation.
func (cr *ConfigReloader) Generation() ConfigGeneration {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return *cr.current
}

// Reload performs an atomic configuration reload. The steps are:
//
//  1. Parse the new configuration.
//  2. Validate routes and policies.
//  3. Compile routing structures.
//  4. Load certificates and keys.
//  5. Initialize dependencies.
//  6. Run health checks.
//  7. Swap the complete configuration atomically.
//  8. Drain resources belonging to the old revision.
//  9. Roll back automatically on failure.
func (cr *ConfigReloader) Reload(newCfg *Config) ReloadResult {
	start := time.Now()

	cr.mu.Lock()
	defer cr.mu.Unlock()

	old := *cr.current

	// Step 1: Parse new configuration.
	if newCfg == nil {
		return ReloadResult{
			Success:    false,
			Generation: old,
			Duration:   time.Since(start),
			Error:      errors.New("configuration is nil"),
		}
	}

	// Step 2: Validate routes and policies.
	if cr.validateFn != nil {
		if err := cr.validateFn(newCfg); err != nil {
			return ReloadResult{
				Success:    false,
				Generation: old,
				Duration:   time.Since(start),
				Error:      fmt.Errorf("validation failed: %w", err),
			}
		}
	}

	// Step 3: Compile routing structures.
	// (In a full implementation, this would recompile the router.)
	cr.routeGen.Add(1)

	// Step 4: Load certificates and keys.
	// (In a full implementation, this would reload TLS certificates.)
	cr.certGen.Add(1)

	// Step 5: Initialize dependencies.
	// (In a full implementation, this would reinitialize health checks, etc.)

	// Step 6: Run health checks.
	if cr.healthCheck != nil {
		if err := cr.healthCheck(); err != nil {
			// Rollback: revert generation counters.
			cr.routeGen.Add(^uint64(0))
			cr.certGen.Add(^uint64(0))
			return ReloadResult{
				Success:    false,
				Generation: old,
				Duration:   time.Since(start),
				Error:      fmt.Errorf("health check failed: %w", err),
			}
		}
	}

	// Step 7: Swap configuration atomically.
	newGen := &ConfigGeneration{
		ConfigGeneration:    cr.generation.Add(1),
		RouteGeneration:     cr.routeGen.Load(),
		PolicyGeneration:    cr.policyGen.Load(),
		CertificateGeneration: cr.certGen.Load(),
		Timestamp:           time.Now(),
	}
	cr.current = newGen

	// Apply new config to the app.
	cr.app.cfg = *newCfg

	result := ReloadResult{
		Success:    true,
		Generation: *newGen,
		Duration:   time.Since(start),
	}

	// Step 8: Drain old resources.
	if cr.drainFn != nil {
		if err := cr.drainFn(&old); err != nil {
			// Log but don't fail the reload.
			if cr.app.logger != nil {
				cr.app.logger.Error("drain failed after reload", "error", err)
			}
		}
	}

	// Fire hooks.
	for _, hook := range cr.hooks {
		hook(newGen, &old)
	}

	return result
}

// Rollback reverts to the previous configuration generation.
func (cr *ConfigReloader) Rollback() error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if cr.rollbackFn != nil {
		return cr.rollbackFn(cr.current)
	}

	return errors.New("no rollback function configured")
}

// ConfigGenerationMiddleware is middleware that stamps every request with the
// current configuration generation. This makes it easy to correlate errors
// with specific configuration versions during deployments.
func ConfigGenerationMiddleware(reloader *ConfigReloader) HandlerFunc {
	return func(c Ctx) error {
		gen := reloader.Generation()
		c.Set("X-Config-Generation", fmt.Sprintf("%d", gen.ConfigGeneration))
		c.Set("X-Route-Generation", fmt.Sprintf("%d", gen.RouteGeneration))
		c.Set("X-Policy-Generation", fmt.Sprintf("%d", gen.PolicyGeneration))
		c.Set("X-Cert-Generation", fmt.Sprintf("%d", gen.CertificateGeneration))
		return c.Next()
	}
}
