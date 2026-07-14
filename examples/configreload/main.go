package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/fh"
)

func main() {
	app := fh.New()

	reloader := fh.NewConfigReloader(app)

	// Register reload hooks
	reloader.OnReload(func(new, old *fh.ConfigGeneration) {
		fmt.Printf("Config reloaded: %s -> %s (took %s)\n",
			old, new, time.Since(new.Timestamp))
	})

	// Set validation function
	reloader.SetValidation(func(cfg *fh.Config) error {
		if cfg.ReadTimeout <= 0 {
			return fmt.Errorf("read_timeout must be positive")
		}
		if cfg.MaxRequestBodySize <= 0 {
			return fmt.Errorf("max_request_body_size must be positive")
		}
		return nil
	})

	// Set health check
	reloader.SetHealthCheck(func() error {
		// Verify dependencies are healthy after reload
		return nil
	})

	// Apply config generation headers
	app.Use(fh.ConfigGenerationMiddleware(reloader))

	app.Get("/", func(c fh.Ctx) error {
		gen := reloader.Generation()
		return c.JSON(fh.Map{
			"message": "hello",
			"config_generation": gen.ConfigGeneration,
			"route_generation":  gen.RouteGeneration,
			"policy_generation": gen.PolicyGeneration,
			"cert_generation":   gen.CertificateGeneration,
		})
	})

	// Admin endpoint to trigger reload
	app.Post("/admin/reload", func(c fh.Ctx) error {
		newCfg := fh.Config{
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			MaxRequestBodySize: 4 << 20,
			Environment:       fh.EnvProduction,
		}

		result := reloader.Reload(&newCfg)
		if result.Success {
			return c.JSON(fh.Map{
				"status":     "reloaded",
				"generation": result.Generation.String(),
				"duration":   result.Duration.String(),
			})
		}
		return c.Status(fh.StatusInternalServerError).JSON(fh.Map{
			"error": result.Error.Error(),
		})
	})

	// Admin endpoint to view current generation
	app.Get("/admin/generation", func(c fh.Ctx) error {
		gen := reloader.Generation()
		return c.JSON(gen)
	})

	fmt.Println("Config reload example on :3000")
	fmt.Println("  GET  /               - shows config generation")
	fmt.Println("  POST /admin/reload   - atomic config reload")
	fmt.Println("  GET  /admin/generation - current generation info")
	app.Listen(":3000")
}
