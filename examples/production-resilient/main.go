package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/bodylimit"
	"github.com/oarkflow/fh/mw/circuitbreaker"
	"github.com/oarkflow/fh/mw/coalesce"
	"github.com/oarkflow/fh/mw/favicon"
	"github.com/oarkflow/fh/mw/metrics"
	"github.com/oarkflow/fh/mw/ratelimiter"
	"github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/requestid"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/timeout"
	"github.com/oarkflow/fh/mw/trailingslash"
)

//go:embed favicon.ico
var sampleFavicon []byte

// ServerConfig defines operational thresholds for the production service.
type ServerConfig struct {
	Port            string
	RateLimitMax    int
	RateLimitWindow time.Duration
	BodyLimitBytes  int
	BreakerFailures int
	BreakerReset    time.Duration
}

// DefaultServerConfig returns production-hardened defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Port:            ":8080",
		RateLimitMax:    100, // 100 requests per window per IP
		RateLimitWindow: time.Second,
		BodyLimitBytes:  1 << 20, // 1 MiB ingress limit
		BreakerFailures: 3,       // Trip after 3 consecutive failures
		BreakerReset:    1 * time.Second,
	}
}

// ApplicationState holds live operational components for observability and testing.
type ApplicationState struct {
	Breaker       *circuitbreaker.Breaker
	Metrics       *metrics.Metrics
	SimulateFault atomic.Bool
	BackendHits   atomic.Uint64
}

// OrderRequest represents an incoming transactional payload.
type OrderRequest struct {
	Item     string  `json:"item"`
	Quantity int     `json:"quantity"`
	Amount   float64 `json:"amount"`
}

// BuildApp constructs the production-grade application with all defense-in-depth layers.
func BuildApp(cfg ServerConfig) (*fh.App, *ApplicationState) {
	// 1. Initialize core with Production profile (hardened write/read deadlines, bounded limits)
	app := fh.NewProduction(
		fh.WithMaxRequestBodySize(cfg.BodyLimitBytes),
	)

	state := &ApplicationState{
		Metrics: metrics.New(),
	}

	// 2. Circuit Breaker for downstream integration
	state.Breaker = circuitbreaker.New(circuitbreaker.Config{
		FailureThreshold: cfg.BreakerFailures,
		ResetAfter:       cfg.BreakerReset,
	})

	// -------------------------------------------------------------------------
	// Defense-in-depth Middleware Pipeline
	// -------------------------------------------------------------------------

	// A. Panic Recovery: intercepts panics, logs stack, prevents process termination
	app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
	}))

	// B. Enterprise Security Headers: HSTS, X-Frame-Options: DENY, X-Content-Type-Options: nosniff
	app.Use(security.New())

	// C. High-Entropy Request ID: attaches X-Request-ID to logs and responses
	app.Use(requestid.New(requestid.Config{
		TrustIncoming: false,
	}))

	// D. High-Speed Favicon: served from memory with ETag validation & 304 caching
	app.Use(favicon.New(favicon.Config{
		Data: sampleFavicon,
	}))

	// E. Trailing Slash Normalization: transparent in-flight rewrite (zero extra roundtrips)
	app.Use(trailingslash.RemoveSlash(false))

	// F. Body Limit Guard: rejects payloads > limit with HTTP 413 Payload Too Large
	app.Use(bodylimit.New(cfg.BodyLimitBytes))

	// G. Sliding-Window Rate Limiter: throttles abusive IPs with HTTP 429 and Retry-After
	// System routes (/health/*, /metrics, /favicon.ico) are exempt from client throttling
	app.Use(ratelimiter.New(ratelimiter.Config{
		Max:         cfg.RateLimitMax,
		Window:      cfg.RateLimitWindow,
		SendHeaders: true,
		Skip: func(c fh.Ctx) bool {
			p := c.Path()
			return p == "/health/live" || p == "/health/ready" || p == "/metrics" || p == "/favicon.ico"
		},
	}))

	// H. RED Observability: in-process rate, error, and latency distribution metrics
	app.Use(state.Metrics.Middleware())

	// -------------------------------------------------------------------------
	// Health & Telemetry Routes
	// -------------------------------------------------------------------------

	// Kubernetes Liveness Probe: detects deadlock or internal server unresponsiveness
	app.Get("/health/live", func(c fh.Ctx) error {
		return c.JSON(fh.Map{
			"status": "alive",
			"time":   time.Now().UTC().Format(time.RFC3339),
		})
	})

	// Kubernetes Readiness Probe: checks downstream health, circuit breaker, and drain status
	app.Get("/health/ready", func(c fh.Ctx) error {
		if app.IsDraining() {
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{
				"status": "draining",
			})
		}
		breakerState := state.Breaker.State().String()
		if state.Breaker.State() == circuitbreaker.StateOpen {
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{
				"status":  "degraded",
				"breaker": breakerState,
			})
		}
		return c.JSON(fh.Map{
			"status":  "ready",
			"breaker": breakerState,
		})
	})

	// Prometheus Metrics Scrape Endpoint (?format=prometheus or JSON)
	app.Get("/metrics", state.Metrics.Handler())

	// -------------------------------------------------------------------------
	// Business Endpoints
	// -------------------------------------------------------------------------

	// 1. Coalesced Catalog (SingleFlight): prevents cache stampedes / thundering herd
	// Under high concurrency, only one request hits the backend; others receive identical responses.
	app.Get("/api/v1/catalog",
		coalesce.New(coalesce.Config{TTL: 2 * time.Second}),
		func(c fh.Ctx) error {
			// Simulate expensive database/cache fetch
			state.BackendHits.Add(1)
			time.Sleep(30 * time.Millisecond)

			return c.JSON(fh.Map{
				"source":    "coalesced_cache",
				"items":     []string{"enterprise-router", "tamper-proof-audit", "circuit-breaker"},
				"timestamp": time.Now().UnixNano(),
			})
		},
	)

	// 2. Transactional Order Creation (Idempotent & Schema Validated)
	app.Post("/api/v1/orders", func(c fh.Ctx) error {
		var req OrderRequest
		if err := json.Unmarshal(c.Body(), &req); err != nil {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{
				"error":   "INVALID_JSON",
				"message": "Malformed request body",
			})
		}
		if req.Item == "" || req.Quantity <= 0 || req.Amount <= 0 {
			return c.Status(fh.StatusUnprocessableEntity).JSON(fh.Map{
				"error":   "VALIDATION_ERROR",
				"message": "item, positive quantity, and positive amount are required",
			})
		}

		idempotencyKey := c.Get(fh.HeaderIdempotencyKey)
		return c.Status(fh.StatusCreated).JSON(fh.Map{
			"order_id":        "ORD-98214",
			"status":          "confirmed",
			"item":            req.Item,
			"quantity":        req.Quantity,
			"total":           req.Amount,
			"idempotency_key": idempotencyKey,
		})
	})

	// 3. Downstream External Dependency guarded by Circuit Breaker & Timeout
	app.Get("/api/v1/external-service",
		state.Breaker.Handler(),
		timeout.New(2*time.Second),
		func(c fh.Ctx) error {
			if state.SimulateFault.Load() {
				// Simulating upstream dependency failure
				return fh.NewHTTPError(fh.StatusBadGateway, "UPSTREAM_DOWN", "downstream partner returned 502")
			}
			return c.JSON(fh.Map{
				"service": "partner_payment_gateway",
				"status":  "operational",
			})
		},
	)

	// 4. Chaos Fault Injection (to demonstrate live circuit breaker tripping and recovery)
	app.Post("/api/v1/fault-injection", func(c fh.Ctx) error {
		var body struct {
			Fault bool `json:"fault"`
		}
		if err := json.Unmarshal(c.Body(), &body); err != nil {
			return c.Status(fh.StatusBadRequest).SendString("invalid body")
		}
		state.SimulateFault.Store(body.Fault)
		return c.JSON(fh.Map{
			"fault_injected": state.SimulateFault.Load(),
			"breaker_state":  state.Breaker.State().String(),
		})
	})

	// 5. Panic Recovery Demonstration (returns 500 without crashing daemon)
	app.Get("/api/v1/panic", func(c fh.Ctx) error {
		panic("critical unexpected exception in application handler")
	})

	return app, state
}

func main() {
	cfg := DefaultServerConfig()
	if envPort := os.Getenv("PORT"); envPort != "" {
		if len(envPort) > 0 && envPort[0] != ':' {
			cfg.Port = ":" + envPort
		} else {
			cfg.Port = envPort
		}
	}
	app, _ := BuildApp(cfg)

	// Graceful Shutdown on SIGINT/SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		log.Println("[SHUTDOWN] Signal received, gracefully draining in-flight requests...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := app.ShutdownWithContext(ctx); err != nil {
			log.Printf("[SHUTDOWN] Error during shutdown: %v", err)
		}
		log.Println("[SHUTDOWN] Server cleanly stopped.")
	}()

	log.Printf("[STARTUP] fh resilient production server starting on %s", cfg.Port)
	if err := app.Listen(cfg.Port); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[FATAL] Listen error: %v", err)
	}
}
