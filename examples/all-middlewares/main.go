package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/actor"
	"github.com/oarkflow/fh/mw/apikey"
	"github.com/oarkflow/fh/mw/apiversion"
	"github.com/oarkflow/fh/mw/basicauth"
	"github.com/oarkflow/fh/mw/bodylimit"
	cachemw "github.com/oarkflow/fh/mw/cache"
	"github.com/oarkflow/fh/mw/circuitbreaker"
	"github.com/oarkflow/fh/mw/compress"
	"github.com/oarkflow/fh/mw/contract"
	"github.com/oarkflow/fh/mw/correlationid"
	"github.com/oarkflow/fh/mw/cors"
	"github.com/oarkflow/fh/mw/csrf"
	"github.com/oarkflow/fh/mw/earlydata"
	"github.com/oarkflow/fh/mw/idempotency"
	"github.com/oarkflow/fh/mw/ipwhitelist"
	"github.com/oarkflow/fh/mw/lifecycle"
	"github.com/oarkflow/fh/mw/logger"
	"github.com/oarkflow/fh/mw/metrics"
	"github.com/oarkflow/fh/mw/policy"
	"github.com/oarkflow/fh/mw/proxy"
	"github.com/oarkflow/fh/mw/ratelimiter"
	"github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/reliability"
	"github.com/oarkflow/fh/mw/replay"
	"github.com/oarkflow/fh/mw/requestid"
	"github.com/oarkflow/fh/mw/rewrite"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/mw/signature"
	"github.com/oarkflow/fh/mw/skip"
	staticmw "github.com/oarkflow/fh/mw/static"
	"github.com/oarkflow/fh/mw/timeout"
	"github.com/oarkflow/fh/mw/workflow"
)

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	upstream := flag.String("upstream", "http://127.0.0.1:4000", "gateway upstream")
	flag.Parse()

	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{
		Enabled:            true,
		DataDir:            env("FH_DATA_DIR", ".fh-data"),
		JournalEnabled:     true,
		IdempotencyEnabled: true,
		QueueEnabled:       true,
		QueueWorkers:       2,
	}))
	requests := metrics.New()
	sessions := session.NewMemoryStore(10 * time.Minute)
	defer sessions.StopGC()
	sessionManager := session.NewSessionManager(sessions,
		session.SessionCookieName("fh_session"),
		session.SessionSecret([]byte(env("SESSION_SECRET", "change-me-session-secret-at-least-32-bytes!"))),
		session.SessionMaxAge(8*time.Hour),
		session.SessionHTTPOnly(true),
		session.SessionSameSite(fh.SameSiteLax),
	)

	// ── Global middleware stack ─────────────────────────────────────────

	app.Use(recover.New(recover.Config{EnableStackTrace: true}))
	app.Use(requestid.New(requestid.Config{TrustIncoming: true}))
	app.Use(correlationid.New(correlationid.Config{TrustIncoming: true}))
	app.Use(requests.Middleware())
	app.Use(security.New())

	// TLS 1.3 0-RTT early data: reject replay-prone unsafe requests.
	app.Use(earlydata.New(earlydata.Config{AllowWithIdempotencyKey: true}))

	// Global body size ceiling (1 MiB), skipped for health and static.
	app.Use(skip.New(
		bodylimit.New(1<<20),
		skip.Any(skip.Health(), skip.Static()),
	))

	// Global request deadline.
	app.Use(timeout.New(10 * time.Second))

	// Structured JSON access logging, quiet on health checks.
	app.Use(logger.New(logger.Config{
		FormatName: "json",
		SkipPaths:  []string{"/health"},
	}))

	app.Use(cors.New(cors.Config{
		AllowOrigins: []string{"http://localhost:5173"},
		AllowMethods: []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders: []string{"Content-Type", "Authorization", "X-API-Key", "X-Request-ID", "X-CSRF-Token", "X-Nonce", "X-Signature", "Idempotency-Key"},
		MaxAge:       600,
	}))

	// Gzip compression for text responses over 256 bytes.
	app.Use(compress.New(compress.Config{MinSize: 256}))

	// Rewrite legacy paths before route matching.
	app.Use(rewrite.New(
		rewrite.Rule{From: "/old-api/*path", To: "/api/*path"},
	))

	// Rate limiter: 120 requests/minute, exempt health + static.
	app.Use(skip.New(
		ratelimiter.New(ratelimiter.Config{Max: 120, Window: time.Minute, SendHeaders: true}),
		skip.Any(skip.Health(), skip.Static()),
	))

	// Session middleware (signed cookies).
	app.Use(skip.New(
		session.New(sessionManager),
		skip.Paths("/demo/cache"),
	))

	// ── Routes ──────────────────────────────────────────────────────────

	// Health check.
	app.Get("/health", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"status": "ok"})
	})

	// Static file serving with ETag, caching, and compression.
	publicDir := filepath.Join(sourceDir(), "public")
	app.Get("/static/*", staticmw.New(publicDir, staticmw.Config{
		Root: publicDir, Prefix: "/static/",
		ETag: true, LastModified: true, MaxAge: time.Hour,
	}))

	// ── Request ID and Correlation ID ────────────────────────────────

	app.Get("/demo/request-id", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{
			"request_id":     c.Locals("request_id"),
			"correlation_id": c.Locals("correlationID"),
		})
	})

	// ── Security Headers ─────────────────────────────────────────────
	// The global security middleware sets headers on every response.
	app.Get("/demo/security-headers", func(c *fh.Ctx) error {
		return c.SendString("check response headers for X-Content-Type-Options, X-Frame-Options, etc.")
	})

	// ── Basic Auth ───────────────────────────────────────────────────
	app.Get("/demo/basic-auth",
		basicauth.New("admin", "password"),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"message": "authenticated", "user": "admin"})
		},
	)

	// ── API Key Auth ─────────────────────────────────────────────────
	app.Get("/demo/api-key",
		apikey.New(apikey.Config{Header: "X-API-Key", Keys: []string{"demo-key-123"}}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"message": "api key accepted"})
		},
	)

	// ── API Versioning ───────────────────────────────────────────────
	app.Get("/demo/api-version",
		apiversion.New(apiversion.Config{Default: "2026-01", Supported: []string{"2025-06", "2026-01"}}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"api_version": c.Locals("api_version")})
		},
	)

	// ── Route Policy (data sensitivity + api version) ────────────────
	app.Post("/demo/policy",
		policy.New(policy.Config{
			Data:    fh.DataPolicy{Sensitivity: "pii", RedactLogs: true},
			Version: apiversion.Config{Default: "v1", Supported: []string{"v1"}},
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"message": "policy applied", "api_version": c.Locals("api_version")})
		},
	)

	// ── Body Limit (route-specific ceiling) ──────────────────────────
	app.Post("/demo/body-limit",
		bodylimit.New(100), // 100 bytes max
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"size": len(c.Body())})
		},
	)

	// ── Request Timeout (route-specific deadline) ────────────────────
	app.Get("/demo/timeout",
		timeout.New(100*time.Millisecond),
		func(c *fh.Ctx) error {
			select {
			case <-time.After(200 * time.Millisecond):
				return c.SendString("too slow")
			case <-c.Context().Done():
				return c.Context().Err()
			}
		},
	)

	// ── Rate Limiter (route-specific) ────────────────────────────────
	app.Get("/demo/rate-limit",
		ratelimiter.New(ratelimiter.Config{Max: 3, Window: time.Minute, SendHeaders: true}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"message": "request allowed"})
		},
	)

	// ── Response Caching ─────────────────────────────────────────────
	app.Get("/demo/cache",
		cachemw.New(cachemw.Config{TTL: 30 * time.Second, MaxEntries: 64}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{
				"message":      "this response is cached for 30s",
				"generated_at": time.Now().UTC().String(),
			})
		},
	)

	// ── Gzip Compression ─────────────────────────────────────────────
	app.Get("/demo/compress", func(c *fh.Ctx) error {
		return c.SendString(strings.Repeat("hello world! ", 100))
	})

	// ── CSRF Protection ──────────────────────────────────────────────
	csrfProtection := csrf.New(csrf.Config{TrustedOrigins: []string{"http://localhost:3000"}})
	app.Get("/demo/csrf-token", csrfProtection, func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"csrf_token": c.Locals("csrf_token")})
	})
	app.Post("/demo/csrf-submit", csrfProtection, func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"message": "CSRF token accepted"})
	})

	// ── Request Contract ─────────────────────────────────────────────
	app.Post("/demo/contract",
		contract.New(contract.Config{
			Methods:        []string{"POST"},
			ContentTypes:   []string{"application/json"},
			RequireHeaders: []string{"X-Client-ID"},
			MaxBodyBytes:   4096,
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"message": "contract satisfied"})
		},
	)

	// ── HMAC Signature Verification ──────────────────────────────────
	app.Post("/demo/signature",
		signature.New(signature.Config{
			Secret:          []byte("hmac-demo-secret"),
			SignatureHeader: "X-Signature",
			Tolerance:       5 * time.Minute,
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"message": "signature verified"})
		},
	)

	// ── Nonce / Replay Protection ────────────────────────────────────
	app.Post("/demo/replay",
		replay.New(replay.Config{Header: "X-Nonce", TTL: 5 * time.Minute}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"message": "nonce accepted"})
		},
	)

	// ── IP Whitelist ─────────────────────────────────────────────────
	app.Get("/demo/ip-whitelist",
		ipwhitelist.New("127.0.0.1/32", "::1/128"),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"message": "ip allowed"})
		},
	)

	// ── Actor (per-key serialization) ────────────────────────────────
	app.Post("/demo/actor",
		actor.New(actor.Config{
			Key: func(c *fh.Ctx) string { return "user:" + c.Get("X-User-ID") },
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"message": "serialized by user", "user_id": c.Get("X-User-ID")})
		},
	)

	// ── Rewrite (per-route) ──────────────────────────────────────────
	app.Get("/api/hello", func(c *fh.Ctx) error {
		return c.SendString("this route was reached via /old-api/hello -> rewrite -> /api/hello")
	})

	// ── Idempotency + Reliability ────────────────────────────────────
	app.Post("/demo/idempotency",
		idempotency.New(func(c *fh.Ctx) string {
			return c.Get(fh.HeaderIdempotencyKey)
		}),
		reliability.New(fh.ReliabilityPolicy{
			Enabled: true, RequireIdempotency: true, Journal: true,
			ReplayResponse: true, MaxReplayAge: 24 * time.Hour,
		}),
		func(c *fh.Ctx) error {
			return c.Status(fh.StatusCreated).JSON(fh.Map{
				"order_id":   "ord_" + time.Now().Format("20060102150405"),
				"request_id": c.Locals("request_id"),
			})
		},
	)

	// ── Lifecycle Hooks ──────────────────────────────────────────────
	app.Post("/demo/lifecycle",
		lifecycle.New(lifecycle.Hooks{
			OnRequestStart: func(c *fh.Ctx) error {
				log.Printf("lifecycle start request=%v", c.Locals("request_id"))
				return nil
			},
			OnError: func(c *fh.Ctx, err error) error {
				log.Printf("lifecycle error request=%v: %v", c.Locals("request_id"), err)
				return nil
			},
			OnRequestEnd: func(c *fh.Ctx, err error) error {
				log.Printf("lifecycle end request=%v status=%d", c.Locals("request_id"), c.StatusCode())
				return nil
			},
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"message": "lifecycle hooks executed"})
		},
	)

	// ── Workflow ─────────────────────────────────────────────────────
	app.Queue().Register("demo.process", func(ctx context.Context, job *fh.QueueJob) error {
		log.Printf("processing workflow job=%s payload=%s", job.ID, job.Payload)
		return nil
	})
	app.Queue().Register("fulfillment.ship", func(ctx context.Context, job *fh.QueueJob) error {
		log.Printf("shipping fulfillment job=%s payload=%s", job.ID, job.Payload)
		return nil
	})
	app.Queue().Register("billing.invoice", func(ctx context.Context, job *fh.QueueJob) error {
		log.Printf("generating invoice job=%s payload=%s", job.ID, job.Payload)
		return nil
	})
	app.Queue().Register("notification.send", func(ctx context.Context, job *fh.QueueJob) error {
		log.Printf("sending notification job=%s payload=%s", job.ID, job.Payload)
		return nil
	})

	// ── 1. Basic linear workflow ─────────────────────────────────────
	basic := workflow.New("basic").
		Use("validate", func(c *fh.Ctx) error {
			c.Locals("validated", true)
			return nil
		}).
		Use("process", func(c *fh.Ctx) error {
			c.Locals("processed", true)
			return nil
		}).
		Job("queue", "demo.process").
		Use("respond", func(c *fh.Ctx) error {
			return c.JSON(fh.Map{
				"type":   "basic",
				"valid":  c.Locals("validated"),
				"done":   c.Locals("processed"),
				"job_id": c.Locals("job_id"),
			})
		})
	app.Post("/demo/workflow/basic", basic.Handler())

	// ── 2. Conditional steps ─────────────────────────────────────────
	cond := workflow.New("conditional").
		Use("validate", func(c *fh.Ctx) error {
			c.Locals("validated", true)
			return nil
		}).
		Use("apply_discount", func(c *fh.Ctx) error {
			c.Locals("discount", "10%")
			return nil
		}, func(c *fh.Ctx) bool {
			return c.Get("X-Plan") == "vip"
		}).
		Use("apply_standard", func(c *fh.Ctx) error {
			c.Locals("discount", "0%")
			return nil
		}, func(c *fh.Ctx) bool {
			return c.Get("X-Plan") != "vip"
		}).
		Use("respond", func(c *fh.Ctx) error {
			return c.JSON(fh.Map{
				"type":     "conditional",
				"plan":     c.Get("X-Plan"),
				"discount": c.Locals("discount"),
			})
		})
	app.Post("/demo/workflow/conditional", cond.Handler())

	// ── 3. Branching (if/else via sub-workflows) ─────────────────────
	branch := workflow.New("branching").
		Use("validate", func(c *fh.Ctx) error {
			c.Locals("validated", true)
			return nil
		}).
		Branch("routing",
			workflow.New("vip_branch").Condition(func(c *fh.Ctx) bool {
				return c.Get("X-Plan") == "vip"
			}).Use("vip_handler", func(c *fh.Ctx) error {
				c.Locals("route", "vip")
				return nil
			}).Job("vip_job", "notification.send"),
			workflow.New("standard_branch").Condition(func(c *fh.Ctx) bool {
				return c.Get("X-Plan") == "standard"
			}).Use("standard_handler", func(c *fh.Ctx) error {
				c.Locals("route", "standard")
				return nil
			}),
			workflow.New("default_branch").Use("default_handler", func(c *fh.Ctx) error {
				c.Locals("route", "default")
				return nil
			}),
		).
		Use("respond", func(c *fh.Ctx) error {
			return c.JSON(fh.Map{
				"type":  "branch",
				"plan":  c.Get("X-Plan"),
				"route": c.Locals("route"),
			})
		})
	app.Post("/demo/workflow/branch", branch.Handler())

	// ── 4. Parallel fan-out ──────────────────────────────────────────
	parallel := workflow.New("parallel").
		Use("validate", func(c *fh.Ctx) error {
			c.Locals("validated", true)
			return nil
		}).
		Parallel("fan_out",
			workflow.New("fulfillment").Use("reserve", func(c *fh.Ctx) error {
				c.Locals("fulfillment_ok", true)
				return nil
			}).Job("ship", "fulfillment.ship"),
			workflow.New("billing").Use("charge", func(c *fh.Ctx) error {
				c.Locals("billing_ok", true)
				return nil
			}).Job("invoice", "billing.invoice"),
			workflow.New("notifications").Use("notify", func(c *fh.Ctx) error {
				c.Locals("notify_ok", true)
				return nil
			}).Job("send", "notification.send"),
		).
		Use("respond", func(c *fh.Ctx) error {
			return c.JSON(fh.Map{
				"type":         "parallel",
				"fulfillment":  c.Locals("fulfillment_ok"),
				"billing":      c.Locals("billing_ok"),
				"notification": c.Locals("notify_ok"),
			})
		})
	app.Post("/demo/workflow/parallel", parallel.Handler())

	// ── Circuit Breaker ──────────────────────────────────────────────
	breaker := circuitbreaker.New(circuitbreaker.Config{
		FailureThreshold: 2,
		ResetAfter:       10 * time.Second,
		IsFailure: func(c *fh.Ctx, err error) bool {
			return err != nil || c.StatusCode() >= 500
		},
		OnOpen: func(c *fh.Ctx) error {
			return fh.NewHTTPError(fh.StatusServiceUnavailable, "CIRCUIT_OPEN", "service temporarily unavailable")
		},
	})
	app.Get("/demo/circuit-breaker", breaker.Handler(), func(c *fh.Ctx) error {
		if c.Query("fail") == "true" {
			return fh.NewHTTPError(fh.StatusInternalServerError, "UPSTREAM_ERROR", "simulated failure")
		}
		return c.JSON(fh.Map{"message": "circuit closed, request passed"})
	})

	// ── Reverse Proxy ────────────────────────────────────────────────
	app.All("/demo/proxy/*", proxy.New(proxy.Config{
		Target: *upstream, StripPrefix: "/demo/proxy", Timeout: 5 * time.Second,
		ErrorHandler: func(c *fh.Ctx, err error) error {
			return fh.NewHTTPError(fh.StatusBadGateway, "PROXY_ERROR", "upstream unreachable")
		},
	}))

	// ── Metrics ──────────────────────────────────────────────────────
	app.Get("/metrics", ipwhitelist.New("127.0.0.1/32", "::1/128"), requests.Handler())

	// ── Rewrite Demo ─────────────────────────────────────────────────
	app.Get("/rewritten", func(c *fh.Ctx) error {
		return c.SendString("rewrite works: /old-api/rewritten -> /api/rewritten -> /rewritten")
	})

	// ── Durable Queue ────────────────────────────────────────────────
	app.Post("/demo/queue", func(c *fh.Ctx) error {
		q := app.Queue()
		if q == nil {
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{"error": "queue disabled"})
		}
		id, err := q.Enqueue("demo.process", fh.Map{"source": "queue_demo"})
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"job_id": id, "status": "queued"})
	})

	app.Get("/demo/queue/stats", func(c *fh.Ctx) error {
		q := app.Queue()
		if q == nil {
			return c.JSON(fh.Map{"enabled": false})
		}
		st, err := q.Stats()
		if err != nil {
			return err
		}
		return c.JSON(st)
	})

	// ── Request journal ──────────────────────────────────────────────
	app.Get("/demo/journal", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{
			"message":    "every request to this server is journaled via the global reliability middleware",
			"request_id": c.Locals("request_id"),
		})
	})

	// ── CORS preflight test ──────────────────────────────────────────
	app.Get("/demo/cors", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"message": "CORS headers should be present"})
	})

	// ── Named Routes ─────────────────────────────────────────────────
	app.Get("/named-route-example", func(c *fh.Ctx) error {
		url, _ := app.URL("demo.hello", map[string]string{"name": "world"})
		return c.JSON(fh.Map{"named_url": url})
	})
	app.Get("/hello/:name", func(c *fh.Ctx) error {
		return c.SendString("hello " + c.Param("name"))
	}).Name("demo.hello")

	// ── Redirect ─────────────────────────────────────────────────────
	app.Get("/old-home", func(c *fh.Ctx) error {
		return c.Redirect("/health")
	})
	app.Get("/go-hello", func(c *fh.Ctx) error {
		return c.RedirectTo("demo.hello", map[string]string{"name": "redirected"})
	})

	// ── URL Query and Path Parameters ───────────────────────────────
	app.Get("/demo/params/:id", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{
			"id":    c.Param("id"),
			"query": c.Query("filter"),
		})
	})

	// ── Content Negotiation (Codecs) ─────────────────────────────────
	// Supports JSON, XML, form, multipart, CSV, NDJSON, text, binary.
	app.Post("/demo/codecs", func(c *fh.Ctx) error {
		if strings.Contains(c.Get("Content-Type"), "xml") {
			var p struct {
				Name string `xml:"name"`
			}
			if err := c.BodyParser(&p); err != nil {
				return c.Status(400).SendString("BodyParser error: " + err.Error())
			}
			return c.JSON(fh.Map{"echo": fh.Map{"name": p.Name}, "content_type": c.Get("Content-Type")})
		}
		var p map[string]any
		if err := c.BodyParser(&p); err != nil {
			return c.Status(400).SendString("BodyParser error: " + err.Error())
		}
		return c.JSON(fh.Map{"echo": p, "content_type": c.Get("Content-Type")})
	})

	// ── Error Handling ──────────────────────────────────────────────
	app.Get("/demo/error", func(c *fh.Ctx) error {
		return fh.NewHTTPError(fh.StatusUnprocessableEntity, "DEMO_ERROR", "this is a demonstration error")
	})

	// ── Panic Recovery ──────────────────────────────────────────────
	app.Get("/demo/panic", func(c *fh.Ctx) error {
		panic("simulated panic for recover middleware demo")
	})

	// ── Outbox / Inbox ───────────────────────────────────────────────
	app.Post("/demo/outbox", func(c *fh.Ctx) error {
		out := app.Outbox()
		if out == nil {
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{"error": "outbox disabled"})
		}
		id, err := out.Publish(context.Background(), fh.OutboxEvent{
			Topic:   "order.placed",
			Key:     "order-42",
			Payload: []byte(`{"order_id":"order-42"}`),
		})
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"event_id": id})
	})

	app.Post("/demo/inbox", func(c *fh.Ctx) error {
		in := app.Inbox()
		if in == nil {
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{"error": "inbox disabled"})
		}
		id, err := in.Accept(context.Background(), fh.InboxEvent{
			Source:  "stripe",
			EventID: c.Get("X-Webhook-ID"),
			Payload: c.Body(),
		}, "")
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"event_id": id})
	})

	log.Printf("all-middlewares example listening on %s", *addr)
	log.Printf("see README.md for curl commands and expected output")
	log.Fatal(app.Listen(*addr))
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func sourceDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file)
}
