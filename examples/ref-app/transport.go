package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/debug"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
)

// The transports.
//
// One intent, reachable over HTTP, the CLI and the queue, with no branch anywhere in
// the business logic about which one it was. That is REF's transport-neutrality claim
// and this file is where it is either true or not.
//
// Each transport's job is the same three things: build an Invocation, dispatch it,
// and project the outcome into whatever the caller understands. This application
// builds its own invocations rather than using ref/transport/http directly, because
// it needs two things the generic adapter does not provide:
//
//   - a non-empty invocation id, always. The id keys the effect transaction, so an
//     execution without one could not commit atomically.
//   - path parameters. A REST-shaped URL (/orders/:id) carries input outside the
//     body, and the intent should not have to know about URLs to see it.

// inputFunc turns a request into the JSON body an intent decodes. Returning nil
// means "no input", which is correct for a GET with nothing to say.
type inputFunc func(c fh.Ctx) []byte

// dispatchHTTP builds the handler for one intent.
func dispatchHTTP(engine *ref.Engine, deps *Deps, name intent.Name, status int, input inputFunc) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		body := c.Body()
		if input != nil {
			body = input(c)
		}
		inv := &invocation.Invocation{
			// A client-supplied request id is used for correlation when present, but
			// it is never trusted to be unique: the effect store keys transactions on
			// this, so a duplicate would collide with somebody else's execution.
			ID:     invocation.ID(newID("inv-")),
			Intent: invocation.IntentID(name),
			Input:  invocation.NewInput(body, orDefault(c.Get("Content-Type"), "application/json")),
			Principal: invocation.PrincipalHint{
				BearerToken: bearerToken(c.Get("Authorization")),
				APIKey:      c.Get("X-API-Key"),
			},
			Metadata: invocation.NewHTTPMeta(
				c.Method(), c.Path(), c.Path(), c.Hostname(), c.GetReqHeaders(), nil, nil,
			),
			Transport: invocation.Transport{
				Protocol: "http",
				RemoteIP: c.IP(),
				TLS:      c.Protocol() == "https",
			},
			Received: time.Now(),
		}
		if correlation := c.Get("X-Request-ID"); correlation != "" {
			c.Set("X-Request-ID", correlation)
		}
		c.Set("X-Execution-ID", string(inv.ID))

		result, err := engine.Dispatch(c.Context(), inv)
		if err != nil {
			// A failed dispatch may have left an effect transaction open — a
			// LocalTransactional effect that failed, for instance. Releasing it here
			// returns the rows immediately instead of waiting for the sweeper.
			deps.Effects.AbortExecution(string(inv.ID))
			return projectError(c, err)
		}
		if result.Meta.CacheControl != "" {
			c.Set("Cache-Control", result.Meta.CacheControl)
		}
		return c.Status(status).JSON(result.Value)
	}
}

// projectError maps a failure onto HTTP. The category carries the status, so an
// intent never names one — which is what lets the same failure travel over the CLI
// and the queue without an HTTP concept leaking into it.
func projectError(c fh.Ctx, err error) error {
	failure, ok := err.(intent.Failure)
	if !ok {
		// An error that is not an intent.Failure is a bug, not a client problem. The
		// caller gets nothing about it; the log gets everything.
		return c.Status(500).JSON(map[string]any{
			"error": map[string]any{"code": "INTERNAL_ERROR", "message": "internal error"},
		})
	}
	status := 500
	switch failure.Category {
	case intent.CategoryInvalidInput:
		status = 422
	case intent.CategoryNotFound:
		status = 404
	case intent.CategoryConflict:
		status = 409
	case intent.CategoryPermission:
		status = 403
	case intent.CategoryAuth:
		status = 401
	case intent.CategoryRateLimit:
		status = 429
	case intent.CategoryUnavailable:
		status = 503
	case intent.CategoryTimeout:
		status = 504
	}
	if status == 401 {
		c.Set("WWW-Authenticate", `Bearer realm="ref-app"`)
	}
	return c.Status(status).JSON(map[string]any{
		"error": map[string]any{"code": failure.Code, "message": failure.Message},
	})
}

// MountRoutes wires the HTTP surface.
func MountRoutes(app *fh.App, engine *ref.Engine, deps *Deps) {
	app.Post("/auth/register", dispatchHTTP(engine, deps, "auth.register", 201, nil))
	app.Post("/auth/login", dispatchHTTP(engine, deps, "auth.login", 200, nil))

	// The catalogue is cacheable, but only per tenant: without Vary a shared cache
	// would serve one tenant's products to another.
	app.Get("/catalog", func(c fh.Ctx) error {
		c.Set("Vary", "X-Tenant-ID")
		return dispatchHTTP(engine, deps, "catalog.list", 200, nil)(c)
	})

	app.Post("/orders", dispatchHTTP(engine, deps, "order.create", 201, nil))
	app.Get("/orders", dispatchHTTP(engine, deps, "order.list", 200, func(c fh.Ctx) []byte {
		payload := map[string]any{}
		// A query parameter is a string; the intent's field is an int. Converting
		// here keeps URL shapes out of the intent, and an unparseable value is
		// simply absent rather than a decode failure the caller cannot act on.
		if limit, err := strconv.Atoi(c.Query("limit")); err == nil && limit > 0 {
			payload["limit"] = limit
		}
		return jsonBody(payload)
	}))
	app.Get("/orders/:id", dispatchHTTP(engine, deps, "order.get", 200, func(c fh.Ctx) []byte {
		return jsonBody(map[string]any{"order_id": c.Params("id")})
	}))
	app.Post("/orders/:id/cancel", dispatchHTTP(engine, deps, "order.cancel", 200, func(c fh.Ctx) []byte {
		// The id comes from the path and the reason from the body, so the two are
		// merged here rather than in the intent.
		payload := map[string]any{"order_id": c.Params("id")}
		var body struct {
			Reason string `json:"reason"`
		}
		if len(c.Body()) > 0 && json.Unmarshal(c.Body(), &body) == nil {
			payload["reason"] = body.Reason
		}
		return jsonBody(payload)
	}))

	mountOperations(app, engine, deps)
}

// mountOperations serves what an operator needs: health, metrics, and the compiled
// plans. The plan endpoints are the honest answer to "what does this application
// actually do" — they are generated from the engine, so they cannot drift from it.
func mountOperations(app *fh.App, engine *ref.Engine, deps *Deps) {
	app.Get("/healthz", func(c fh.Ctx) error {
		ctx, cancel := context.WithTimeout(c.Context(), 2*time.Second)
		defer cancel()

		health := map[string]any{
			"status":      "ok",
			"environment": deps.Config.Environment,
			"notifier":    deps.Notifier.Describe(),
			"observations_dropped": map[string]any{
				"dispatcher": deps.DroppedObservations(),
				"audit":      deps.Audit.Dropped(),
			},
		}
		if err := deps.DB().PingContext(ctx); err != nil {
			health["status"] = "degraded"
			health["database"] = "unreachable"
			return c.Status(503).JSON(health)
		}
		health["database"] = "ok"
		if stats, err := deps.Outbox.Stats(ctx); err == nil {
			health["outbox"] = stats
			// A dead letter is not a reason to fail a health check — the service is
			// serving — but it is a reason to say so out loud.
			if stats["dead"] > 0 {
				health["status"] = "degraded"
				health["note"] = "there are dead-lettered notifications"
			}
		}
		return c.JSON(health)
	})

	app.Get("/metrics", func(c fh.Ctx) error {
		c.Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		return c.SendString(deps.Metrics.Prometheus())
	})
	app.Get("/metrics.json", func(c fh.Ctx) error { return c.JSON(deps.Metrics.Snapshot()) })

	app.Get("/ref/intents", func(c fh.Ctx) error {
		names := engine.Intents().All()
		out := make([]map[string]any, 0, len(names))
		for name, def := range names {
			out = append(out, map[string]any{
				"name":        string(name),
				"description": def.Spec.Description,
				"timeout_ms":  def.Spec.Timeout.Milliseconds(),
				"max_db":      def.Spec.MaxDBQueries,
				"max_effects": def.Spec.MaxEffects,
			})
		}
		return c.JSON(map[string]any{"intents": out})
	})

	app.Get("/ref/inspect/:intent", func(c fh.Ctx) error {
		plan, ok := engine.Plan(intent.Name(c.Params("intent")))
		if !ok {
			return c.Status(404).JSON(map[string]any{
				"error": map[string]any{"code": "NOT_FOUND", "message": "no such intent"},
			})
		}
		return c.JSON(debug.InspectPlan(plan))
	})

	app.Get("/ref/diagram/:intent", func(c fh.Ctx) error {
		plan, ok := engine.Plan(intent.Name(c.Params("intent")))
		if !ok {
			return c.Status(404).SendString("no such intent")
		}
		c.Set("Content-Type", "text/plain; charset=utf-8")
		return c.SendString(debug.ToMermaid(plan))
	})
}

// MountQueue registers the queue consumers.
//
// order.create over a queue is the transport-neutrality claim made concrete: the
// same intent, the same policy, the same effect plan, reached from a job instead of
// a request. The job's headers carry the bearer token, because a queued order still
// belongs to somebody.
func MountQueue(deps *Deps, engine *ref.Engine) {
	deps.Queue.Register(jobDeliverOutbox, func(ctx context.Context, job *fh.QueueJob) error {
		_, err := deps.Outbox.Sweep(ctx)
		return err
	})

	deps.Queue.Register("order.create", func(ctx context.Context, job *fh.QueueJob) error {
		inv := &invocation.Invocation{
			ID:        invocation.ID(newID("inv-")),
			Intent:    invocation.IntentID("order.create"),
			Input:     invocation.NewInput(job.Payload, "application/json"),
			Principal: invocation.PrincipalHint{BearerToken: job.Headers["authorization"]},
			Metadata:  invocation.NewQueueMeta("order.create", job.Attempts, job.ID, job.Headers),
			Transport: invocation.Transport{Protocol: "queue"},
			Received:  time.Now(),
		}
		if _, err := engine.Dispatch(ctx, inv); err != nil {
			deps.Effects.AbortExecution(string(inv.ID))
			// Returning the error is what makes the queue retry it. A failure that
			// will never succeed — an invalid payload — is not worth retrying, so it
			// is swallowed after being logged.
			if failure, ok := err.(intent.Failure); ok && failure.Category == intent.CategoryInvalidInput {
				deps.Log.Warn("dropping an unprocessable queued order",
					slog.String("job", job.ID), slog.String("reason", failure.Message))
				return nil
			}
			return err
		}
		return nil
	})
}

// DispatchCLI runs one intent from the command line, with stdin as its input.
func DispatchCLI(ctx context.Context, engine *ref.Engine, deps *Deps, name string, token string, payload []byte) ([]byte, error) {
	inv := &invocation.Invocation{
		ID:        invocation.ID(newID("inv-")),
		Intent:    invocation.IntentID(name),
		Input:     invocation.NewInput(payload, "application/json"),
		Principal: invocation.PrincipalHint{BearerToken: token},
		Metadata:  invocation.NewCLIMeta([]string{name}, nil, ""),
		Transport: invocation.Transport{Protocol: "cli"},
		Received:  time.Now(),
	}
	result, err := engine.Dispatch(ctx, inv)
	if err != nil {
		deps.Effects.AbortExecution(string(inv.ID))
		return nil, err
	}
	return json.MarshalIndent(result.Value, "", "  ")
}

func jsonBody(payload map[string]any) []byte {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return encoded
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	if len(header) > len(prefix) && header[:len(prefix)] == prefix {
		return header[len(prefix):]
	}
	return ""
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
