package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/adaptiveconcurrency"
	"github.com/oarkflow/fh/mw/admin"
	"github.com/oarkflow/fh/mw/backpressure"
	"github.com/oarkflow/fh/mw/bulkhead"
	"github.com/oarkflow/fh/mw/etag"
	"github.com/oarkflow/fh/mw/hostguard"
	"github.com/oarkflow/fh/mw/jwt"
	"github.com/oarkflow/fh/mw/loadshed"
	"github.com/oarkflow/fh/mw/maintenance"
	"github.com/oarkflow/fh/mw/metrics"
	"github.com/oarkflow/fh/mw/pprof"
	"github.com/oarkflow/fh/mw/realip"
	"github.com/oarkflow/fh/mw/requesthash"
	"github.com/oarkflow/fh/mw/retrybudget"
	"github.com/oarkflow/fh/mw/slowlog"
	"github.com/oarkflow/fh/mw/tenantlimit"
	"github.com/oarkflow/fh/mw/tracing"
	"github.com/oarkflow/fh/mw/webhook"
	"github.com/oarkflow/fh/pkg/storage/memory"
)

func main() {
	store := memory.New()
	app := fh.NewProduction(
		fh.WithReadTimeout(5*time.Second),
		fh.WithWriteTimeout(10*time.Second),
		fh.WithReliability(fh.ReliabilityConfig{
			Enabled:               true,
			JournalStore:          store.Journal,
			IdempotencyRepository: store.Idempotency,
			QueueStorage:          store.Queue,
			QueueWorkers:          2,
		}),
	)

	maint := maintenance.NewSwitch()
	m := metrics.New()
	_, loopbackProxy, _ := net.ParseCIDR("127.0.0.0/8")

	app.Use(
		realip.New(realip.Config{TrustedProxies: []*net.IPNet{loopbackProxy}}),
		hostguard.New(hostguard.Config{Allowed: []string{"localhost", "127.0.0.1"}, AllowEmpty: true}),
		tracing.New(tracing.Config{TrustIncoming: true}),
		requesthash.New(requesthash.Config{SkipEmpty: true}),
		m.Middleware(),
		loadshed.New(loadshed.Config{MaxInFlight: 10000, MaxGoroutines: 50000}),
		adaptiveconcurrency.New(adaptiveconcurrency.Config{InitialLimit: 2048, MinLimit: 64, MaxLimit: 10000, TargetLatency: 150 * time.Millisecond}),
		bulkhead.New(bulkhead.Config{MaxConcurrent: 2048, Timeout: 25 * time.Millisecond}),
		retrybudget.New(retrybudget.Config{MaxRetries: 10, Window: time.Minute}),
		tenantlimit.New(tenantlimit.Config{Limit: 500}),
		backpressure.New(backpressure.Config{Queue: app.Queue(), MaxPending: 50000}),
		maintenance.New(maintenance.Config{Switch: maint, BypassHeader: "X-Maintenance-Bypass", BypassToken: "dev-token", Path: "/maintenance", Renderer: func(c fh.Ctx) error {
			return c.Status(fh.StatusServiceUnavailable).Type("html").SendString(`<!doctype html><h1>Maintenance</h1><p>Service is temporarily unavailable.</p>`)
		}}),
		etag.New(etag.Config{MinSize: 1}),
		slowlog.New(slowlog.Config{Threshold: 250 * time.Millisecond}),
	)

	admin.Enable(app, admin.Config{Prefix: "/_fh/admin", Auth: admin.StaticToken("X-Admin-Token", "dev-admin")})
	pprof.Enable(app, pprof.Config{Prefix: "/_fh/debug/pprof", Auth: pprof.StaticToken("X-Admin-Token", "dev-admin")})
	app.Get("/_fh/metrics", m.Handler())
	app.AddHealthCheck("queue", time.Second, func(ctx context.Context) error {
		if app.Queue() == nil {
			return nil
		}
		_, err := app.Queue().Stats()
		return err
	})
	app.EnableHealth("/_fh")
	app.EnableRuntime("/_fh")

	if q := app.Queue(); q != nil {
		q.Register("email", func(ctx context.Context, job *fh.QueueJob) error { return nil })
	}

	app.Get("/", func(c fh.Ctx) error { return c.JSON(fh.Map{"ok": true}) })
	app.Post("/jobs/email", func(c fh.Ctx) error {
		id, err := app.Queue().Enqueue("email", c.BodyCopy())
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"job_id": id})
	})
	app.Get("/admin-only", jwt.New(jwt.Config{Secret: []byte("dev-secret")}), fh.RequireRole("admin"), func(c fh.Ctx) error { return c.JSON(fh.Map{"ok": true, "scope": "admin"}) })
	app.Post("/webhook", webhook.New(webhook.Config{Secret: []byte("webhook-secret"), Prefix: "sha256"}), func(c fh.Ctx) error { return c.JSON(fh.Map{"accepted": true}) })

	log.Fatal(app.Listen(":8080"))
}
