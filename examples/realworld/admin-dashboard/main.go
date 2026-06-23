package main

import (
	"flag"
	"log"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/basicauth"
	"github.com/oarkflow/fh/mw/ratelimiter"
	recovermw "github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/security"
)

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{Enabled: true, DataDir: ".fh-data", JournalEnabled: true, IdempotencyEnabled: false, QueueEnabled: true}))
	app.Use(recovermw.New())
	app.Use(security.New())
	app.Use(ratelimiter.New(ratelimiter.Config{Max: 60, Window: time.Minute}))
	admin := app.Group("/admin", basicauth.New("admin", "admin123"))
	admin.Get("", func(c *fh.Ctx) error {
		c.Type("text/html; charset=utf-8")
		return c.SendString(`<h1>fh Admin</h1><ul><li><a href="/admin/queue">Queue stats</a></li><li><a href="/admin/audit">Audit journal location</a></li></ul>`)
	})
	admin.Get("/queue", func(c *fh.Ctx) error {
		st, err := app.Queue().Stats()
		if err != nil {
			return err
		}
		return c.JSON(st)
	})
	admin.Get("/audit", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"journal": ".fh-data/request-journal.jsonl", "queue_events": ".fh-data/queue/events.jsonl"})
	})
	log.Fatal(app.Listen(*addr))
}
