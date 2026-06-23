package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

type EmailRequest struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Message string `json:"message"`
}

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	data := flag.String("data", ".fh-data", "reliability data directory")
	workers := flag.Int("workers", 2, "queue workers")
	flag.Parse()

	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{
		Enabled: true, DataDir: *data, JournalEnabled: true, IdempotencyEnabled: true,
		QueueEnabled: true, QueueWorkers: *workers, QueueMaxAttempts: 5, QueuePollInterval: 100 * time.Millisecond,
	}))

	app.Queue().Register("email.send", func(ctx context.Context, job *fh.QueueJob) error {
		var req EmailRequest
		if err := json.Unmarshal(job.Payload, &req); err != nil {
			return err
		}
		log.Printf("email worker job=%s to=%s subject=%q request_id=%s", job.ID, req.To, req.Subject, job.Headers["request_id"])
		return nil
	})

	app.Get("/", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"service": "reliable-email", "post": "/email", "stats": "/queue/stats"})
	})
	app.Post("/email", func(c *fh.Ctx) error {
		var req EmailRequest
		if err := c.BodyParser(&req); err != nil {
			return fh.BadRequest("invalid JSON body")
		}
		if !strings.Contains(req.To, "@") || req.Subject == "" || req.Message == "" {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid_email_request"})
		}
		jobID, err := app.Queue().Enqueue("email.send", req, map[string]string{"request_id": c.Locals("request_id").(string)})
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"request_id": c.Locals("request_id"), "job_id": jobID, "status": "accepted"})
	})
	app.Get("/queue/stats", func(c *fh.Ctx) error {
		st, err := app.Queue().Stats()
		if err != nil {
			return err
		}
		return c.JSON(st)
	})

	log.Fatal(app.Listen(*addr))
}
