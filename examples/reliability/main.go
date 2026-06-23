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

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Message string `json:"message"`
}

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	reliable := flag.Bool("reliable", true, "enable request journal, idempotency, and durable queue")
	dataDir := flag.String("data", ".fh-data", "durable reliability data directory")
	requireIdempotency := flag.Bool("require-idempotency", false, "require Idempotency-Key on POST/PUT/PATCH/DELETE")
	workers := flag.Int("queue-workers", 2, "durable queue worker count")
	flag.Parse()

	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{
		Enabled:               *reliable,
		DataDir:               *dataDir,
		JournalEnabled:        true,
		IdempotencyEnabled:    true,
		QueueEnabled:          true,
		RequireIdempotencyKey: *requireIdempotency,
		IdempotencyTTL:        24 * time.Hour,
		QueueWorkers:          *workers,
		QueueMaxAttempts:      5,
		QueuePollInterval:     100 * time.Millisecond,
	}))

	if q := app.Queue(); q != nil {
		q.Register("send_email", func(ctx context.Context, job *fh.QueueJob) error {
			var email EmailJob
			if err := json.Unmarshal(job.Payload, &email); err != nil {
				return err
			}
			// Replace this with SMTP/provider integration. The queue guarantees durable
			// at-least-once delivery, so the real sender should use job.ID as a message
			// idempotency key to avoid duplicate external sends after retries.
			log.Printf("send email job=%s to=%s subject=%q", job.ID, email.To, email.Subject)
			return nil
		})
	}

	app.Get("/", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{
			"message": "fh reliability example",
			"sync":    "POST /orders with Idempotency-Key",
			"async":   "POST /email queues durable job",
			"stats":   "GET /queue/stats",
		})
	})

	app.Post("/orders", func(c *fh.Ctx) error {
		// This endpoint is safe to retry when the client sends the same Idempotency-Key.
		return c.Status(fh.StatusCreated).JSON(fh.Map{
			"request_id": c.Locals("request_id"),
			"order_id":   "ord_" + time.Now().Format("20060102150405"),
			"status":     "created",
		})
	})

	app.Post("/email", func(c *fh.Ctx) error {
		var req EmailJob
		if err := c.BodyParser(&req); err != nil {
			return fh.BadRequest("invalid JSON body")
		}
		if !strings.Contains(req.To, "@") || req.Subject == "" {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid email request"})
		}
		q := app.Queue()
		if q == nil {
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{"error": "queue disabled"})
		}
		jobID, err := q.Enqueue("send_email", req, map[string]string{"request_id": c.Locals("request_id").(string)})
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"request_id": c.Locals("request_id"), "job_id": jobID, "status": "accepted"})
	})

	app.Get("/queue/stats", func(c *fh.Ctx) error {
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

	log.Fatal(app.Listen(*addr))
}
