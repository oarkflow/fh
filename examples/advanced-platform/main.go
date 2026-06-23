package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/logger"
	"github.com/oarkflow/fh/mw/metrics"
	"github.com/oarkflow/fh/mw/recover"
	reliabilitymw "github.com/oarkflow/fh/mw/reliability"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/signature"
)

type EmailRequest struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Message string `json:"message"`
}
type PaymentRequest struct {
	ExternalID string `json:"external_id"`
	Amount     int    `json:"amount"`
}

type PaymentResponse struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
}

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()

	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{
		Enabled: true, DataDir: ".fh-data", JournalEnabled: true, IdempotencyEnabled: true, QueueEnabled: true,
		QueueWorkers: 2, QueueConcurrencyLimitByKey: true,
	}))
	app.Use(recover.New())
	app.Use(security.New())
	app.Use(logger.New(logger.Config{FormatName: "json"}))
	m := metrics.New()
	app.Use(m.Middleware())
	app.Get("/_fh/metrics", m.Handler())
	app.EnableSecurityEvents("/_fh/security-events")

	app.Queue().Register("email.send", func(ctx context.Context, job *fh.QueueJob) error {
		var req EmailRequest
		_ = json.Unmarshal(job.Payload, &req)
		log.Printf("sending email job=%s to=%s subject=%s", job.ID, req.To, req.Subject)
		return nil
	})
	app.Queue().Register("outbox.audit", func(ctx context.Context, job *fh.QueueJob) error {
		log.Printf("outbox event: %s", string(job.Payload))
		return nil
	})
	app.Queue().Register("inbox.payment", func(ctx context.Context, job *fh.QueueJob) error {
		log.Printf("payment webhook accepted once: %s", string(job.Payload))
		return nil
	})

	app.Post("/payments", reliabilitymw.Endpoint(reliabilitymw.EndpointOptions[PaymentRequest, PaymentResponse]{
		Policy: fh.ReliabilityPolicy{Enabled: true, RequireIdempotency: true, Journal: true, ReplayResponse: true, IdempotencyFingerprint: func(c *fh.Ctx) string {
			var p PaymentRequest
			_ = c.BodyParser(&p)
			return fh.DeterministicIdempotencyKey("payment", p.ExternalID, fmt.Sprint(p.Amount))
		}, Data: fh.DataPolicy{Sensitivity: "financial", RedactLogs: true}},
		Validate: func(c *fh.Ctx, r *PaymentRequest) error {
			if r.ExternalID == "" || r.Amount <= 0 {
				return fh.NewHTTPError(fh.StatusBadRequest, "INVALID_PAYMENT", "external_id and positive amount are required")
			}
			return nil
		},
		Handle: func(ctx context.Context, c *fh.Ctx, r PaymentRequest) (PaymentResponse, error) {
			c.Compensate(func(ctx context.Context) error { log.Println("compensating", r.ExternalID); return nil })
			_, _ = c.ServerOutbox().Publish(ctx, fh.OutboxEvent{Topic: "audit", Key: r.ExternalID, Payload: mustJSON(r)})
			return PaymentResponse{PaymentID: "pay_" + r.ExternalID, Status: "created"}, nil
		},
	}))

	app.Post("/email", reliabilitymw.New(fh.ReliabilityPolicy{Enabled: true, RequireIdempotency: true, Journal: true, ReplayResponse: true}), func(c *fh.Ctx) error {
		var req EmailRequest
		if err := c.BodyParser(&req); err != nil {
			return err
		}
		id, err := fh.AtomicHandoff(c, "email.send", req, fh.QueueJob{Priority: 10, ConcurrencyKey: req.To})
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"job_id": id, "status": "accepted"})
	})

	secret := []byte("change-me")
	app.Post("/webhooks/payment", signature.New(signature.Config{Secret: secret}), func(c *fh.Ctx) error {
		id, err := c.ServerInbox().Accept(c.Context(), fh.InboxEvent{Source: "payment", EventID: c.Get("X-Event-ID"), Payload: c.BodyCopy()}, "inbox.payment")
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"job_id": id, "status": "accepted"})
	})

	app.Get("/events", func(c *fh.Ctx) error {
		return c.SSE(func(s *fh.SSE) error { return s.Event("queue.stats", mustStats(app.Queue())) })
	})
	app.Get("/queue/stats", func(c *fh.Ctx) error { st, _ := app.Queue().Stats(); return c.JSON(st) })
	app.Get("/sign-demo", func(c *fh.Ctx) error {
		body := []byte(`{"ok":true}`)
		ts := time.Now().UTC().Format(time.RFC3339)
		mac := hmac.New(sha256.New, secret)
		mac.Write([]byte(ts))
		mac.Write([]byte("."))
		mac.Write(body)
		return c.JSON(fh.Map{"timestamp": ts, "signature": "sha256=" + hex.EncodeToString(mac.Sum(nil)), "body": string(body)})
	})

	log.Fatal(app.Listen(*addr))
}

func mustJSON(v any) []byte                      { b, _ := json.Marshal(v); return b }
func mustStats(q *fh.DurableQueue) fh.QueueStats { st, _ := q.Stats(); return st }
