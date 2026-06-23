package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type PaymentRequest struct {
	CustomerID string `json:"customer_id"`
	Amount     int    `json:"amount"`
	Currency   string `json:"currency"`
}
type PaymentWebhook struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
}

var seq uint64

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{Enabled: true, DataDir: ".fh-data", JournalEnabled: true, IdempotencyEnabled: true, QueueEnabled: true, RequireIdempotencyKey: true}))
	app.Queue().Register("payment.webhook", func(ctx context.Context, job *fh.QueueJob) error {
		var ev PaymentWebhook
		_ = json.Unmarshal(job.Payload, &ev)
		log.Printf("payment webhook payment=%s status=%s", ev.PaymentID, ev.Status)
		return nil
	})
	app.Post("/payments", func(c *fh.Ctx) error {
		var req PaymentRequest
		if err := c.BodyParser(&req); err != nil {
			return fh.BadRequest("invalid JSON body")
		}
		if req.CustomerID == "" || req.Amount <= 0 || req.Currency == "" {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid_payment"})
		}
		id := "pay_" + time.Now().Format("20060102150405") + "_" + strconv.FormatUint(atomic.AddUint64(&seq, 1), 10)
		return c.Status(fh.StatusCreated).JSON(fh.Map{"payment_id": id, "status": "requires_confirmation", "amount": req.Amount, "currency": req.Currency})
	})
	app.Post("/webhooks/payment", func(c *fh.Ctx) error {
		var ev PaymentWebhook
		if err := c.BodyParser(&ev); err != nil {
			return fh.BadRequest("invalid JSON body")
		}
		jobID, err := app.Queue().Enqueue("payment.webhook", ev)
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"job_id": jobID})
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
