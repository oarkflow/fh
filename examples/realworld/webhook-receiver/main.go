package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

type WebhookEvent struct {
	ID   string         `json:"id"`
	Type string         `json:"type"`
	Data map[string]any `json:"data"`
}

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	secret := flag.String("secret", "dev-secret", "HMAC webhook secret")
	flag.Parse()
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{Enabled: true, DataDir: ".fh-data", JournalEnabled: true, IdempotencyEnabled: true, QueueEnabled: true, QueueWorkers: 2}))
	app.Queue().Register("webhook.process", func(ctx context.Context, job *fh.QueueJob) error {
		log.Printf("process webhook job=%s payload=%s", job.ID, string(job.Payload))
		return nil
	})

	app.Post("/webhooks/payment", func(c *fh.Ctx) error {
		body := c.BodyCopy()
		if !verifySignature(body, c.Get("X-Signature"), *secret) {
			return c.Status(fh.StatusUnauthorized).JSON(fh.Map{"error": "bad_signature"})
		}
		var evt WebhookEvent
		if err := json.Unmarshal(body, &evt); err != nil {
			return fh.BadRequest("invalid JSON body")
		}
		if evt.ID == "" || evt.Type == "" {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid_event"})
		}
		jobID, err := app.Queue().Enqueue("webhook.process", evt, map[string]string{"event_id": evt.ID, "request_id": c.Locals("request_id").(string)})
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"status": "accepted", "job_id": jobID, "request_id": c.Locals("request_id")})
	})
	app.Get("/queue/stats", func(c *fh.Ctx) error {
		st, err := app.Queue().Stats()
		if err != nil {
			return err
		}
		return c.JSON(st)
	})
	_ = time.Now()
	log.Fatal(app.Listen(*addr))
}

func verifySignature(body []byte, header, secret string) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	got := strings.TrimPrefix(header, "sha256=")
	return hmac.Equal([]byte(got), []byte(expected))
}
