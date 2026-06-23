package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"time"

	"github.com/oarkflow/fh"
)

type NotificationRequest struct {
	UserID   string   `json:"user_id"`
	Message  string   `json:"message"`
	Channels []string `json:"channels"`
}
type ChannelJob struct {
	NotificationID string `json:"notification_id"`
	Channel        string `json:"channel"`
	UserID         string `json:"user_id"`
	Message        string `json:"message"`
}

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{Enabled: true, DataDir: ".fh-data", JournalEnabled: true, IdempotencyEnabled: true, QueueEnabled: true, QueueWorkers: 3}))
	for _, typ := range []string{"notify.email", "notify.sms", "notify.websocket"} {
		jobType := typ
		app.Queue().Register(jobType, func(ctx context.Context, job *fh.QueueJob) error {
			var j ChannelJob
			_ = json.Unmarshal(job.Payload, &j)
			log.Printf("deliver %s notification=%s user=%s", jobType, j.NotificationID, j.UserID)
			return nil
		})
	}
	app.Post("/notifications", func(c *fh.Ctx) error {
		var req NotificationRequest
		if err := c.BodyParser(&req); err != nil {
			return fh.BadRequest("invalid JSON body")
		}
		if req.UserID == "" || req.Message == "" || len(req.Channels) == 0 {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid_notification"})
		}
		id := "ntf_" + time.Now().Format("20060102150405")
		jobs := []string{}
		for _, ch := range req.Channels {
			typ := "notify." + ch
			jobID, err := app.Queue().Enqueue(typ, ChannelJob{id, ch, req.UserID, req.Message}, map[string]string{"request_id": c.Locals("request_id").(string)})
			if err != nil {
				return err
			}
			jobs = append(jobs, jobID)
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"notification_id": id, "jobs": jobs, "status": "accepted"})
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
