package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/oarkflow/fh"
)

type InvoiceJob struct {
	InvoiceID string `json:"invoice_id"`
	Customer  string `json:"customer"`
	Amount    int    `json:"amount"`
}

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()
	_ = os.MkdirAll("public/invoices", 0700)
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{Enabled: true, DataDir: ".fh-data", JournalEnabled: true, IdempotencyEnabled: true, QueueEnabled: true, QueueWorkers: 1}))
	app.Static("/files", "public")
	app.Queue().Register("invoice.generate", func(ctx context.Context, job *fh.QueueJob) error {
		var j InvoiceJob
		if err := json.Unmarshal(job.Payload, &j); err != nil {
			return err
		}
		p := filepath.Join("public", "invoices", j.InvoiceID+".txt")
		return os.WriteFile(p, []byte(fmt.Sprintf("Invoice %s\nCustomer: %s\nAmount: %d\n", j.InvoiceID, j.Customer, j.Amount)), 0600)
	})
	app.Post("/invoices", func(c *fh.Ctx) error {
		var req InvoiceJob
		if err := c.BodyParser(&req); err != nil {
			return fh.BadRequest("invalid JSON body")
		}
		if req.Customer == "" || req.Amount <= 0 {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid_invoice"})
		}
		req.InvoiceID = "inv_" + time.Now().Format("20060102150405")
		jobID, err := app.Queue().Enqueue("invoice.generate", req, map[string]string{"request_id": c.Locals("request_id").(string)})
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"invoice_id": req.InvoiceID, "job_id": jobID, "download": "/files/invoices/" + req.InvoiceID + ".txt"})
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
