package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/bodylimit"
	"github.com/oarkflow/fh/mw/security"
)

type FileJob struct {
	DocumentID string `json:"document_id"`
	Path       string `json:"path"`
	Original   string `json:"original"`
}

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()
	_ = os.MkdirAll("storage", 0700)
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{Enabled: true, DataDir: ".fh-data", JournalEnabled: true, IdempotencyEnabled: true, QueueEnabled: true, QueueWorkers: 2}))
	app.Use(security.New())
	app.Use(bodylimit.New(25 << 20))
	app.Static("/downloads", "storage")
	app.Queue().Register("document.scan", func(ctx context.Context, job *fh.QueueJob) error {
		var j FileJob
		_ = json.Unmarshal(job.Payload, &j)
		log.Printf("scan document=%s path=%s", j.DocumentID, j.Path)
		return nil
	})
	app.Post("/documents", func(c *fh.Ctx) error {
		file, err := c.FormFile("file")
		if err != nil {
			return fh.BadRequest("missing file")
		}
		name := strings.ToLower(file.FileName)
		if !(strings.HasSuffix(name, ".txt") || strings.HasSuffix(name, ".csv") || strings.HasSuffix(name, ".pdf")) {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "unsupported_file_type"})
		}
		id := "doc_" + time.Now().Format("20060102150405")
		dst := filepath.Join("storage", id+"_"+filepath.Base(file.FileName))
		if err := c.SaveFile(file, dst); err != nil {
			return err
		}
		jobID, err := app.Queue().Enqueue("document.scan", FileJob{id, dst, file.FileName}, map[string]string{"request_id": c.Locals("request_id").(string)})
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"document_id": id, "job_id": jobID, "download": "/downloads/" + filepath.Base(dst)})
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
