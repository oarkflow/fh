package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/bodylimit"
)

type ImportJob struct {
	ImportID string `json:"import_id"`
	File     string `json:"file"`
}

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()
	_ = os.MkdirAll("uploads", 0700)
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{Enabled: true, DataDir: ".fh-data", JournalEnabled: true, IdempotencyEnabled: true, QueueEnabled: true, QueueWorkers: 1, QueuePollInterval: 200 * time.Millisecond}))
	app.Use(bodylimit.New(20 << 20))
	app.Queue().Register("csv.import.users", func(ctx context.Context, job *fh.QueueJob) error {
		var j ImportJob
		if err := json.Unmarshal(job.Payload, &j); err != nil {
			return err
		}
		f, err := os.Open(j.File)
		if err != nil {
			return err
		}
		defer f.Close()
		r := csv.NewReader(f)
		rows := 0
		for {
			_, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			rows++
		}
		log.Printf("import_id=%s rows=%d file=%s", j.ImportID, rows, j.File)
		return nil
	})
	app.Get("/", func(c *fh.Ctx) error { return c.SendString(uploadForm) })
	app.Post("/imports/users", func(c *fh.Ctx) error {
		file, err := c.FormFile("file")
		if err != nil {
			return fh.BadRequest("missing file field")
		}
		id := "imp_" + time.Now().Format("20060102150405")
		dst := filepath.Join("uploads", id+"_"+filepath.Base(file.FileName))
		if err := c.SaveFile(file, dst); err != nil {
			return err
		}
		jobID, err := app.Queue().Enqueue("csv.import.users", ImportJob{ImportID: id, File: dst}, map[string]string{"request_id": c.Locals("request_id").(string)})
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"import_id": id, "job_id": jobID, "status": "accepted"})
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

const uploadForm = `<form method="post" action="/imports/users" enctype="multipart/form-data"><input type="file" name="file"><button>Import CSV</button></form>`
