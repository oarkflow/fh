package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"time"

	"github.com/oarkflow/fh"
)

type Project struct {
	Name string `json:"name"`
}
type ReportJob struct {
	TenantID  string `json:"tenant_id"`
	ProjectID string `json:"project_id"`
}

func tenantMiddleware(c *fh.Ctx) error {
	tenant := c.Get("X-Tenant-ID")
	if tenant == "" {
		return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "missing_tenant"})
	}
	c.Locals("tenant_id", tenant)
	return c.Next()
}

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{Enabled: true, DataDir: ".fh-data", JournalEnabled: true, IdempotencyEnabled: true, QueueEnabled: true, QueueWorkers: 2}))
	app.Queue().Register("project.report", func(ctx context.Context, job *fh.QueueJob) error {
		var j ReportJob
		_ = json.Unmarshal(job.Payload, &j)
		log.Printf("tenant=%s generate report for project=%s", j.TenantID, j.ProjectID)
		return nil
	})
	api := app.Group("/api", tenantMiddleware)
	api.Post("/projects", func(c *fh.Ctx) error {
		var p Project
		if err := c.BodyParser(&p); err != nil {
			return fh.BadRequest("invalid JSON body")
		}
		if p.Name == "" {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "missing_name"})
		}
		id := "prj_" + time.Now().Format("20060102150405")
		return c.Status(fh.StatusCreated).JSON(fh.Map{"tenant_id": c.Locals("tenant_id"), "project_id": id, "name": p.Name})
	})
	api.Post("/projects/:id/report", func(c *fh.Ctx) error {
		tenant := c.Locals("tenant_id").(string)
		jobID, err := app.Queue().Enqueue("project.report", ReportJob{tenant, c.Param("id")}, map[string]string{"tenant_id": tenant})
		if err != nil {
			return err
		}
		return c.Status(fh.StatusAccepted).JSON(fh.Map{"job_id": jobID, "tenant_id": tenant})
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
