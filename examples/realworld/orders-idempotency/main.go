package main

import (
	"flag"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type OrderRequest struct {
	Items    []OrderItem `json:"items"`
	Customer string      `json:"customer"`
}
type OrderItem struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

var seq uint64

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{Enabled: true, DataDir: ".fh-data", JournalEnabled: true, IdempotencyEnabled: true, QueueEnabled: false, RequireIdempotencyKey: true}))

	app.Get("/", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"service": "orders-idempotency", "post": "/orders requires Idempotency-Key"})
	})
	app.Post("/orders", func(c *fh.Ctx) error {
		var req OrderRequest
		if err := c.BodyParser(&req); err != nil {
			return fh.BadRequest("invalid JSON body")
		}
		if req.Customer == "" || len(req.Items) == 0 {
			return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid_order"})
		}
		for _, it := range req.Items {
			if it.SKU == "" || it.Qty <= 0 {
				return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid_item"})
			}
		}
		id := "ord_" + time.Now().Format("20060102150405") + "_" + strconv.FormatUint(atomic.AddUint64(&seq, 1), 10)
		return c.Status(fh.StatusCreated).JSON(fh.Map{"request_id": c.Locals("request_id"), "order_id": id, "status": "created", "items": req.Items})
	})
	if err := app.Listen(*addr); err != nil {
		panic(err)
	}
}
