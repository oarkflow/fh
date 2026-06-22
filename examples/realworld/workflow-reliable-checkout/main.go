package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/actor"
	"github.com/oarkflow/fh/mw/idempotency"
	"github.com/oarkflow/fh/mw/lifecycle"
	"github.com/oarkflow/fh/mw/reliability"
	"github.com/oarkflow/fh/mw/workflow"
)

type CheckoutRequest struct {
	CartID       string `json:"cart_id"`
	SKU          string `json:"sku"`
	Quantity     int    `json:"quantity"`
	PaymentToken string `json:"payment_token"`
}

type RestockRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

var inventory = struct {
	sync.Mutex
	units map[string]int
}{units: map[string]int{"notebook": 25, "pen": 100, "backpack": 8}}

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()

	app := fh.New(fh.Config{Reliability: fh.ReliabilityConfig{
		Enabled: true, DataDir: ".fh-data", JournalEnabled: true,
		IdempotencyEnabled: true, QueueEnabled: true, QueueWorkers: 2,
	}})
	app.Queue().Register("checkout.fulfill", func(ctx context.Context, job *fh.QueueJob) error {
		log.Printf("fulfilling checkout job=%s payload=%s", job.ID, job.Payload)
		return nil
	})
	app.Queue().Register("inventory.restock", func(ctx context.Context, job *fh.QueueJob) error {
		var req RestockRequest
		if err := json.Unmarshal(job.Payload, &req); err != nil {
			return err
		}
		inventory.Lock()
		inventory.units[req.SKU] += req.Quantity
		inventory.Unlock()
		return nil
	})

	checkout := workflow.New("reliable-checkout").
		Use("validate_cart", validateCheckout).
		Use("reserve_inventory", reserveInventory).
		Use("authorize_payment", authorizePayment).
		Job("handoff_fulfillment", "checkout.fulfill").
		Use("respond", checkoutAccepted)

	app.Post("/checkouts",
		// Mobile clients may send X-Checkout-Token instead of the standard header.
		idempotency.New(func(c *fh.Ctx) string {
			if key := c.Get(fh.HeaderIdempotencyKey); key != "" {
				return key
			}
			return c.Get("X-Checkout-Token")
		}),
		reliability.New(fh.ReliabilityPolicy{
			Enabled: true, RequireIdempotency: true, Journal: true,
			ReplayResponse: true, ConflictOnBodyDrift: true, MaxReplayAge: 24 * time.Hour,
			Data: fh.DataPolicy{Sensitivity: "payment", RedactLogs: true, JournalMode: "hash-only"},
		}),
		// Two retries for one cart serialize, while unrelated carts can proceed together.
		actor.New(actor.Config{Key: func(c *fh.Ctx) string {
			var req CheckoutRequest
			if json.Unmarshal(c.Body(), &req) != nil {
				return ""
			}
			return req.CartID
		}}),
		lifecycle.New(lifecycle.Hooks{
			OnRequestStart: func(c *fh.Ctx) { log.Printf("checkout start request=%v", c.Locals("request_id")) },
			OnError: func(c *fh.Ctx, err error) {
				log.Printf("checkout failed request=%v: %v", c.Locals("request_id"), err)
				if reserved, _ := c.Locals("inventory_reserved").(bool); reserved {
					if compensateErr := c.RunCompensations(); compensateErr != nil {
						log.Printf("checkout compensation failed request=%v: %v", c.Locals("request_id"), compensateErr)
					}
				}
			},
			OnRequestEnd: func(c *fh.Ctx) {
				log.Printf("checkout end request=%v status=%d", c.Locals("request_id"), c.StatusCode())
			},
		}),
		checkout.Handler(),
	)

	// The typed endpoint shows the concise async form of the reliability middleware.
	app.Post("/inventory/restocks", reliability.Endpoint[RestockRequest, fh.Map](reliability.EndpointOptions[RestockRequest, fh.Map]{
		Policy: fh.ReliabilityPolicy{Enabled: true, RequireIdempotency: true, Journal: true, ReplayResponse: true},
		Validate: func(c *fh.Ctx, req *RestockRequest) error {
			if req.SKU == "" || req.Quantity <= 0 {
				return fh.NewHTTPError(fh.StatusUnprocessableEntity, "INVALID_RESTOCK", "sku and positive quantity are required")
			}
			return nil
		},
		Async: true, QueueType: "inventory.restock",
	}))

	app.Get("/inventory", func(c *fh.Ctx) error {
		inventory.Lock()
		copy := make(map[string]int, len(inventory.units))
		for sku, count := range inventory.units {
			copy[sku] = count
		}
		inventory.Unlock()
		return c.JSON(copy)
	})
	app.Get("/queue/stats", func(c *fh.Ctx) error {
		stats, err := app.Queue().Stats()
		if err != nil {
			return err
		}
		return c.JSON(stats)
	})

	log.Printf("reliable checkout example listening on %s", *addr)
	log.Fatal(app.Listen(*addr))
}

func validateCheckout(c *fh.Ctx) error {
	var req CheckoutRequest
	if err := c.BodyParser(&req); err != nil {
		return fh.BadRequest("invalid JSON checkout")
	}
	if req.CartID == "" || req.SKU == "" || req.Quantity <= 0 || req.PaymentToken == "" {
		return fh.NewHTTPError(fh.StatusUnprocessableEntity, "INVALID_CHECKOUT", "cart_id, sku, positive quantity, and payment_token are required")
	}
	c.Locals("checkout", req)
	return nil
}

func reserveInventory(c *fh.Ctx) error {
	req := c.Locals("checkout").(CheckoutRequest)
	inventory.Lock()
	defer inventory.Unlock()
	if inventory.units[req.SKU] < req.Quantity {
		return fh.NewHTTPError(fh.StatusConflict, "OUT_OF_STOCK", "requested inventory is unavailable")
	}
	inventory.units[req.SKU] -= req.Quantity
	c.Locals("remaining", inventory.units[req.SKU])
	c.Locals("inventory_reserved", true)
	c.Compensate(func(context.Context) error {
		inventory.Lock()
		inventory.units[req.SKU] += req.Quantity
		inventory.Unlock()
		c.Locals("inventory_reserved", false)
		return nil
	})
	return nil
}

func authorizePayment(c *fh.Ctx) error {
	req := c.Locals("checkout").(CheckoutRequest)
	if req.PaymentToken == "declined" {
		return fh.NewHTTPError(fh.StatusPaymentRequired, "PAYMENT_DECLINED", "payment authorization was declined")
	}
	c.Locals("authorization_id", fmt.Sprintf("auth_%d", time.Now().UnixNano()))
	return nil
}

func checkoutAccepted(c *fh.Ctx) error {
	req := c.Locals("checkout").(CheckoutRequest)
	return c.Status(fh.StatusAccepted).JSON(fh.Map{
		"status": "accepted", "cart_id": req.CartID, "sku": req.SKU,
		"remaining": c.Locals("remaining"), "authorization_id": c.Locals("authorization_id"),
		"fulfillment_job_id": c.Locals("job_id"), "request_id": c.Locals("request_id"),
	})
}
