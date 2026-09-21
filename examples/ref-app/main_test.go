package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/oarkflow/fh"
	refCli "github.com/oarkflow/fh/ref/transport/cli"
)

func TestRefAppEndToEnd(t *testing.T) {
	app := fh.NewFast()
	engine := buildEngine(app)

	// Verify CLI dispatch
	cliCmd := refCli.Command(engine, "order.create")
	payload := []byte(`{"sku":"WIDGET-99","quantity":3,"price":10.50}`)
	out, err := cliCmd(context.Background(), nil, payload)
	if err != nil {
		t.Fatalf("CLI dispatch failed: %v", err)
	}

	var res CreateOrderOutput
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("failed to unmarshal CLI response: %v", err)
	}

	if res.SKU != "WIDGET-99" || res.Quantity != 3 || res.Total != 31.50 {
		t.Errorf("unexpected output: %+v", res)
	}

	// Verify HTTP dispatch
	app.Post("/orders", func(c fh.Ctx) error {
		// Test route mounted
		return c.SendStatus(200)
	})

	req := httptest.NewRequest("POST", "/orders", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "test-tenant")
	req.Header.Set("Authorization", "Bearer token-123")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestRefAppPolicyDeny(t *testing.T) {
	app := fh.NewFast()
	engine := buildEngine(app)

	cliCmd := refCli.Command(engine, "order.create")
	// Test validation error
	payload := []byte(`{"sku":"","quantity":0}`)
	_, err := cliCmd(context.Background(), nil, payload)
	if err == nil {
		t.Fatalf("expected error for invalid input, got nil")
	}
}
