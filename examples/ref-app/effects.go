package main

import (
	"context"
	"log"

	"github.com/oarkflow/fh/ref"
)

// DebitInventoryEffect is a local transactional effect with compensation.
type DebitInventoryEffect struct {
	TenantID string
	SKU      string
	Quantity int
}

func (e *DebitInventoryEffect) Name() string {
	return "effect.inventory.debit"
}

func (e *DebitInventoryEffect) Kind() ref.EffectKind {
	return ref.LocalTransactional
}

func (e *DebitInventoryEffect) Commit(ctx context.Context) error {
	log.Printf("[DB-TX] Debited %d units of SKU %s for tenant %s", e.Quantity, e.SKU, e.TenantID)
	return nil
}

// Compensate restores inventory if subsequent local effects fail.
func (e *DebitInventoryEffect) Compensate(ctx context.Context) error {
	log.Printf("[DB-TX-ROLLBACK] Restoring %d units of SKU %s for tenant %s", e.Quantity, e.SKU, e.TenantID)
	return nil
}

// InsertOrderRecordEffect writes the order entry in the same DB transaction.
type InsertOrderRecordEffect struct {
	OrderID  string
	TenantID string
	Total    float64
}

func (e *InsertOrderRecordEffect) Name() string {
	return "effect.order.insert"
}

func (e *InsertOrderRecordEffect) Kind() ref.EffectKind {
	return ref.LocalTransactional
}

func (e *InsertOrderRecordEffect) Commit(ctx context.Context) error {
	log.Printf("[DB-TX] Inserted Order %s ($%.2f) for tenant %s", e.OrderID, e.Total, e.TenantID)
	return nil
}

// SendConfirmationEmailEffect is a durable delivery effect recorded in the transactional outbox.
type SendConfirmationEmailEffect struct {
	OrderID  string
	UserID   string
	SKU      string
	Quantity int
}

func (e *SendConfirmationEmailEffect) Name() string {
	return "effect.email.confirmation"
}

func (e *SendConfirmationEmailEffect) Kind() ref.EffectKind {
	return ref.DurableDelivery
}

func (e *SendConfirmationEmailEffect) Commit(ctx context.Context) error {
	log.Printf("[OUTBOX-WORKER] Dispatching confirmation email for Order %s to User %s", e.OrderID, e.UserID)
	return nil
}

// EmitTelemetryEffect is a fire-and-forget telemetry effect.
type EmitTelemetryEffect struct {
	Metric string
	Value  float64
}

func (e *EmitTelemetryEffect) Name() string {
	return "effect.telemetry.emit"
}

func (e *EmitTelemetryEffect) Kind() ref.EffectKind {
	return ref.FireAndForget
}

func (e *EmitTelemetryEffect) Commit(ctx context.Context) error {
	log.Printf("[TELEMETRY] Metric %s = %.2f", e.Metric, e.Value)
	return nil
}
