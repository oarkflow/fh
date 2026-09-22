package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/oarkflow/fh/ref"
)

// The effects.
//
// An intent never writes. It returns a plan, and these types are what the plan is
// made of. That separation is what lets REF run the business logic speculatively,
// reject it on a policy deny, and commit the writes exactly once at the barrier.
//
// Every LocalTransactional effect here writes through the *same* transaction,
// which it fetches from the effect store by execution id. None of them commits:
// the store does, once, after the last one has run.

// txSource is what an effect needs from the effect store. Taking an interface
// rather than the concrete store keeps the effects testable without a database.
type txSource interface {
	Tx(executionID string) (*sql.Tx, bool)
}

// requireTx fetches the shared transaction, failing loudly when it is absent. A
// missing transaction means the effect ran outside the commit path, and writing
// through the pool instead would silently break atomicity — the one thing this
// design exists to provide.
func requireTx(source txSource, executionID, effectName string) (*sql.Tx, error) {
	tx, ok := source.Tx(executionID)
	if !ok {
		return nil, fmt.Errorf("ref-app: effect %q has no open transaction for execution %q", effectName, executionID)
	}
	return tx, nil
}

// ---------------------------------------------------------------------------
// Inventory
// ---------------------------------------------------------------------------

// ReserveStockEffect moves stock from available to reserved.
//
// It implements Compensate, so a later failure in the same plan undoes it. In this
// application the compensation is belt-and-braces — the shared transaction would
// roll the reservation back anyway — but it is what makes the effect correct if
// somebody later moves it to a plan that spans two stores.
type ReserveStockEffect struct {
	Source      txSource
	ExecutionID string
	TenantID    string
	SKU         string
	Quantity    int
}

func (e *ReserveStockEffect) Name() string         { return "effect.inventory.reserve" }
func (e *ReserveStockEffect) Kind() ref.EffectKind { return ref.LocalTransactional }

func (e *ReserveStockEffect) Commit(ctx context.Context) error {
	tx, err := requireTx(e.Source, e.ExecutionID, e.Name())
	if err != nil {
		return err
	}
	return reserveStock(ctx, tx, e.TenantID, e.SKU, e.Quantity)
}

func (e *ReserveStockEffect) Compensate(ctx context.Context) error {
	tx, ok := e.Source.Tx(e.ExecutionID)
	if !ok {
		// The transaction is already gone, which means it rolled back and the
		// reservation never existed. Nothing to compensate.
		return nil
	}
	return releaseStock(ctx, tx, e.TenantID, e.SKU, e.Quantity)
}

// ReleaseStockEffect returns reserved stock to available. It is the cancellation
// path's counterpart to ReserveStockEffect.
type ReleaseStockEffect struct {
	Source      txSource
	ExecutionID string
	TenantID    string
	SKU         string
	Quantity    int
}

func (e *ReleaseStockEffect) Name() string         { return "effect.inventory.release" }
func (e *ReleaseStockEffect) Kind() ref.EffectKind { return ref.LocalTransactional }

func (e *ReleaseStockEffect) Commit(ctx context.Context) error {
	tx, err := requireTx(e.Source, e.ExecutionID, e.Name())
	if err != nil {
		return err
	}
	return releaseStock(ctx, tx, e.TenantID, e.SKU, e.Quantity)
}

// ---------------------------------------------------------------------------
// Orders
// ---------------------------------------------------------------------------

// InsertOrderEffect writes the order row.
type InsertOrderEffect struct {
	Source         txSource
	ExecutionID    string
	Order          Order
	IdempotencyKey string
}

func (e *InsertOrderEffect) Name() string         { return "effect.order.insert" }
func (e *InsertOrderEffect) Kind() ref.EffectKind { return ref.LocalTransactional }

func (e *InsertOrderEffect) Commit(ctx context.Context) error {
	tx, err := requireTx(e.Source, e.ExecutionID, e.Name())
	if err != nil {
		return err
	}
	return insertOrder(ctx, tx, e.Order, e.IdempotencyKey)
}

// TransitionOrderEffect moves an order between two statuses, refusing the write if
// it is no longer in the expected one.
type TransitionOrderEffect struct {
	Source      txSource
	ExecutionID string
	TenantID    string
	OrderID     string
	From        string
	To          string
}

func (e *TransitionOrderEffect) Name() string         { return "effect.order.transition" }
func (e *TransitionOrderEffect) Kind() ref.EffectKind { return ref.LocalTransactional }

func (e *TransitionOrderEffect) Commit(ctx context.Context) error {
	tx, err := requireTx(e.Source, e.ExecutionID, e.Name())
	if err != nil {
		return err
	}
	return setOrderStatus(ctx, tx, e.TenantID, e.OrderID, e.From, e.To)
}

// ---------------------------------------------------------------------------
// Notification: the transactional outbox
// ---------------------------------------------------------------------------

// Notification is what gets delivered. It is stored as the outbox row's payload,
// so the worker can deliver it long after the request that planned it is gone.
type Notification struct {
	Channel string         `json:"channel"`
	To      string         `json:"to"`
	Subject string         `json:"subject"`
	Body    string         `json:"body"`
	Data    map[string]any `json:"data,omitempty"`
}

// deliveryHandle is shared between the pair of effects below: the row is written
// by one and delivered by the other, and the id only exists after the insert.
type deliveryHandle struct{ id atomic.Int64 }

// WriteOutboxEffect inserts the notification into the outbox **inside the
// transaction**. This is the whole point of the outbox pattern: the promise to
// notify commits atomically with the order, so there is no window in which the
// order exists and the notification was lost, or the reverse.
type WriteOutboxEffect struct {
	Source       txSource
	ExecutionID  string
	Notification Notification
	Handle       *deliveryHandle
}

func (e *WriteOutboxEffect) Name() string         { return "effect.outbox.write" }
func (e *WriteOutboxEffect) Kind() ref.EffectKind { return ref.LocalTransactional }

func (e *WriteOutboxEffect) Commit(ctx context.Context) error {
	tx, err := requireTx(e.Source, e.ExecutionID, e.Name())
	if err != nil {
		return err
	}
	payload, err := json.Marshal(e.Notification)
	if err != nil {
		return err
	}
	var id int64
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO outbox (execution_id,effect,payload) VALUES ($1,$2,$3) RETURNING id`,
		e.ExecutionID, "notification", payload).Scan(&id); err != nil {
		return err
	}
	if e.Handle != nil {
		e.Handle.id.Store(id)
	}
	return nil
}

// DeliverNotificationEffect is the durable-delivery half. REF runs it after the
// transaction has committed, so a failure here loses nothing: the row is still
// pending and the worker will retry it with backoff.
type DeliverNotificationEffect struct {
	Outbox       *OutboxDeliverer
	Notification Notification
	Handle       *deliveryHandle
}

func (e *DeliverNotificationEffect) Name() string         { return "effect.notification.deliver" }
func (e *DeliverNotificationEffect) Kind() ref.EffectKind { return ref.DurableDelivery }

func (e *DeliverNotificationEffect) Commit(ctx context.Context) error {
	id := int64(0)
	if e.Handle != nil {
		id = e.Handle.id.Load()
	}
	if id == 0 {
		// The row was never written, which means the transaction rolled back.
		// Delivering now would notify somebody about an order that does not exist.
		return fmt.Errorf("ref-app: refusing to deliver a notification whose outbox row was not committed")
	}
	return e.Outbox.DeliverRow(ctx, id, e.Notification)
}

// ---------------------------------------------------------------------------
// Telemetry
// ---------------------------------------------------------------------------

// EmitMetricEffect is fire-and-forget: it must never fail a request, and REF
// ignores its error accordingly.
type EmitMetricEffect struct {
	Metrics *Metrics
	Metric  string
	Value   float64
}

func (e *EmitMetricEffect) Name() string         { return "effect.telemetry.emit" }
func (e *EmitMetricEffect) Kind() ref.EffectKind { return ref.FireAndForget }

func (e *EmitMetricEffect) Commit(_ context.Context) error {
	e.Metrics.AddBusiness(e.Metric, e.Value)
	return nil
}
