package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/intent"
)

// The catalogue and the orders.
//
// Every intent here is the same shape: require the facts that make the operation
// legitimate, compute, and return a plan. None of them writes to the database
// directly — that is what makes the effect barrier meaningful, and what lets the
// engine speculate the reads while the policy is still being decided.

// ---------------------------------------------------------------------------
// catalog.list — an anonymous, cached, tenant-scoped read
// ---------------------------------------------------------------------------

type CatalogInput struct{}

type CatalogOutput struct {
	TenantID string    `json:"tenant_id"`
	Products []Product `json:"products"`
	Cached   bool      `json:"cached"`
}

type CatalogIntent struct{}

func (CatalogIntent) Name() intent.Name { return "catalog.list" }

func (CatalogIntent) Spec() intent.Spec {
	return intent.Spec{
		Description: "List a tenant's active products, served from the shared cache when it is warm",
		// No PrincipalKey: the catalogue is public per tenant. The cache read is a
		// capability rather than code here so it can be speculated.
		Requires:     []fact.AnyKey{CatalogKey.Any(), AddressRateKey.Any()},
		Timeout:      3 * time.Second,
		MaxDBQueries: 1,
	}
}

func (CatalogIntent) Run(nc *ref.NodeContext, _ CatalogInput) (ref.Outcome[CatalogOutput], error) {
	catalog, err := ref.Require(nc, CatalogKey)
	if err != nil {
		return ref.Outcome[CatalogOutput]{}, err
	}
	return ref.Outcome[CatalogOutput]{
		Value: CatalogOutput{TenantID: catalog.TenantID, Products: catalog.Products, Cached: catalog.Cached},
		// Public, and briefly cacheable by a CDN: the tenant is part of the path's
		// identity via the header, so Vary matters — see mountRoutes.
		Meta: ref.OutcomeMeta{CacheControl: "public, max-age=15"},
	}, nil
}

// ---------------------------------------------------------------------------
// order.create — the flagship
// ---------------------------------------------------------------------------

type CreateOrderInput struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	// IdempotencyKey lets a client retry safely. The uniqueness is enforced by an
	// index, so two simultaneous retries cannot both create an order.
	IdempotencyKey string `json:"idempotency_key"`
}

type CreateOrderOutput struct {
	Order    Order  `json:"order"`
	Replayed bool   `json:"replayed"`
	Region   string `json:"region"`
}

type CreateOrderIntent struct{ deps *Deps }

func (CreateOrderIntent) Name() intent.Name { return "order.create" }

func (CreateOrderIntent) Spec() intent.Spec {
	return intent.Spec{
		Description: "Reserve stock and place an order, notifying the customer through the outbox",
		// This list is the security model. The engine refuses to compile an intent
		// whose facts have no producer, and refuses to run the operation until every
		// decision among them has allowed — so an order cannot be placed without an
		// identity, a validated tenant, a rate-limit allowance and a policy verdict.
		Requires: []fact.AnyKey{
			capability.PrincipalKey.Any(),
			capability.TenantKey.Any(),
			AuthorizationKey.Any(),
			PrincipalRateKey.Any(),
		},
		Timeout:      10 * time.Second,
		MaxDBQueries: 6,
		MaxEffects:   4,
	}
}

func (i CreateOrderIntent) Run(nc *ref.NodeContext, in CreateOrderInput) (ref.Outcome[CreateOrderOutput], error) {
	authz, err := ref.Require(nc, AuthorizationKey)
	if err != nil {
		return ref.Outcome[CreateOrderOutput]{}, err
	}
	if !roleHas(authz.Roles, "order:create") {
		return ref.Outcome[CreateOrderOutput]{}, forbidden("this account may not place orders")
	}
	in.SKU = strings.TrimSpace(strings.ToUpper(in.SKU))
	if in.SKU == "" {
		return ref.Outcome[CreateOrderOutput]{}, invalid("INVALID_SKU", "a sku is required")
	}
	if in.Quantity <= 0 || in.Quantity > 100 {
		return ref.Outcome[CreateOrderOutput]{}, invalid("INVALID_QUANTITY", "quantity must be between 1 and 100")
	}

	// The tenant the writes will use comes from the decision set's constraints, not
	// from this function's own idea of it. Every decision that ran contributed to
	// that value and a contradiction between any two of them would already have
	// denied the execution.
	constraints := nc.Decisions().Constraints()
	tenantID := constraints.TenantID
	if tenantID == "" {
		// A plan whose policy did not pin a tenant must not write tenant-scoped rows.
		return ref.Outcome[CreateOrderOutput]{}, forbidden("no tenant constraint was established")
	}
	region := "unknown"
	if len(constraints.Regions) > 0 {
		region = constraints.Regions[0]
	}

	if in.IdempotencyKey != "" {
		if err := nc.Budget().AcquireDBQuery(1); err != nil {
			return ref.Outcome[CreateOrderOutput]{}, err
		}
		existing, err := i.deps.Store.OrderByIdempotencyKey(nc.Context, tenantID, in.IdempotencyKey)
		switch {
		case err == nil:
			// The work is already done. Returning it with no effect plan is what
			// makes the retry free: nothing is reserved and nothing is notified twice.
			return ref.Outcome[CreateOrderOutput]{
				Value: CreateOrderOutput{Order: existing, Replayed: true, Region: region},
				Meta:  ref.OutcomeMeta{CacheControl: "no-store"},
			}, nil
		case !errors.Is(err, errNotFound):
			return ref.Outcome[CreateOrderOutput]{}, unavailable("the order could not be read")
		}
	}

	if err := nc.Budget().AcquireDBQuery(1); err != nil {
		return ref.Outcome[CreateOrderOutput]{}, err
	}
	product, err := i.deps.Store.Product(nc.Context, tenantID, in.SKU)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return ref.Outcome[CreateOrderOutput]{}, notFound(fmt.Sprintf("%s is not in this catalogue", in.SKU))
		}
		return ref.Outcome[CreateOrderOutput]{}, unavailable("the product could not be read")
	}
	// The availability check here is advice, not enforcement: by the time the effect
	// commits another order may have taken the stock. The UPDATE's own predicate is
	// what actually prevents an oversell — this check only turns the common case into
	// a clear 409 instead of a transaction failure.
	if product.Available < in.Quantity {
		return ref.Outcome[CreateOrderOutput]{}, intent.Failure{
			Code:     "INSUFFICIENT_STOCK",
			Category: intent.CategoryConflict,
			Message:  fmt.Sprintf("only %d units of %s are available", product.Available, in.SKU),
		}
	}

	order := Order{
		ID:             newID("ord-"),
		TenantID:       tenantID,
		CustomerID:     authz.PrincipalID,
		SKU:            product.SKU,
		Quantity:       in.Quantity,
		UnitPriceCents: product.PriceCents,
		TotalCents:     product.PriceCents * int64(in.Quantity),
		Currency:       product.Currency,
		Status:         "placed",
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
	}

	confirmation := Notification{
		Channel: "email",
		To:      authz.PrincipalID,
		Subject: "Order " + order.ID + " confirmed",
		Body:    fmt.Sprintf("%d × %s for %s %.2f", order.Quantity, order.SKU, order.Currency, float64(order.TotalCents)/100),
		Data: map[string]any{
			"order_id":  order.ID,
			"tenant_id": order.TenantID,
			"total":     order.TotalCents,
		},
	}
	handle := &deliveryHandle{}
	executionID := string(nc.Invocation().ID)

	// The order of LocalTx effects is the order they commit in, inside one
	// transaction: reserve the stock, write the order, write the promise to notify.
	// If any of them fails, none of them happened.
	return ref.Outcome[CreateOrderOutput]{
		Value: CreateOrderOutput{Order: order, Region: region},
		Effects: ref.EffectPlan{
			LocalTx: []ref.Effect{
				&ReserveStockEffect{Source: i.deps.Effects, ExecutionID: executionID,
					TenantID: tenantID, SKU: order.SKU, Quantity: order.Quantity},
				&InsertOrderEffect{Source: i.deps.Effects, ExecutionID: executionID,
					Order: order, IdempotencyKey: in.IdempotencyKey},
				&WriteOutboxEffect{Source: i.deps.Effects, ExecutionID: executionID,
					Notification: confirmation, Handle: handle},
			},
			Durable: []ref.Effect{
				&DeliverNotificationEffect{Outbox: i.deps.Outbox, Notification: confirmation, Handle: handle},
			},
			FireAndForget: []ref.Effect{
				&EmitMetricEffect{Metrics: i.deps.Metrics, Metric: "orders.placed", Value: 1},
				&EmitMetricEffect{Metrics: i.deps.Metrics, Metric: "orders.value_cents", Value: float64(order.TotalCents)},
			},
		},
		Meta: ref.OutcomeMeta{
			CacheControl: "no-store",
			Tags:         map[string]string{"entity": "order", "id": order.ID},
		},
	}, nil
}

// ---------------------------------------------------------------------------
// order.get and order.list
// ---------------------------------------------------------------------------

type GetOrderInput struct {
	OrderID string `json:"order_id"`
}

type GetOrderIntent struct{ deps *Deps }

func (GetOrderIntent) Name() intent.Name { return "order.get" }

func (GetOrderIntent) Spec() intent.Spec {
	return intent.Spec{
		Description:  "Read one order, scoped by the policy's constraints",
		Requires:     []fact.AnyKey{capability.PrincipalKey.Any(), capability.TenantKey.Any(), AuthorizationKey.Any(), PrincipalRateKey.Any()},
		Timeout:      3 * time.Second,
		MaxDBQueries: 1,
	}
}

func (i GetOrderIntent) Run(nc *ref.NodeContext, in GetOrderInput) (ref.Outcome[Order], error) {
	authz, err := ref.Require(nc, AuthorizationKey)
	if err != nil {
		return ref.Outcome[Order]{}, err
	}
	if strings.TrimSpace(in.OrderID) == "" {
		return ref.Outcome[Order]{}, invalid("INVALID_ORDER_ID", "an order id is required")
	}
	if err := nc.Budget().AcquireDBQuery(1); err != nil {
		return ref.Outcome[Order]{}, err
	}

	// "own" callers get their own id in the predicate; "tenant" callers get an empty
	// one, which the store reads as "anything inside this tenant". The scope came
	// from the policy, so widening it is a policy change, not a code change.
	order, err := i.deps.Store.Order(nc.Context, authz.TenantID, ownerFilter(authz), in.OrderID)
	if err != nil {
		if errors.Is(err, errNotFound) {
			// Deliberately the same answer whether the order belongs to somebody
			// else or does not exist: the difference is not the caller's business.
			return ref.Outcome[Order]{}, notFound("no such order")
		}
		return ref.Outcome[Order]{}, unavailable("the order could not be read")
	}
	return ref.Outcome[Order]{Value: order, Meta: ref.OutcomeMeta{CacheControl: "private, no-store"}}, nil
}

type ListOrdersInput struct {
	Limit int `json:"limit"`
}

type ListOrdersOutput struct {
	Orders []Order `json:"orders"`
	Scope  string  `json:"scope"`
}

type ListOrdersIntent struct{ deps *Deps }

func (ListOrdersIntent) Name() intent.Name { return "order.list" }

func (ListOrdersIntent) Spec() intent.Spec {
	return intent.Spec{
		Description:  "List orders the caller is allowed to see",
		Requires:     []fact.AnyKey{capability.PrincipalKey.Any(), capability.TenantKey.Any(), AuthorizationKey.Any(), PrincipalRateKey.Any()},
		Timeout:      3 * time.Second,
		MaxDBQueries: 1,
	}
}

func (i ListOrdersIntent) Run(nc *ref.NodeContext, in ListOrdersInput) (ref.Outcome[ListOrdersOutput], error) {
	authz, err := ref.Require(nc, AuthorizationKey)
	if err != nil {
		return ref.Outcome[ListOrdersOutput]{}, err
	}
	if err := nc.Budget().AcquireDBQuery(1); err != nil {
		return ref.Outcome[ListOrdersOutput]{}, err
	}
	orders, err := i.deps.Store.Orders(nc.Context, authz.TenantID, ownerFilter(authz), in.Limit)
	if err != nil {
		return ref.Outcome[ListOrdersOutput]{}, unavailable("the orders could not be read")
	}
	return ref.Outcome[ListOrdersOutput]{
		Value: ListOrdersOutput{Orders: orders, Scope: authz.Scope},
		Meta:  ref.OutcomeMeta{CacheControl: "private, no-store"},
	}, nil
}

// ---------------------------------------------------------------------------
// order.cancel — the compensating path
// ---------------------------------------------------------------------------

type CancelOrderInput struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

type CancelOrderOutput struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

type CancelOrderIntent struct{ deps *Deps }

func (CancelOrderIntent) Name() intent.Name { return "order.cancel" }

func (CancelOrderIntent) Spec() intent.Spec {
	return intent.Spec{
		Description:  "Cancel a placed order, returning its stock and notifying the customer",
		Requires:     []fact.AnyKey{capability.PrincipalKey.Any(), capability.TenantKey.Any(), AuthorizationKey.Any(), PrincipalRateKey.Any()},
		Timeout:      10 * time.Second,
		MaxDBQueries: 4,
		MaxEffects:   4,
	}
}

func (i CancelOrderIntent) Run(nc *ref.NodeContext, in CancelOrderInput) (ref.Outcome[CancelOrderOutput], error) {
	authz, err := ref.Require(nc, AuthorizationKey)
	if err != nil {
		return ref.Outcome[CancelOrderOutput]{}, err
	}
	if !roleHas(authz.Roles, "order:cancel") && !roleHas(authz.Roles, "order:cancel:own") {
		return ref.Outcome[CancelOrderOutput]{}, forbidden("this account may not cancel orders")
	}
	if strings.TrimSpace(in.OrderID) == "" {
		return ref.Outcome[CancelOrderOutput]{}, invalid("INVALID_ORDER_ID", "an order id is required")
	}
	if err := nc.Budget().AcquireDBQuery(1); err != nil {
		return ref.Outcome[CancelOrderOutput]{}, err
	}

	order, err := i.deps.Store.Order(nc.Context, authz.TenantID, ownerFilter(authz), in.OrderID)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return ref.Outcome[CancelOrderOutput]{}, notFound("no such order")
		}
		return ref.Outcome[CancelOrderOutput]{}, unavailable("the order could not be read")
	}
	if order.Status != "placed" {
		return ref.Outcome[CancelOrderOutput]{}, intent.Failure{
			Code:     "NOT_CANCELLABLE",
			Category: intent.CategoryConflict,
			Message:  fmt.Sprintf("order %s is %s", order.ID, order.Status),
		}
	}

	notice := Notification{
		Channel: "email",
		To:      order.CustomerID,
		Subject: "Order " + order.ID + " cancelled",
		Body:    strings.TrimSpace("Your order was cancelled. " + in.Reason),
		Data:    map[string]any{"order_id": order.ID, "tenant_id": order.TenantID},
	}
	handle := &deliveryHandle{}
	executionID := string(nc.Invocation().ID)

	// The transition runs first and refuses if the order is no longer "placed", so
	// two concurrent cancellations cannot both restock the same units: the second
	// finds no row to update and the whole transaction rolls back.
	return ref.Outcome[CancelOrderOutput]{
		Value: CancelOrderOutput{OrderID: order.ID, Status: "cancelled"},
		Effects: ref.EffectPlan{
			LocalTx: []ref.Effect{
				&TransitionOrderEffect{Source: i.deps.Effects, ExecutionID: executionID,
					TenantID: order.TenantID, OrderID: order.ID, From: "placed", To: "cancelled"},
				&ReleaseStockEffect{Source: i.deps.Effects, ExecutionID: executionID,
					TenantID: order.TenantID, SKU: order.SKU, Quantity: order.Quantity},
				&WriteOutboxEffect{Source: i.deps.Effects, ExecutionID: executionID,
					Notification: notice, Handle: handle},
			},
			Durable: []ref.Effect{
				&DeliverNotificationEffect{Outbox: i.deps.Outbox, Notification: notice, Handle: handle},
			},
			FireAndForget: []ref.Effect{
				&EmitMetricEffect{Metrics: i.deps.Metrics, Metric: "orders.cancelled", Value: 1},
			},
		},
		Meta: ref.OutcomeMeta{CacheControl: "no-store"},
	}, nil
}

// ownerFilter turns the policy's scope into a SQL predicate: an empty customer id
// means "everything in the tenant", which only a caller the policy gave tenant scope
// ever receives.
func ownerFilter(authz Authorization) string {
	if authz.Scope == "tenant" {
		return ""
	}
	return authz.PrincipalID
}
