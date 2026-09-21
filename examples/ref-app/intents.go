package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/intent"
)

// CreateOrderInput is the typed, validated payload for order creation.
type CreateOrderInput struct {
	SKU      string  `json:"sku"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

// CreateOrderOutput is the transport-agnostic business result.
type CreateOrderOutput struct {
	OrderID   string    `json:"order_id"`
	SKU       string    `json:"sku"`
	Quantity  int       `json:"quantity"`
	Total     float64   `json:"total"`
	TenantID  string    `json:"tenant_id"`
	UserID    string    `json:"user_id"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateOrderIntent implements ref.Intent[CreateOrderInput, CreateOrderOutput].
type CreateOrderIntent struct{}

func (CreateOrderIntent) Name() intent.Name {
	return "order.create"
}

func (CreateOrderIntent) Spec() intent.Spec {
	return intent.Spec{
		Description: "Creates an order with inventory debit and outbox notifications",
		Requires: []fact.AnyKey{
			capability.PrincipalKey.Any(),
			TenantKey.Any(),
		},
		Timeout:       2 * time.Second,
		MaxDBQueries:  5,
		MaxExternalIO: 2,
		MaxEffects:    3,
	}
}

// Run contains the pure domain logic. It does not perform side effects directly;
// instead, it schedules an EffectPlan for the effect runtime to execute atomically.
func (CreateOrderIntent) Run(nc *ref.NodeContext, in CreateOrderInput) (ref.Outcome[CreateOrderOutput], error) {
	// Validation
	if in.SKU == "" {
		return ref.Outcome[CreateOrderOutput]{}, intent.Failure{
			Code:     "INVALID_SKU",
			Category: intent.CategoryInvalidInput,
			Message:  "SKU cannot be empty",
		}
	}
	if in.Quantity <= 0 {
		return ref.Outcome[CreateOrderOutput]{}, intent.Failure{
			Code:     "INVALID_QUANTITY",
			Category: intent.CategoryInvalidInput,
			Message:  "Quantity must be greater than zero",
		}
	}

	// Retrieve pre-computed and verified facts
	principal, err := ref.Require(nc, capability.PrincipalKey)
	if err != nil {
		return ref.Outcome[CreateOrderOutput]{}, err
	}

	tenantID, err := ref.Require(nc, TenantKey)
	if err != nil {
		return ref.Outcome[CreateOrderOutput]{}, err
	}

	orderID := fmt.Sprintf("ord-%d", time.Now().UnixNano()%1000000)
	total := float64(in.Quantity) * in.Price

	// Plan mutations through the authorized effect runtime
	plan := ref.EffectPlan{
		LocalTx: []ref.Effect{
			&DebitInventoryEffect{
				TenantID: tenantID,
				SKU:      in.SKU,
				Quantity: in.Quantity,
			},
			&InsertOrderRecordEffect{
				OrderID:  orderID,
				TenantID: tenantID,
				Total:    total,
			},
		},
		Durable: []ref.Effect{
			&SendConfirmationEmailEffect{
				OrderID:  orderID,
				UserID:   principal.ID,
				SKU:      in.SKU,
				Quantity: in.Quantity,
			},
		},
		FireAndForget: []ref.Effect{
			&EmitTelemetryEffect{
				Metric: "orders.created",
				Value:  1,
			},
		},
	}

	output := CreateOrderOutput{
		OrderID:   orderID,
		SKU:       in.SKU,
		Quantity:  in.Quantity,
		Total:     total,
		TenantID:  tenantID,
		UserID:    principal.ID,
		CreatedAt: time.Now().UTC(),
	}

	return ref.Outcome[CreateOrderOutput]{
		Value:   output,
		Effects: plan,
		Meta: ref.OutcomeMeta{
			CacheControl: "no-store",
			Tags: map[string]string{
				"entity": "order",
				"id":     orderID,
			},
		},
	}, nil
}
