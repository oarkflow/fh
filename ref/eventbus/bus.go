package eventbus

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Event represents an immutable fact that occurred in the business domain.
type Event struct {
	ID        string
	Aggregate string         // The business entity this event belongs to (e.g., "User")
	Type      string         // e.g., "UserCreated", "BalanceDeposited"
	Version   int            // Optimistic concurrency control
	Payload   map[string]any // The event data
	Timestamp time.Time
}

// Handler is a function that reacts to an event asynchronously.
type Handler func(ctx context.Context, e Event) error

// Bus is a CQRS Event Bus that routes domain events to asynchronous projectors.
type Bus struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
}

// NewBus creates a new Event Bus for Event Sourcing and CQRS.
func NewBus() *Bus {
	return &Bus{
		handlers: make(map[string][]Handler),
	}
}

// Subscribe attaches a handler (projector) to an event type.
func (b *Bus) Subscribe(eventType string, h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[eventType] = append(b.handlers[eventType], h)
}

// Publish broadcasts an event to all subscribed projectors asynchronously.
// In a true enterprise system, this would write to Kafka/RabbitMQ.
func (b *Bus) Publish(ctx context.Context, events ...Event) {
	for _, e := range events {
		fmt.Printf("[EVENT BUS] Published Event: %s (Agg: %s)\n", e.Type, e.Aggregate)
		
		b.mu.RLock()
		handlers := b.handlers[e.Type]
		b.mu.RUnlock()

		for _, h := range handlers {
			// Fire and forget (Asynchronous Projection)
			go func(handler Handler, evt Event) {
				// We create a background context to prevent cancellation from killing projections
				bgCtx := context.Background()
				if err := handler(bgCtx, evt); err != nil {
					fmt.Printf("[EVENT BUS ERROR] Projection failed for event %s: %v\n", evt.ID, err)
				}
			}(h, e)
		}
	}
}
