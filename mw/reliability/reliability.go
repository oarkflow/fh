package reliability

import (
	"context"
	"errors"

	"github.com/oarkflow/fh"
)

// New applies a core reliability policy to a route.
func New(policy fh.ReliabilityPolicy) fh.HandlerFunc {
	return func(c fh.Ctx) error { return c.Reliability().ApplyPolicy(c, policy) }
}

type EndpointOptions[Req any, Res any] struct {
	Policy    fh.ReliabilityPolicy
	Validate  func(fh.Ctx, *Req) error
	Handle    func(context.Context, fh.Ctx, Req) (Res, error)
	QueueType string
	Async     bool
}

func Endpoint[Req any, Res any](opt EndpointOptions[Req, Res]) fh.HandlerFunc {
	endpoint := func(c fh.Ctx) error {
		var req Req
		if err := c.BodyParser(&req); err != nil {
			return err
		}
		if opt.Validate != nil {
			if err := opt.Validate(c, &req); err != nil {
				return err
			}
		}
		if opt.Async {
			if c.Queue() == nil {
				return errors.New("fh: queue disabled")
			}
			id, err := c.Queue().Enqueue(opt.QueueType, req)
			if err != nil {
				return err
			}
			c.Lifecycle().Mark(c, fh.LifecycleQueued)
			return c.Status(fh.StatusAccepted).JSON(fh.Map{"job_id": id, "status": "accepted"})
		}
		if opt.Handle == nil {
			var zero Res
			return c.JSON(zero)
		}
		res, err := opt.Handle(c.Context(), c, req)
		if err != nil {
			return err
		}
		c.Lifecycle().Mark(c, fh.LifecycleCompleted)
		return c.JSON(res)
	}
	return func(c fh.Ctx) error { return c.RunReliableEndpoint(opt.Policy, endpoint) }
}
