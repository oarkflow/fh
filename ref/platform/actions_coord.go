package platform

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/fh/ref/platform/spi"
)

// Coordination actions: locks, rate limits, circuit breakers and idempotency.
//
// These are the actions whose whole value is in what they refuse. A lock that
// hands out a lease it cannot honour, a limiter that allows when it cannot count,
// an idempotency guard that treats "I do not know" as "not seen before" — each is
// worse than not having the primitive at all, because the application was written
// believing the guarantee held.
//
// So every one of them fails closed. When the backing store cannot be reached,
// lock.acquire reports that it did not get the lock, rate_limit.check denies, and
// idempotency.guard refuses rather than allowing a possible duplicate.

func registerCoordinationActions(r *Registry) {
	mustAction(r, "lock.acquire", lockAcquireAction, ActionInfo{
		Family:       "coordination",
		Summary:      "Take a leased lock, optionally waiting for it",
		ResourceKind: "lock",
		Provides:     "An object with acquired and token",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key", Type: "expression", Required: true},
			{Name: "ttl", Type: "duration", Default: "30s", Summary: "Lease length; the lock releases itself if the holder dies"},
			{Name: "wait", Type: "duration", Summary: "How long to keep trying. Zero fails immediately."},
			{Name: "required", Type: "bool", Default: "true", Summary: "Fail when the lock cannot be taken, rather than publishing acquired=false"},
		},
	})

	mustAction(r, "lock.release", lockReleaseAction, ActionInfo{
		Family:       "coordination",
		Summary:      "Release a lock held under a token",
		ResourceKind: "lock",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key", Type: "expression", Required: true},
			{Name: "token_fact", Type: "fact", Required: true, Summary: "The token from lock.acquire"},
		},
	})

	mustAction(r, "rate_limit.check", rateLimitCheckAction, ActionInfo{
		Family:       "coordination",
		Summary:      "Consume a rate-limit token, failing when the window is exhausted",
		ResourceKind: "ratelimit",
		Provides:     "An object with remaining and reset_at",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key", Type: "expression", Required: true},
			{Name: "limit", Type: "int", Required: true},
			{Name: "window", Type: "duration", Required: true},
			{Name: "message", Type: "string"},
			{Name: "required", Type: "bool", Default: "true", Summary: "Fail when throttled, rather than publishing allowed=false"},
		},
	})

	mustAction(r, "circuit_breaker.guard", circuitBreakerGuardAction, ActionInfo{
		Family:       "coordination",
		Summary:      "Refuse to proceed while a dependency's circuit is open",
		ResourceKind: "circuit_breaker",
		Provides:     "True when the circuit admitted the call",
		Kind:         "read",
		Config: []ConfigField{
			{Name: "key", Type: "expression", Required: true},
			{Name: "message", Type: "string"},
		},
	})

	mustAction(r, "circuit_breaker.record", circuitBreakerRecordAction, ActionInfo{
		Family:       "coordination",
		Summary:      "Report a dependency call's outcome to its circuit breaker",
		ResourceKind: "circuit_breaker",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key", Type: "expression", Required: true},
			{Name: "success_fact", Type: "fact", Summary: "Truthy means success; omitted means success"},
		},
	})

	mustAction(r, "idempotency.guard", idempotencyGuardAction, ActionInfo{
		Family:       "coordination",
		Summary:      "Ensure an operation runs at most once per key, publishing the stored result on a repeat",
		ResourceKind: "cache",
		Provides:     "An object with first_time and result",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key", Type: "expression", Required: true},
			{Name: "ttl", Type: "duration", Default: "24h"},
			{Name: "prefix", Type: "string", Default: "idem:"},
			{Name: "on_duplicate", Type: "string", Default: "return", Summary: `"return" publishes the stored result; "fail" raises a conflict`},
		},
	})

	mustAction(r, "idempotency.record", idempotencyRecordAction, ActionInfo{
		Family:       "coordination",
		Summary:      "Store an operation's result against its idempotency key",
		ResourceKind: "cache",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key", Type: "expression", Required: true},
			{Name: "result_fact", Type: "fact", Required: true},
			{Name: "ttl", Type: "duration", Default: "24h"},
			{Name: "prefix", Type: "string", Default: "idem:"},
		},
	})
}

// ---------------------------------------------------------------------------
// Locks
// ---------------------------------------------------------------------------

var lockAcquireAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	locker, err := requireResource[spi.Locker](build, spec, "a lock resource")
	if err != nil {
		return nil, err
	}
	key, err := requiredExpr(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	ttl, err := configDuration(spec.Config, "ttl", 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	wait, err := configDuration(spec.Config, "wait", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	required := configBool(spec.Config, "required", true)
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		resolved, err := key.String(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		if resolved == "" {
			return ActionResult{}, invalidInput("the lock key evaluated to empty")
		}

		deadline := ctx.Now.Add(wait)
		backoff := 20 * time.Millisecond
		for {
			token, acquired, err := locker.Acquire(ctx.Context, resolved, ttl)
			if err != nil {
				return ActionResult{}, unavailable("the lock service is unavailable: %v", err)
			}
			if acquired {
				return singleOutput(spec, map[string]any{"acquired": true, "token": token, "key": resolved}), nil
			}
			if wait <= 0 || time.Now().After(deadline) {
				if required {
					return ActionResult{}, conflict("this operation is already in progress; try again shortly")
				}
				return singleOutput(spec, map[string]any{"acquired": false, "token": "", "key": resolved}), nil
			}
			select {
			case <-ctx.Context.Done():
				return ActionResult{}, ctx.Context.Err()
			case <-time.After(backoff):
			}
			// Back off up to a quarter second. Polling a contended lock every 20ms
			// for a whole second is pointless load on a shared store.
			if backoff < 250*time.Millisecond {
				backoff *= 2
			}
		}
	}), nil
})

var lockReleaseAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	locker, err := requireResource[spi.Locker](build, spec, "a lock resource")
	if err != nil {
		return nil, err
	}
	key, err := requiredExpr(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	tokenFact, err := requiredString(spec.Config, "token_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		resolved, err := key.String(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		token, err := factString(ctx.Inputs, tokenFact)
		if err != nil {
			// No token means nothing was acquired, so nothing needs releasing.
			// Failing here would break the common pattern of a release node
			// downstream of a non-required acquire.
			return acknowledgement(spec, false), nil
		}
		if err := locker.Release(ctx.Context, resolved, token); err != nil {
			return ActionResult{}, unavailable("could not release the lock: %v", err)
		}
		return acknowledgement(spec, true), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Rate limits and breakers
// ---------------------------------------------------------------------------

var rateLimitCheckAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	limiter, err := requireResource[spi.RateLimiter](build, spec, "a rate limiter resource")
	if err != nil {
		return nil, err
	}
	key, err := requiredExpr(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	limit, err := configInt(spec.Config, "limit", 0)
	if err != nil || limit <= 0 {
		return nil, fmt.Errorf("node %q: rate_limit.check needs a positive limit", spec.Name)
	}
	window, err := configDuration(spec.Config, "window", 0)
	if err != nil || window <= 0 {
		return nil, fmt.Errorf("node %q: rate_limit.check needs a positive window", spec.Name)
	}
	required := configBool(spec.Config, "required", true)
	message := configString(spec.Config, "message", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		resolved, err := key.String(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		if resolved == "" {
			return ActionResult{}, invalidInput("the rate-limit key evaluated to empty")
		}
		allowed, remaining, resetAt, err := limiter.Allow(ctx.Context, resolved, limit, window)
		if err != nil {
			return ActionResult{}, unavailable("the rate limiter is unavailable")
		}
		if !allowed && required {
			return ActionResult{}, rateLimited(message)
		}
		return acknowledgement(spec, map[string]any{
			"allowed":   allowed,
			"remaining": remaining,
			"reset_at":  resetAt,
		}), nil
	}), nil
})

var circuitBreakerGuardAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	breaker, err := requireResource[spi.CircuitBreaker](build, spec, "a circuit breaker resource")
	if err != nil {
		return nil, err
	}
	key, err := requiredExpr(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	message := configString(spec.Config, "message", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		resolved, err := key.String(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		allowed, err := breaker.Allow(ctx.Context, resolved)
		if err != nil {
			// A breaker whose state cannot be read allows the call. This is the one
			// place in this file that fails open, and deliberately: the breaker is
			// an optimisation over a dependency that may well be healthy, and
			// refusing every call because the breaker's store blipped would turn a
			// protective measure into the outage.
			return acknowledgement(spec, true), nil
		}
		if !allowed {
			if message == "" {
				message = "this dependency is temporarily unavailable; the circuit is open"
			}
			return ActionResult{}, unavailable("%s", message)
		}
		return acknowledgement(spec, true), nil
	}), nil
})

var circuitBreakerRecordAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	breaker, err := requireResource[spi.CircuitBreaker](build, spec, "a circuit breaker resource")
	if err != nil {
		return nil, err
	}
	key, err := requiredExpr(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	successFact := configString(spec.Config, "success_fact", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		resolved, err := key.String(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		success := true
		if successFact != "" {
			value, _ := resolvePath(ctx.Inputs, successFact)
			success = Truthy(value)
		}
		if err := breaker.Record(ctx.Context, resolved, success); err != nil {
			// Losing one observation does not warrant failing the request the
			// observation was about.
			return acknowledgement(spec, false), nil
		}
		return acknowledgement(spec, true), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

// idempotencyRecord is what the guard stores: a claim marker while the operation
// is in flight, then the result once it completes.
type idempotencyRecord struct {
	State     string    `json:"state"`
	Result    any       `json:"result,omitempty"`
	ClaimedAt time.Time `json:"claimed_at"`
}

var idempotencyGuardAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	cache, err := requireCache(build, spec, "")
	if err != nil {
		return nil, err
	}
	key, err := requiredExpr(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	ttl, err := configDuration(spec.Config, "ttl", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	prefix := configString(spec.Config, "prefix", "idem:")
	onDuplicate := configString(spec.Config, "on_duplicate", "return")
	if onDuplicate != "return" && onDuplicate != "fail" {
		return nil, fmt.Errorf("node %q: on_duplicate must be \"return\" or \"fail\"", spec.Name)
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		resolved, err := key.String(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		if resolved == "" {
			return ActionResult{}, invalidInput("the idempotency key evaluated to empty")
		}
		full := prefix + resolved

		raw, found, err := cache.get(ctx.Context, full)
		if err != nil {
			// Fail closed. "I cannot tell whether this already ran" must not become
			// "run it again" for an operation whose whole point is running once.
			return ActionResult{}, unavailable("the idempotency store is unavailable, so this request cannot be safely processed")
		}
		if found && len(raw) > 0 {
			var record idempotencyRecord
			if json.Unmarshal(raw, &record) == nil {
				if onDuplicate == "fail" {
					return ActionResult{}, conflict("this request has already been processed")
				}
				return singleOutput(spec, map[string]any{
					"first_time": false,
					"state":      record.State,
					"result":     record.Result,
				}), nil
			}
		}

		claim, err := json.Marshal(idempotencyRecord{State: "in_progress", ClaimedAt: ctx.Now})
		if err != nil {
			return ActionResult{}, err
		}
		if err := cache.set(ctx.Context, full, claim, ttl); err != nil {
			return ActionResult{}, unavailable("could not claim the idempotency key")
		}
		return singleOutput(spec, map[string]any{"first_time": true, "state": "in_progress", "key": resolved}), nil
	}), nil
})

var idempotencyRecordAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	cache, err := requireCache(build, spec, "")
	if err != nil {
		return nil, err
	}
	key, err := requiredExpr(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	resultFact, err := requiredString(spec.Config, "result_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	ttl, err := configDuration(spec.Config, "ttl", 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	prefix := configString(spec.Config, "prefix", "idem:")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		resolved, err := key.String(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		value, _ := resolvePath(ctx.Inputs, resultFact)
		encoded, err := json.Marshal(idempotencyRecord{State: "done", Result: value, ClaimedAt: ctx.Now})
		if err != nil {
			return ActionResult{}, err
		}
		if err := cache.set(ctx.Context, prefix+resolved, encoded, ttl); err != nil {
			// The operation already succeeded. Failing the response because the
			// record of it could not be written would be strictly worse than a
			// possible future duplicate.
			return acknowledgement(spec, false), nil
		}
		return acknowledgement(spec, true), nil
	}), nil
})
