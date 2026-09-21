package capability

import (
	"errors"
	"fmt"

	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/graph"
)

var (
	ErrRateLimitExceeded = errors.New("ref: rate limit exceeded")
)

// RateLimiterFunc checks whether the given key is allowed under rate policies.
// Returns (allowed, reason, error).
type RateLimiterFunc func(key string) (bool, string, error)

// NewRateLimitCapability creates a rate limit policy capability (DecisionNode).
func NewRateLimitCapability(name string, limiter RateLimiterFunc, keySelector func(nc *execution.NodeContext) string, opts ...Option) Registration {
	if name == "" {
		name = "capability.ratelimit"
	}
	reg := NewRegistration(name, graph.DecisionNode, opts...)

	reg.Run = func(nc *execution.NodeContext) error {
		if limiter == nil {
			nc.Decisions().RecordAllow(name, nil)
			return nil
		}

		key := ""
		if keySelector != nil {
			key = keySelector(nc)
		} else {
			key = nc.Invocation().Transport.RemoteIP
		}

		allowed, reason, err := limiter(key)
		if err != nil {
			nc.Decisions().RecordDeny(name, fmt.Sprintf("rate limit evaluation error: %v", err))
			return err
		}

		if !allowed {
			nc.Decisions().RecordDeny(name, reason)
			return ErrRateLimitExceeded
		}

		nc.Decisions().RecordAllow(name, nil)
		return nil
	}

	return reg
}

// WithTenantRateLimit configures rate limit capability to require TenantKey.
func WithTenantRateLimit(name string, limiter func(tenantID string) (bool, string, error), opts ...Option) Registration {
	reg := NewRateLimitCapability(
		name,
		limiter,
		func(nc *execution.NodeContext) string {
			if tenant, err := execution.Require(nc, TenantKey); err == nil {
				return tenant.ID
			}
			return ""
		},
		opts...,
	)
	reg.Requires = append(reg.Requires, TenantKey.Any())
	return reg
}
