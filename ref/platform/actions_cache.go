package platform

import (
	"encoding/json"
	"fmt"
)

// Cache actions.
//
// The invariant worth naming: a cache failure never fails a request. A read that
// cannot reach the cache falls through to the source; a write that cannot reach
// the cache still returns success. The one exception is cache.invalidate_prefix
// on a write path, where silently failing to invalidate would serve stale data —
// there, the error surfaces.

func registerCacheActions(r *Registry) {
	mustAction(r, "cache.get", cacheGetAction, ActionInfo{
		Family:       "cache",
		Summary:      "Read a cache entry, publishing {found, value}",
		ResourceKind: "cache",
		Provides:     "An object with found and value",
		Kind:         "read",
		Config: []ConfigField{
			{Name: "key_fact", Type: "fact", Required: true},
			{Name: "prefix", Type: "string"},
		},
	})

	mustAction(r, "cache.set", cacheSetAction, ActionInfo{
		Family:       "cache",
		Summary:      "Write a cache entry",
		ResourceKind: "cache",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key_fact", Type: "fact", Required: true},
			{Name: "value_fact", Type: "fact", Required: true},
			{Name: "prefix", Type: "string"},
			{Name: "ttl", Type: "duration", Summary: "Zero means no expiry of its own"},
		},
	})

	mustAction(r, "cache.delete", cacheDeleteAction, ActionInfo{
		Family:       "cache",
		Summary:      "Invalidate one cache entry",
		ResourceKind: "cache",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key_fact", Type: "fact", Required: true},
			{Name: "prefix", Type: "string"},
		},
	})

	mustAction(r, "cache.invalidate_prefix", cacheInvalidatePrefixAction, ActionInfo{
		Family:       "cache",
		Summary:      "Invalidate every entry under a prefix. Needs a provider that can enumerate keys.",
		ResourceKind: "cache",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "prefix", Type: "string", Required: true},
			{Name: "key_fact", Type: "fact", Summary: "Appended to the prefix, to scope the sweep"},
		},
	})
}

var cacheGetAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	cache, err := requireCache(build, spec, "")
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	keyFact, err := requiredString(spec.Config, "key_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	prefix := configString(spec.Config, "prefix", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		key, err := factString(ctx.Inputs, keyFact)
		if err != nil {
			return ActionResult{}, invalidInput("%v", err)
		}
		raw, found, err := cache.get(ctx.Context, prefix+key)
		if err != nil {
			// Report a miss rather than an error. Every caller of cache.get has a
			// fallback path by construction; making them handle a cache outage
			// separately would be all downside.
			return singleOutput(spec, map[string]any{"found": false, "value": nil}), nil
		}
		var value any
		if found && len(raw) > 0 {
			if err := json.Unmarshal(raw, &value); err != nil {
				return singleOutput(spec, map[string]any{"found": false, "value": nil}), nil
			}
		}
		return singleOutput(spec, map[string]any{"found": found, "value": value}), nil
	}), nil
})

var cacheSetAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	cache, err := requireCache(build, spec, "")
	if err != nil {
		return nil, err
	}
	keyFact, err := requiredString(spec.Config, "key_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	valueFact, err := requiredString(spec.Config, "value_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	ttl, err := configDuration(spec.Config, "ttl", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	prefix := configString(spec.Config, "prefix", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		key, err := factString(ctx.Inputs, keyFact)
		if err != nil {
			return ActionResult{}, invalidInput("%v", err)
		}
		value, _ := resolvePath(ctx.Inputs, valueFact)
		encoded, err := json.Marshal(value)
		if err != nil {
			return ActionResult{}, invalidInput("the value at %q cannot be cached: %v", valueFact, err)
		}
		if err := cache.set(ctx.Context, prefix+key, encoded, ttl); err != nil {
			return acknowledgement(spec, false), nil
		}
		return acknowledgement(spec, true), nil
	}), nil
})

var cacheDeleteAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	cache, err := requireCache(build, spec, "")
	if err != nil {
		return nil, err
	}
	keyFact, err := requiredString(spec.Config, "key_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	prefix := configString(spec.Config, "prefix", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		key, err := factString(ctx.Inputs, keyFact)
		if err != nil {
			return ActionResult{}, invalidInput("%v", err)
		}
		if err := cache.delete(ctx.Context, prefix+key); err != nil {
			// An invalidation that did not happen means the next read serves stale
			// data. That is worth surfacing, unlike a failed populate.
			return ActionResult{}, unavailable("could not invalidate the cache entry: %v", err)
		}
		return acknowledgement(spec, true), nil
	}), nil
})

var cacheInvalidatePrefixAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	cache, err := requireCache(build, spec, "")
	if err != nil {
		return nil, err
	}
	if cache.prefixed == nil {
		return nil, fmt.Errorf("node %q: cache resource %q cannot enumerate keys, so it cannot invalidate a prefix. Use cache.sql, or invalidate individual keys with cache.delete",
			spec.Name, cache.name)
	}
	prefix, err := requiredString(spec.Config, "prefix")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	keyFact := configString(spec.Config, "key_fact", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		full := prefix
		if keyFact != "" {
			key, err := factString(ctx.Inputs, keyFact)
			if err != nil {
				return ActionResult{}, invalidInput("%v", err)
			}
			full += key
		}
		removed, err := cache.prefixed.DeletePrefix(full)
		if err != nil {
			return ActionResult{}, unavailable("could not invalidate cache entries under %q: %v", full, err)
		}
		return acknowledgement(spec, map[string]any{"invalidated": removed}), nil
	}), nil
})
