package platform

import (
	"fmt"

	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/ref/intent"
)

// Session actions.
//
// Every one of these needs a session to exist, which only happens when the route
// named a session resource. A node that tries to read a session on a route
// without one fails rather than silently seeing an empty session — the second
// behaviour turns a missing `session "…"` on a route into an authorization hole
// that looks like a logged-out user.

func registerSessionActions(r *Registry) {
	mustAction(r, "session.get", sessionGetAction, ActionInfo{
		Family:       "session",
		Summary:      "Read one value from the request's session",
		ResourceKind: "session",
		Kind:         "read",
		Config: []ConfigField{
			{Name: "key", Type: "string", Required: true},
			{Name: "required", Type: "bool", Summary: "Fail when the key is absent, instead of publishing null"},
		},
	})

	mustAction(r, "session.set", sessionSetAction, ActionInfo{
		Family:       "session",
		Summary:      "Write one value into the request's session",
		ResourceKind: "session",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key", Type: "string", Required: true},
			{Name: "value_fact", Type: "fact", Required: true},
		},
	})

	mustAction(r, "session.delete", sessionDeleteAction, ActionInfo{
		Family:       "session",
		Summary:      "Remove one value from the request's session",
		ResourceKind: "session",
		Kind:         "effect",
		Config:       []ConfigField{{Name: "key", Type: "string", Required: true}},
	})

	mustAction(r, "session.all", sessionAllAction, ActionInfo{
		Family:       "session",
		Summary:      "Publish the session id and its values",
		ResourceKind: "session",
		Kind:         "read",
	})

	mustAction(r, "session.flash", sessionFlashAction, ActionInfo{
		Family:       "session",
		Summary:      "Set or consume a one-shot session message",
		ResourceKind: "session",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key", Type: "string", Required: true},
			{Name: "value_fact", Type: "fact", Summary: "Omit to consume the existing value instead of setting one"},
		},
	})
}

// requireSessionManager checks at build time that the named resource really is a
// session manager, so a mistyped resource name fails the deployment rather than
// every request.
func requireSessionManager(build BuildContext, spec NodeSpec) error {
	_, err := requireResource[*session.SessionManager](build, spec, "a session resource")
	return err
}

// currentSession fetches the live session for this invocation.
func currentSession(ctx *ActionContext) (*session.Session, error) {
	sess, ok := sessionFromContext(ctx.Context)
	if !ok {
		return nil, intent.Failure{
			Code:     "SESSION_UNAVAILABLE",
			Category: intent.CategoryUnavailable,
			Message:  "this route has no session; add a session resource to the route to use session nodes",
		}
	}
	return sess, nil
}

var sessionGetAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := requireSessionManager(build, spec); err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	key, err := requiredString(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	required := configBool(spec.Config, "required", false)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		sess, err := currentSession(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		value := sess.Get(key)
		if required && value == nil {
			return ActionResult{}, intent.Failure{
				Code:     "UNAUTHENTICATED",
				Category: intent.CategoryAuth,
				Message:  "your session has expired; sign in again",
			}
		}
		return singleOutput(spec, value), nil
	}), nil
})

var sessionSetAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := requireSessionManager(build, spec); err != nil {
		return nil, err
	}
	key, err := requiredString(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	valueFact, err := requiredString(spec.Config, "value_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		sess, err := currentSession(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		value, _ := resolvePath(ctx.Inputs, valueFact)
		sess.Set(key, value)
		return acknowledgement(spec, true), nil
	}), nil
})

var sessionDeleteAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := requireSessionManager(build, spec); err != nil {
		return nil, err
	}
	key, err := requiredString(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		sess, err := currentSession(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		sess.Delete(key)
		return acknowledgement(spec, true), nil
	}), nil
})

var sessionAllAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := requireSessionManager(build, spec); err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		sess, err := currentSession(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, sessionMap(sess)), nil
	}), nil
})

var sessionFlashAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := requireSessionManager(build, spec); err != nil {
		return nil, err
	}
	key, err := requiredString(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	valueFact := configString(spec.Config, "value_fact", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		sess, err := currentSession(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		if valueFact == "" {
			// Reading a flash consumes it, which is the whole contract of a flash
			// message: it survives exactly one subsequent request.
			return acknowledgement(spec, sess.Flash(key)), nil
		}
		value, _ := resolvePath(ctx.Inputs, valueFact)
		return acknowledgement(spec, sess.Flash(key, value)), nil
	}), nil
})
