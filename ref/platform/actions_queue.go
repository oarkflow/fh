package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/platform/spi"
)

// Queue and messaging actions.
//
// A queue publish is an async_effect in REF's terms: it commits after the policy
// gate and the effect barrier, and it may complete after the response is sent.
// That classification is what stops a denied request from having already
// enqueued its work.
//
// The transactional outbox exists for the case a queue publish cannot handle: a
// database write and a message that must either both happen or neither. The
// message goes into the same transaction as the write, and a dispatcher publishes
// it afterwards — at-least-once delivery, with the "message sent for work that
// rolled back" failure mode removed.

func registerQueueActions(r *Registry) {
	mustAction(r, "queue.publish", queuePublishAction, ActionInfo{
		Family:       "queue",
		Summary:      "Publish a durable job",
		ResourceKind: "queue",
		Provides:     "The job id",
		Kind:         "async_effect",
		Config: []ConfigField{
			{Name: "job_type", Type: "string", Required: true},
			{Name: "payload_fact", Type: "fact", Default: "input"},
			{Name: "priority", Type: "int", Summary: "Higher runs first"},
			{Name: "concurrency_key", Type: "expression", Summary: "Jobs sharing a key are not run concurrently"},
			{Name: "headers", Type: "map"},
		},
	})

	mustAction(r, "queue.publish_delayed", queuePublishDelayedAction, ActionInfo{
		Family:       "queue",
		Summary:      "Publish a durable job to run later",
		ResourceKind: "queue",
		Provides:     "The job id",
		Kind:         "async_effect",
		Config: []ConfigField{
			{Name: "job_type", Type: "string", Required: true},
			{Name: "payload_fact", Type: "fact", Default: "input"},
			{Name: "delay", Type: "duration", Summary: "Relative delay from now"},
			{Name: "run_at_fact", Type: "fact", Summary: "Absolute RFC3339 instant, taking precedence over delay"},
			{Name: "headers", Type: "map"},
		},
	})

	mustAction(r, "queue.outbox_publish", outboxPublishAction, ActionInfo{
		Family:       "outbox",
		Summary:      "Record a message in the transactional outbox for later dispatch",
		ResourceKind: "outbox",
		Provides:     "The outbox message id",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "topic", Type: "string", Required: true},
			{Name: "payload_fact", Type: "fact", Default: "input"},
			{Name: "headers", Type: "map"},
		},
	})

	mustAction(r, "queue.inbox_dedupe", inboxDedupeAction, ActionInfo{
		Family:       "outbox",
		Summary:      "Reject a message this deployment has already processed",
		ResourceKind: "outbox",
		Provides:     "True when this message is new",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "source", Type: "string", Required: true, Summary: "Namespace for the dedupe key"},
			{Name: "key_fact", Type: "fact", Summary: "Explicit dedupe key; omitted means hash the payload"},
			{Name: "payload_fact", Type: "fact", Default: "input"},
			{Name: "on_duplicate", Type: "string", Default: "skip", Summary: `"skip" publishes false; "fail" raises a conflict`},
		},
	})
}

// compileQueue resolves the queue and the job configuration one publish needs.
type queuePublish struct {
	queue       spi.JobQueue
	delayed     spi.QueueDelay
	jobType     string
	payloadFact string
	priority    int
	headers     map[string]string
	concurrency *Expression
}

func compileQueuePublish(build BuildContext, spec NodeSpec) (*queuePublish, error) {
	queue, err := requireResource[spi.JobQueue](build, spec, "a queue resource")
	if err != nil {
		return nil, err
	}
	jobType, err := requiredString(spec.Config, "job_type")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	priority, err := configInt(spec.Config, "priority", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	concurrency, err := configExpr(spec.Config, "concurrency_key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	publish := &queuePublish{
		queue:       queue,
		jobType:     jobType,
		payloadFact: configString(spec.Config, "payload_fact", "input"),
		priority:    priority,
		headers:     stringMap(spec.Config["headers"]),
		concurrency: concurrency,
	}
	publish.delayed, _ = queue.(spi.QueueDelay)
	return publish, nil
}

// payload renders the message body plus the headers that carry identity across
// the queue boundary.
//
// Carrying the principal and tenant matters: a worker running an intent on the
// other side of a queue needs the same identity the request had, or a
// tenant-scoped query in that intent has no tenant and either fails closed or —
// worse, without the scoping checks in this platform — reads everything.
func (p *queuePublish) payload(ctx *ActionContext) (any, map[string]string, error) {
	value, found := resolvePath(ctx.Inputs, p.payloadFact)
	if !found {
		return nil, nil, invalidInput("no payload at %q to publish", p.payloadFact)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, nil, invalidInput("the payload at %q cannot be serialised: %v", p.payloadFact, err)
	}
	headers := make(map[string]string, len(p.headers)+4)
	for key, header := range p.headers {
		headers[key] = header
	}
	if ctx.Principal.ID != "" {
		headers["principal_id"] = ctx.Principal.ID
	}
	if ctx.TenantID != "" {
		headers["tenant_id"] = ctx.TenantID
	}
	if ctx.Invocation != nil && ctx.Invocation.ID != "" {
		headers["origin_invocation"] = string(ctx.Invocation.ID)
	}
	return json.RawMessage(encoded), headers, nil
}

var queuePublishAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	publish, err := compileQueuePublish(build, spec)
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		body, headers, err := publish.payload(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		// A concurrency key or a priority needs the richer enqueue path, which
		// only fh's own queue exposes; a plain SPI queue gets the simple form.
		if native, ok := publish.queue.(*fh.DurableQueue); ok && (publish.priority != 0 || publish.concurrency != nil) {
			job := fh.QueueJob{Type: publish.jobType, Priority: publish.priority}
			if publish.concurrency != nil {
				key, err := publish.concurrency.String(actionEnv(ctx))
				if err != nil {
					return ActionResult{}, err
				}
				job.ConcurrencyKey = key
			}
			id, err := native.EnqueueJob(job, body, headers)
			if err != nil {
				return ActionResult{}, unavailable("could not publish the job: %v", err)
			}
			return acknowledgement(spec, map[string]any{"job_id": id, "job_type": publish.jobType}), nil
		}
		id, err := publish.queue.Enqueue(publish.jobType, body, headers)
		if err != nil {
			return ActionResult{}, unavailable("could not publish the job: %v", err)
		}
		return acknowledgement(spec, map[string]any{"job_id": id, "job_type": publish.jobType}), nil
	}), nil
})

var queuePublishDelayedAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	publish, err := compileQueuePublish(build, spec)
	if err != nil {
		return nil, err
	}
	if publish.delayed == nil {
		return nil, fmt.Errorf("node %q: queue resource %q cannot schedule delayed jobs", spec.Name, spec.Resource)
	}
	delay, err := configDuration(spec.Config, "delay", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	runAtFact := configString(spec.Config, "run_at_fact", "")
	if delay <= 0 && runAtFact == "" {
		return nil, fmt.Errorf("node %q: queue.publish_delayed needs either config.delay or config.run_at_fact", spec.Name)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		body, headers, err := publish.payload(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		runAt := ctx.Now.Add(delay)
		if runAtFact != "" {
			raw, found := resolvePath(ctx.Inputs, runAtFact)
			if !found {
				return ActionResult{}, invalidInput("no instant at %q", runAtFact)
			}
			parsed, err := parseInstant(raw)
			if err != nil {
				return ActionResult{}, invalidInput("%q is not a timestamp: %v", runAtFact, err)
			}
			runAt = parsed
		}
		id, err := publish.delayed.EnqueueDelayed(publish.jobType, body, runAt, headers)
		if err != nil {
			return ActionResult{}, unavailable("could not schedule the job: %v", err)
		}
		return acknowledgement(spec, map[string]any{"job_id": id, "job_type": publish.jobType, "run_at": runAt.UTC().Format(time.RFC3339)}), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Outbox and inbox
// ---------------------------------------------------------------------------

type outboxStore interface {
	SaveOutbox(context.Context, *fh.OutboxMessage) error
	BeginInbox(context.Context, fh.InboxMessage) (bool, error)
}

var outboxPublishAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	store, err := requireResource[outboxStore](build, spec, "an outbox resource")
	if err != nil {
		return nil, err
	}
	topic, err := requiredString(spec.Config, "topic")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	payloadFact := configString(spec.Config, "payload_fact", "input")
	headers := stringMap(spec.Config["headers"])

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, found := resolvePath(ctx.Inputs, payloadFact)
		if !found {
			return ActionResult{}, invalidInput("no payload at %q", payloadFact)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return ActionResult{}, invalidInput("the payload cannot be serialised: %v", err)
		}
		message := &fh.OutboxMessage{
			ID:      newPrefixedID("obx"),
			Topic:   topic,
			Payload: encoded,
			Headers: headers,
		}
		if err := store.SaveOutbox(ctx.Context, message); err != nil {
			return ActionResult{}, unavailable("could not record the outbox message: %v", err)
		}
		return acknowledgement(spec, map[string]any{"message_id": message.ID, "topic": topic}), nil
	}), nil
})

var inboxDedupeAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	store, err := requireResource[outboxStore](build, spec, "an outbox resource")
	if err != nil {
		return nil, err
	}
	source, err := requiredString(spec.Config, "source")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	keyFact := configString(spec.Config, "key_fact", "")
	payloadFact := configString(spec.Config, "payload_fact", "input")
	onDuplicate := configString(spec.Config, "on_duplicate", "skip")
	if onDuplicate != "skip" && onDuplicate != "fail" {
		return nil, fmt.Errorf("node %q: on_duplicate must be \"skip\" or \"fail\"", spec.Name)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		var dedupeKey string
		if keyFact != "" {
			key, err := factString(ctx.Inputs, keyFact)
			if err != nil {
				return ActionResult{}, invalidInput("%v", err)
			}
			dedupeKey = fh.InboxDedupeKey(source, []byte(key))
		} else {
			value, _ := resolvePath(ctx.Inputs, payloadFact)
			encoded, err := json.Marshal(value)
			if err != nil {
				return ActionResult{}, invalidInput("the payload cannot be hashed: %v", err)
			}
			dedupeKey = fh.InboxDedupeKey(source, encoded)
		}
		fresh, err := store.BeginInbox(ctx.Context, fh.InboxMessage{ID: dedupeKey, Source: source, CreatedAt: ctx.Now})
		if err != nil {
			return ActionResult{}, unavailable("could not check the inbox: %v", err)
		}
		if !fresh && onDuplicate == "fail" {
			return ActionResult{}, conflict("this message has already been processed")
		}
		return acknowledgement(spec, fresh), nil
	}), nil
})

// parseInstant accepts the timestamp shapes that reach configuration: a real
// time, an RFC3339 string, or a Unix epoch number.
func parseInstant(value any) (time.Time, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed, nil
	case string:
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"} {
			if parsed, err := time.Parse(layout, typed); err == nil {
				return parsed, nil
			}
		}
		return time.Time{}, fmt.Errorf("%q is not an RFC3339 timestamp", typed)
	default:
		if seconds, ok := ToFloat(value); ok && seconds > 0 {
			return time.Unix(int64(seconds), 0).UTC(), nil
		}
		return time.Time{}, fmt.Errorf("cannot read %T as a timestamp", value)
	}
}
