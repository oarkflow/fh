package platform

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/process"
)

// Triggers: inbound webhooks and event delivery.
//
// A webhook endpoint is the one place an application accepts a mutation from
// somebody it cannot authenticate with a session or a bearer token, so the
// verification here is not optional and not configurable away:
//
//   - The signature is an HMAC over the timestamp and the raw body, compared in
//     constant time. The compiler refuses a webhook trigger with no secret.
//   - The timestamp is checked against a tolerance, which is what stops a captured
//     request from being replayed a week later.
//   - Verification runs against the *raw* body, before any parsing, because a
//     signature over a re-serialised payload verifies something the sender never
//     signed.

// compiledTrigger is one trigger with its secret and verification resolved.
type compiledTrigger struct {
	spec        TriggerSpec
	secret      []byte
	algorithm   func() hash.Hash
	engine      *process.Engine
	correlation *Expression
	// tolerance is the parsed replay window.
	tolerance time.Duration
}

// compileTriggers resolves every trigger.
func (p *Platform) compileTriggers(doc Document) error {
	for _, spec := range doc.Triggers {
		if spec.Disabled {
			continue
		}
		tolerance, err := durationField("trigger "+spec.Name, "tolerance", spec.Tolerance, 0)
		if err != nil {
			return err
		}
		trigger := compiledTrigger{spec: spec, tolerance: tolerance}

		if spec.Secret != "" {
			value, ok := p.Secret(spec.Secret)
			if !ok {
				return fmt.Errorf("ref/platform: trigger %q references secret %q, which resolved to nothing", spec.Name, spec.Secret)
			}
			if len(value) < 16 {
				return fmt.Errorf("ref/platform: trigger %q secret must contain at least 16 bytes", spec.Name)
			}
			trigger.secret = []byte(value)
		}
		algorithm, algErr := signatureHash(spec.SignatureAlgorithm)
		if algErr != nil {
			return fmt.Errorf("ref/platform: trigger %q: %w", spec.Name, algErr)
		}
		trigger.algorithm = algorithm

		if spec.Process != "" {
			engine, ok := p.processes[spec.Process]
			if !ok {
				return fmt.Errorf("ref/platform: trigger %q names unknown process %q", spec.Name, spec.Process)
			}
			trigger.engine = engine
		}
		if spec.CorrelationPath != "" {
			expr, err := CompileExpr("input." + spec.CorrelationPath)
			if err != nil {
				return fmt.Errorf("ref/platform: trigger %q correlation_path: %w", spec.Name, err)
			}
			trigger.correlation = expr
		}
		if strings.EqualFold(spec.Kind, "event") && spec.Event == "" {
			return fmt.Errorf("ref/platform: event trigger %q needs an event name", spec.Name)
		}
		p.triggers = append(p.triggers, trigger)
	}
	return nil
}

func signatureHash(name string) (func() hash.Hash, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "sha256":
		return sha256.New, nil
	case "sha512":
		return sha512.New, nil
	default:
		return nil, fmt.Errorf("unsupported signature algorithm %q (use sha256 or sha512)", name)
	}
}

// mountTriggers registers the HTTP endpoints for webhook triggers.
func (p *Platform) mountTriggers(app *fh.App) error {
	for i := range p.triggers {
		trigger := p.triggers[i]
		if !strings.EqualFold(trigger.spec.Kind, "webhook") {
			continue
		}
		app.Add("POST", trigger.spec.Path, func(c fh.Ctx) error {
			return p.serveWebhook(c, trigger)
		}).Name("trigger." + trigger.spec.Name)
	}
	return nil
}

// serveWebhook verifies and delivers one inbound webhook.
func (p *Platform) serveWebhook(c fh.Ctx, trigger compiledTrigger) error {
	body := c.Body()
	if err := trigger.verify(c, body); err != nil {
		// The response says only that verification failed. Distinguishing a bad
		// signature from a stale timestamp would help an attacker tune their attempt.
		return projectFailure(c, permissionDenied("the request signature could not be verified"))
	}

	var input any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return projectFailure(c, invalidInput("the webhook body is not valid JSON"))
		}
	}
	ctx := c.Context()

	correlation := ""
	if trigger.correlation != nil {
		value, err := trigger.correlation.String(Env{"input": input})
		if err == nil {
			correlation = value
		}
	}

	switch {
	case trigger.spec.Event != "":
		// The webhook delivers an event to whatever runs are waiting for it.
		engine, ok := p.anyEngine()
		if !ok {
			return projectFailure(c, unavailable("this application has no process engine to deliver the event to"))
		}
		woken, err := engine.Signal(ctx, trigger.spec.Event, correlation, input)
		if err != nil {
			return projectFailure(c, processFailure(err))
		}
		return c.Status(202).JSON(map[string]any{"event": trigger.spec.Event, "woken": len(woken)})

	case trigger.spec.Process != "":
		run, err := trigger.engine.Start(ctx, trigger.spec.Process, input, process.StartOptions{
			// Correlating the run on the webhook's own identifier is what makes a
			// redelivery return the original run instead of starting a second.
			IdempotencyKey: correlation,
			CorrelationID:  correlation,
			Detached:       true,
		})
		if err != nil {
			return projectFailure(c, processFailure(err))
		}
		return c.Status(202).JSON(map[string]any{"run_id": run.ID, "status": string(run.Status)})

	case trigger.spec.Queue != "":
		// Queueing rather than dispatching inline is right for a webhook: the sender
		// wants a fast acknowledgement, and retries belong to the queue.
		queue, ok := p.resources[trigger.spec.Queue].(interface {
			Enqueue(string, any, ...map[string]string) (string, error)
		})
		if !ok {
			return projectFailure(c, unavailable("the trigger queue is unavailable"))
		}
		jobID, err := queue.Enqueue("platform.trigger."+trigger.spec.Name, json.RawMessage(body),
			map[string]string{"trigger": trigger.spec.Name, "correlation": correlation})
		if err != nil {
			return projectFailure(c, unavailable("the webhook could not be queued"))
		}
		return c.Status(202).JSON(map[string]any{"job_id": jobID, "status": "queued"})

	default:
		inv := newInternalInvocation(trigger.spec.Intent, body, "", "")
		result, err := p.Engine.Dispatch(ctx, inv)
		if err != nil {
			return projectFailure(c, err)
		}
		return c.Status(200).JSON(result.Value)
	}
}

// verify checks the signature and the timestamp.
func (t compiledTrigger) verify(c fh.Ctx, body []byte) error {
	if len(t.secret) == 0 {
		// The compiler rejects a webhook trigger without a secret, so reaching here
		// means something constructed a trigger by hand. Refusing is the safe answer.
		return fmt.Errorf("this trigger has no signing secret")
	}
	header := t.spec.SignatureHeader
	if header == "" {
		header = "X-Signature"
	}
	presented := c.Get(header)
	if presented == "" {
		return fmt.Errorf("the %s header is missing", header)
	}
	// Providers commonly prefix the digest with its algorithm.
	if index := strings.IndexByte(presented, '='); index >= 0 {
		presented = presented[index+1:]
	}

	timestampHeader := t.spec.TimestampHeader
	if timestampHeader == "" {
		timestampHeader = "X-Signature-Timestamp"
	}
	timestamp := c.Get(timestampHeader)

	if t.tolerance > 0 {
		if timestamp == "" {
			return fmt.Errorf("the %s header is missing", timestampHeader)
		}
		seconds, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil {
			return fmt.Errorf("the %s header is not a Unix timestamp", timestampHeader)
		}
		skew := time.Since(time.Unix(seconds, 0))
		if skew < 0 {
			skew = -skew
		}
		if skew > t.tolerance {
			return fmt.Errorf("the request timestamp is outside the %s tolerance", t.tolerance)
		}
	}

	mac := hmac.New(t.algorithm, t.secret)
	if timestamp != "" {
		mac.Write([]byte(timestamp))
		mac.Write([]byte("."))
	}
	// The raw body, not a re-serialised one: a signature over reformatted JSON
	// verifies something the sender never signed.
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))

	if subtle.ConstantTimeCompare([]byte(expected), []byte(strings.ToLower(presented))) != 1 {
		return fmt.Errorf("the signature does not match")
	}
	return nil
}

// anyEngine returns any process engine, for delivering an event when the
// application has one store.
func (p *Platform) anyEngine() (*process.Engine, bool) {
	for _, engine := range p.engines {
		return engine, true
	}
	return nil, false
}

// SignalEvent delivers an external event to every waiting run, from host code.
func (p *Platform) SignalEvent(ctx context.Context, name, correlation string, payload any) ([]string, error) {
	var woken []string
	for _, engine := range p.engines {
		ids, err := engine.Signal(ctx, name, correlation, payload)
		if err != nil {
			return woken, processFailure(err)
		}
		woken = append(woken, ids...)
	}
	return woken, nil
}

// newInternalInvocation builds an invocation for work that did not arrive over
// HTTP: a schedule, a trigger or host code.
func newInternalInvocation(intentName string, body []byte, principalID, tenant string) *invocation.Invocation {
	return &invocation.Invocation{
		ID:     invocation.ID(newPrefixedID("int")),
		Intent: invocation.IntentID(intentName),
		Input:  invocation.NewInput(body, "application/json"),
		Metadata: invocation.NewQueueMeta("internal", 0, "", map[string]string{
			"principal_id": principalID,
			"tenant_id":    tenant,
		}),
		Transport: invocation.Transport{Protocol: "internal"},
		Received:  time.Now(),
	}
}
