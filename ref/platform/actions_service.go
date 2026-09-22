package platform

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/oarkflow/fh/ref/platform/spi"
)

// Outbound service actions: HTTP, GraphQL, mail, notifications and models.
//
// Every one of them goes through a configured resource that already enforces the
// destination allowlist, the timeout and the response size limit. A node supplies
// the path, the body and the headers; it cannot widen what the resource permits.
// That split is what makes an outbound call configurable without making it a
// server-side request forgery primitive.

func registerServiceActions(r *Registry) {
	mustAction(r, "service.http", httpCallAction, ActionInfo{
		Family:       "service",
		Summary:      "Call an HTTP endpoint through a configured client",
		ResourceKind: "service",
		Provides:     "An object with status, headers and body",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "method", Type: "string", Default: "GET"},
			{Name: "url", Type: "template", Required: true, Summary: "Absolute, or relative to the resource's base_url"},
			{Name: "body_fact", Type: "fact", Summary: "Fact serialised as the JSON request body"},
			{Name: "body", Type: "map", Summary: "Literal body, with templated string values"},
			{Name: "headers", Type: "map", Summary: "Per-call headers, templated"},
			{Name: "query", Type: "map", Summary: "Query parameters, templated"},
			{Name: "expect_status", Type: "[]int", Summary: "Statuses treated as success. Default is 2xx."},
			{Name: "raw", Type: "bool", Summary: "Publish the response body as text rather than parsed JSON"},
		},
	})

	mustAction(r, "service.graphql", graphQLAction, ActionInfo{
		Family:       "service",
		Summary:      "Send a GraphQL query or mutation and publish its data, failing on GraphQL errors",
		ResourceKind: "service",
		Provides:     "The data field of the GraphQL response",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "url", Type: "template", Default: "/graphql"},
			{Name: "query", Type: "string", Required: true},
			{Name: "operation_name", Type: "string"},
			{Name: "variables_fact", Type: "fact"},
			{Name: "headers", Type: "map"},
		},
	})

	mustAction(r, "service.smtp", mailSendAction, ActionInfo{
		Family:       "service",
		Summary:      "Send mail through a configured SMTP resource",
		ResourceKind: "service",
		Provides:     "An object with message_id",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "to", Type: "[]template", Summary: "Recipients; may be templated"},
			{Name: "to_fact", Type: "fact", Summary: "Fact holding recipients, taking precedence over to"},
			{Name: "cc", Type: "[]template"},
			{Name: "bcc", Type: "[]template"},
			{Name: "from", Type: "template", Summary: "Overrides the resource's default From"},
			{Name: "reply_to", Type: "template"},
			{Name: "subject", Type: "template", Required: true},
			{Name: "body", Type: "template", Summary: "Plain-text body"},
			{Name: "html", Type: "template", Summary: "HTML body"},
			{Name: "headers", Type: "map"},
		},
	})

	mustAction(r, "notify.send", notifySendAction, ActionInfo{
		Family:   "notification",
		Summary:  "Deliver a notification through whichever channel resource is named — mail, HTTP, queue or a host adapter",
		Provides: "An object with delivered and reference",
		Kind:     "effect",
		Config: []ConfigField{
			{Name: "channel", Type: "resource", Required: true},
			{Name: "target", Type: "template", Summary: "Address for the channel: an email, a URL, a phone number, a topic"},
			{Name: "target_fact", Type: "fact"},
			{Name: "subject", Type: "template"},
			{Name: "body", Type: "template"},
			{Name: "data_fact", Type: "fact", Summary: "Structured payload for channels that carry one"},
			{Name: "job_type", Type: "string", Summary: "Job type, when the channel is a queue"},
		},
	})

	mustAction(r, "service.llm_chat", llmChatAction, ActionInfo{
		Family:       "intelligence",
		Summary:      "Send a chat completion request and publish the text plus token usage",
		ResourceKind: "service",
		Provides:     "An object with text, model and token counts",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "model", Type: "string", Summary: "Overrides the resource default"},
			{Name: "system", Type: "template"},
			{Name: "prompt", Type: "template", Summary: "A single user message"},
			{Name: "messages_fact", Type: "fact", Summary: "A list of {role, content}, taking precedence over prompt"},
			{Name: "temperature", Type: "number"},
			{Name: "max_tokens", Type: "int"},
			{Name: "json", Type: "bool", Summary: "Parse the reply as JSON and publish the value"},
		},
	})

	mustAction(r, "service.llm_embed", llmEmbedAction, ActionInfo{
		Family:       "intelligence",
		Summary:      "Compute embeddings for one or more strings",
		ResourceKind: "service",
		Provides:     "A list of vectors",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "model", Type: "string"},
			{Name: "input_fact", Type: "fact", Required: true, Summary: "A string or a list of strings"},
		},
	})

	mustAction(r, "search.index", searchIndexAction, ActionInfo{
		Family:       "search",
		Summary:      "Index documents for search",
		ResourceKind: "search",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "collection", Type: "string", Required: true},
			{Name: "documents_fact", Type: "fact", Required: true, Summary: "A record or a list of records"},
			{Name: "id_field", Type: "string", Default: "id"},
			{Name: "fields", Type: "[]string", Summary: "Fields to index. Empty indexes every field."},
		},
	})

	mustAction(r, "search.query", searchQueryAction, ActionInfo{
		Family:       "search",
		Summary:      "Search a collection",
		ResourceKind: "search",
		Provides:     "An object with hits and total",
		Kind:         "read",
		Config: []ConfigField{
			{Name: "collection", Type: "string", Required: true},
			{Name: "text", Type: "template"},
			{Name: "text_fact", Type: "fact"},
			{Name: "filters", Type: "map"},
			{Name: "limit", Type: "int", Default: "20"},
			{Name: "offset_fact", Type: "fact"},
		},
	})

	mustAction(r, "search.delete", searchDeleteAction, ActionInfo{
		Family:       "search",
		Summary:      "Remove documents from a search collection",
		ResourceKind: "search",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "collection", Type: "string", Required: true},
			{Name: "ids_fact", Type: "fact", Required: true},
		},
	})
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

// compiledHTTPCall holds everything one outbound call needs, compiled once.
type compiledHTTPCall struct {
	service      *HTTPService
	method       string
	url          *Template
	bodyFact     string
	body         map[string]*Template
	bodyLiteral  map[string]any
	headers      map[string]*Template
	query        map[string]*Template
	expectStatus []int
	raw          bool
}

func compileHTTPCall(build BuildContext, spec NodeSpec, defaultURL, defaultMethod string) (*compiledHTTPCall, error) {
	service, err := requireResource[*HTTPService](build, spec, "a service.http resource")
	if err != nil {
		return nil, err
	}
	url, err := configTemplate(spec.Config, "url", defaultURL)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if url == nil {
		return nil, fmt.Errorf("node %q: config.url is required", spec.Name)
	}
	call := &compiledHTTPCall{
		service:  service,
		method:   strings.ToUpper(configString(spec.Config, "method", defaultMethod)),
		url:      url,
		bodyFact: configString(spec.Config, "body_fact", ""),
		raw:      configBool(spec.Config, "raw", false),
	}
	if call.headers, err = templateMap(spec, "headers"); err != nil {
		return nil, err
	}
	if call.query, err = templateMap(spec, "query"); err != nil {
		return nil, err
	}
	if literal := configMap(spec.Config, "body"); len(literal) > 0 {
		call.bodyLiteral = literal
		call.body = map[string]*Template{}
		for key, value := range literal {
			text, ok := value.(string)
			if !ok {
				continue
			}
			tmpl, err := CompileTemplate(text)
			if err != nil {
				return nil, fmt.Errorf("node %q: body.%s: %w", spec.Name, key, err)
			}
			if tmpl.HasHoles() {
				call.body[key] = tmpl
			}
		}
	}
	for _, status := range configStrings(spec.Config, "expect_status") {
		value, ok := ToFloat(status)
		if !ok {
			return nil, fmt.Errorf("node %q: expect_status must contain integers", spec.Name)
		}
		call.expectStatus = append(call.expectStatus, int(value))
	}
	return call, nil
}

func templateMap(spec NodeSpec, key string) (map[string]*Template, error) {
	raw := stringMap(spec.Config[key])
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]*Template, len(raw))
	for name, value := range raw {
		tmpl, err := CompileTemplate(value)
		if err != nil {
			return nil, fmt.Errorf("node %q: %s.%s: %w", spec.Name, key, name, err)
		}
		out[name] = tmpl
	}
	return out, nil
}

func renderTemplateMap(templates map[string]*Template, env Env) (map[string]string, error) {
	if len(templates) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(templates))
	for name, tmpl := range templates {
		value, err := tmpl.Render(env)
		if err != nil {
			return nil, err
		}
		out[name] = value
	}
	return out, nil
}

// request assembles the outbound call for one invocation.
func (c *compiledHTTPCall) request(ctx *ActionContext) (HTTPRequest, error) {
	env := actionEnv(ctx)
	url, err := c.url.Render(env)
	if err != nil {
		return HTTPRequest{}, err
	}
	headers, err := renderTemplateMap(c.headers, env)
	if err != nil {
		return HTTPRequest{}, err
	}
	query, err := renderTemplateMap(c.query, env)
	if err != nil {
		return HTTPRequest{}, err
	}

	var body []byte
	switch {
	case c.bodyFact != "":
		value, found := resolvePath(ctx.Inputs, c.bodyFact)
		if !found {
			return HTTPRequest{}, invalidInput("no body at %q", c.bodyFact)
		}
		if body, err = json.Marshal(value); err != nil {
			return HTTPRequest{}, invalidInput("the body cannot be serialised: %v", err)
		}
	case c.bodyLiteral != nil:
		payload := make(map[string]any, len(c.bodyLiteral))
		for key, value := range c.bodyLiteral {
			if tmpl, templated := c.body[key]; templated {
				rendered, err := tmpl.Render(env)
				if err != nil {
					return HTTPRequest{}, err
				}
				payload[key] = rendered
				continue
			}
			payload[key] = value
		}
		if body, err = json.Marshal(payload); err != nil {
			return HTTPRequest{}, err
		}
	}
	return HTTPRequest{Method: c.method, URL: url, Headers: headers, Query: query, Body: body}, nil
}

// accepts reports whether a status counts as success. An explicit expect_status
// replaces the default entirely, which is what lets a node treat 404 as a normal
// "absent" answer rather than an error.
func (c *compiledHTTPCall) accepts(status int) bool {
	if len(c.expectStatus) == 0 {
		return status >= 200 && status < 300
	}
	for _, expected := range c.expectStatus {
		if status == expected {
			return true
		}
	}
	return false
}

func (c *compiledHTTPCall) result(spec NodeSpec, response HTTPResponse) ActionResult {
	value := map[string]any{"status": response.Status, "headers": response.Headers}
	if c.raw {
		value["body"] = string(response.Body)
	} else if response.JSON != nil {
		value["body"] = response.JSON
	} else {
		value["body"] = response.Text
	}
	return acknowledgement(spec, value)
}

var httpCallAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileHTTPCall(build, spec, "", http.MethodGet)
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		request, err := call.request(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		response, err := call.service.Do(ctx.Context, request)
		if err != nil && !call.accepts(response.Status) {
			return ActionResult{}, upstreamFailure(err, response.Status)
		}
		if !call.accepts(response.Status) {
			return ActionResult{}, upstreamFailure(fmt.Errorf("upstream returned %d", response.Status), response.Status)
		}
		return call.result(spec, response), nil
	}), nil
})

// upstreamFailure maps another service's status onto a category of our own, so a
// retry policy and the caller both see something truthful: their bad request is
// a 422 here, and the upstream's outage is a 503, not a generic 500.
func upstreamFailure(err error, status int) error {
	switch {
	case status == http.StatusNotFound:
		return notFound("upstream resource", "")
	case status == http.StatusConflict:
		return conflict("the upstream service reported a conflict")
	case status == http.StatusTooManyRequests:
		return rateLimited("the upstream service is rate limiting this deployment")
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return unavailable("the upstream service rejected our credentials")
	case status >= 400 && status < 500:
		return invalidInput("the upstream service rejected the request (%d)", status)
	case status >= 500:
		return unavailable("the upstream service is failing (%d)", status)
	default:
		return unavailable("the upstream call failed: %v", err)
	}
}

var graphQLAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileHTTPCall(build, spec, "/graphql", http.MethodPost)
	if err != nil {
		return nil, err
	}
	query, err := requiredString(spec.Config, "query")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	operationName := configString(spec.Config, "operation_name", "")
	variablesFact := configString(spec.Config, "variables_fact", "")
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		payload := map[string]any{"query": query}
		if operationName != "" {
			payload["operationName"] = operationName
		}
		if variablesFact != "" {
			if variables, found := resolvePath(ctx.Inputs, variablesFact); found {
				payload["variables"] = variables
			}
		}
		body, err := json.Marshal(payload)
		if err != nil {
			return ActionResult{}, err
		}
		request, err := call.request(ctx)
		if err != nil {
			return ActionResult{}, err
		}
		request.Method, request.Body = http.MethodPost, body
		if request.Headers == nil {
			request.Headers = map[string]string{}
		}
		request.Headers["Content-Type"] = "application/json"

		response, err := call.service.Do(ctx.Context, request)
		if err != nil {
			return ActionResult{}, upstreamFailure(err, response.Status)
		}
		var decoded struct {
			Data   any `json:"data"`
			Errors []struct {
				Message string `json:"message"`
			} `json:"errors"`
		}
		if err := json.Unmarshal(response.Body, &decoded); err != nil {
			return ActionResult{}, unavailable("the GraphQL response could not be parsed: %v", err)
		}
		// A GraphQL error arrives with HTTP 200, so the status check above would
		// have passed it. Surfacing it as a failure is the whole reason this action
		// exists rather than an author using service.http directly.
		if len(decoded.Errors) > 0 {
			messages := make([]string, 0, len(decoded.Errors))
			for _, item := range decoded.Errors {
				messages = append(messages, item.Message)
			}
			return ActionResult{}, unavailable("the GraphQL call failed: %s", strings.Join(messages, "; "))
		}
		return singleOutput(spec, decoded.Data), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Mail and notifications
// ---------------------------------------------------------------------------

type compiledMail struct {
	mailer  spi.Mailer
	to      []*Template
	toFact  string
	cc      []*Template
	bcc     []*Template
	from    *Template
	replyTo *Template
	subject *Template
	body    *Template
	html    *Template
	headers map[string]*Template
}

func compileMail(build BuildContext, spec NodeSpec) (*compiledMail, error) {
	mailer, err := requireResource[spi.Mailer](build, spec, "a service.smtp resource")
	if err != nil {
		return nil, err
	}
	mail := &compiledMail{mailer: mailer, toFact: configString(spec.Config, "to_fact", "")}
	for _, pair := range []struct {
		key    string
		target **Template
	}{
		{"from", &mail.from},
		{"reply_to", &mail.replyTo},
		{"subject", &mail.subject},
		{"body", &mail.body},
		{"html", &mail.html},
	} {
		tmpl, err := configTemplate(spec.Config, pair.key, "")
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", spec.Name, err)
		}
		*pair.target = tmpl
	}
	if mail.subject == nil {
		return nil, fmt.Errorf("node %q: a message needs a subject", spec.Name)
	}
	if mail.body == nil && mail.html == nil {
		return nil, fmt.Errorf("node %q: a message needs body or html", spec.Name)
	}
	for _, pair := range []struct {
		key    string
		target *[]*Template
	}{{"to", &mail.to}, {"cc", &mail.cc}, {"bcc", &mail.bcc}} {
		for _, raw := range configStrings(spec.Config, pair.key) {
			tmpl, err := CompileTemplate(raw)
			if err != nil {
				return nil, fmt.Errorf("node %q: %s: %w", spec.Name, pair.key, err)
			}
			*pair.target = append(*pair.target, tmpl)
		}
	}
	if len(mail.to) == 0 && mail.toFact == "" {
		return nil, fmt.Errorf("node %q: a message needs to or to_fact", spec.Name)
	}
	var err2 error
	if mail.headers, err2 = templateMap(spec, "headers"); err2 != nil {
		return nil, err2
	}
	return mail, nil
}

func renderAddresses(templates []*Template, env Env) ([]string, error) {
	out := make([]string, 0, len(templates))
	for _, tmpl := range templates {
		value, err := tmpl.Render(env)
		if err != nil {
			return nil, err
		}
		for _, address := range strings.Split(value, ",") {
			if trimmed := strings.TrimSpace(address); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out, nil
}

var mailSendAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	mail, err := compileMail(build, spec)
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		env := actionEnv(ctx)
		message := spi.Mail{}
		if mail.toFact != "" {
			value, found := resolvePath(ctx.Inputs, mail.toFact)
			if !found {
				return ActionResult{}, invalidInput("no recipients at %q", mail.toFact)
			}
			message.To = stringSlice(value)
		}
		if len(message.To) == 0 {
			if message.To, err = renderAddresses(mail.to, env); err != nil {
				return ActionResult{}, err
			}
		}
		if len(message.To) == 0 {
			return ActionResult{}, invalidInput("the message has no recipients")
		}
		if message.CC, err = renderAddresses(mail.cc, env); err != nil {
			return ActionResult{}, err
		}
		if message.BCC, err = renderAddresses(mail.bcc, env); err != nil {
			return ActionResult{}, err
		}
		for _, pair := range []struct {
			tmpl   *Template
			target *string
		}{
			{mail.from, &message.From},
			{mail.replyTo, &message.ReplyTo},
			{mail.subject, &message.Subject},
			{mail.body, &message.Body},
			{mail.html, &message.HTML},
		} {
			if pair.tmpl == nil {
				continue
			}
			value, err := pair.tmpl.Render(env)
			if err != nil {
				return ActionResult{}, err
			}
			*pair.target = value
		}
		if message.Headers, err = renderTemplateMap(mail.headers, env); err != nil {
			return ActionResult{}, err
		}
		messageID, err := mail.mailer.Send(ctx.Context, message)
		if err != nil {
			return ActionResult{}, unavailable("the message could not be sent: %v", err)
		}
		return acknowledgement(spec, map[string]any{"message_id": messageID, "recipients": len(message.To)}), nil
	}), nil
})

// notifySendAction routes to whichever channel the named resource actually is.
//
// The resolution happens at build time, so a channel that cannot deliver a
// notification is a compile error rather than a silent no-op at 3am.
var notifySendAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	name, err := requiredString(spec.Config, "channel")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	channel, ok := build.Resource(name)
	if !ok {
		return nil, fmt.Errorf("node %q: notification channel %q is not declared", spec.Name, name)
	}

	target, err := configTemplate(spec.Config, "target", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	subject, err := configTemplate(spec.Config, "subject", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	body, err := configTemplate(spec.Config, "body", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	targetFact := configString(spec.Config, "target_fact", "")
	dataFact := configString(spec.Config, "data_fact", "")
	jobType := configString(spec.Config, "job_type", "notification.send")

	resolveTarget := func(ctx *ActionContext, env Env) (string, error) {
		if targetFact != "" {
			if value, found := resolvePath(ctx.Inputs, targetFact); found {
				return Stringify(value), nil
			}
		}
		if target == nil {
			return "", invalidInput("no notification target configured")
		}
		return target.Render(env)
	}

	switch typed := channel.(type) {
	case spi.Mailer:
		return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
			env := actionEnv(ctx)
			recipient, err := resolveTarget(ctx, env)
			if err != nil {
				return ActionResult{}, err
			}
			message := spi.Mail{To: []string{recipient}}
			if subject != nil {
				if message.Subject, err = subject.Render(env); err != nil {
					return ActionResult{}, err
				}
			}
			if body != nil {
				if message.Body, err = body.Render(env); err != nil {
					return ActionResult{}, err
				}
			}
			reference, err := typed.Send(ctx.Context, message)
			if err != nil {
				return ActionResult{}, unavailable("the notification could not be sent: %v", err)
			}
			return acknowledgement(spec, map[string]any{"delivered": true, "reference": reference, "channel": name}), nil
		}), nil

	case spi.Notifier:
		return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
			env := actionEnv(ctx)
			recipient, err := resolveTarget(ctx, env)
			if err != nil {
				return ActionResult{}, err
			}
			message := spi.Notification{Target: recipient}
			if subject != nil {
				if message.Subject, err = subject.Render(env); err != nil {
					return ActionResult{}, err
				}
			}
			if body != nil {
				if message.Body, err = body.Render(env); err != nil {
					return ActionResult{}, err
				}
			}
			if dataFact != "" {
				if value, found := resolvePath(ctx.Inputs, dataFact); found {
					message.Data, _ = value.(map[string]any)
				}
			}
			reference, err := typed.Notify(ctx.Context, message)
			if err != nil {
				return ActionResult{}, unavailable("the notification could not be sent: %v", err)
			}
			return acknowledgement(spec, map[string]any{"delivered": true, "reference": reference, "channel": name}), nil
		}), nil

	case spi.JobQueue:
		// Queueing a notification is the right default for anything that must not
		// fail the request it was triggered by: the send is retried by the worker
		// rather than tied to this response.
		return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
			env := actionEnv(ctx)
			recipient, err := resolveTarget(ctx, env)
			if err != nil {
				return ActionResult{}, err
			}
			payload := map[string]any{"target": recipient, "channel": name}
			if subject != nil {
				if payload["subject"], err = subject.Render(env); err != nil {
					return ActionResult{}, err
				}
			}
			if body != nil {
				if payload["body"], err = body.Render(env); err != nil {
					return ActionResult{}, err
				}
			}
			if dataFact != "" {
				payload["data"], _ = resolvePath(ctx.Inputs, dataFact)
			}
			id, err := typed.Enqueue(jobType, payload)
			if err != nil {
				return ActionResult{}, unavailable("the notification could not be queued: %v", err)
			}
			return acknowledgement(spec, map[string]any{"delivered": false, "queued": true, "reference": id, "channel": name}), nil
		}), nil

	case *HTTPService:
		return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
			env := actionEnv(ctx)
			recipient, err := resolveTarget(ctx, env)
			if err != nil {
				return ActionResult{}, err
			}
			payload := map[string]any{"target": recipient}
			if subject != nil {
				if payload["subject"], err = subject.Render(env); err != nil {
					return ActionResult{}, err
				}
			}
			if body != nil {
				if payload["body"], err = body.Render(env); err != nil {
					return ActionResult{}, err
				}
			}
			if dataFact != "" {
				payload["data"], _ = resolvePath(ctx.Inputs, dataFact)
			}
			encoded, err := json.Marshal(payload)
			if err != nil {
				return ActionResult{}, err
			}
			response, err := typed.Do(ctx.Context, HTTPRequest{Method: http.MethodPost, URL: recipient, Body: encoded})
			if err != nil {
				return ActionResult{}, upstreamFailure(err, response.Status)
			}
			return acknowledgement(spec, map[string]any{"delivered": true, "status": response.Status, "channel": name}), nil
		}), nil

	default:
		return nil, fmt.Errorf("node %q: resource %q cannot deliver notifications. Use a service.smtp, service.http or queue resource, or register a Notifier adapter",
			spec.Name, name)
	}
})

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

var llmChatAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	model, err := requireResource[spi.LLM](build, spec, "a service.llm resource")
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	system, err := configTemplate(spec.Config, "system", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	prompt, err := configTemplate(spec.Config, "prompt", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	messagesFact := configString(spec.Config, "messages_fact", "")
	if prompt == nil && messagesFact == "" {
		return nil, fmt.Errorf("node %q: service.llm_chat needs config.prompt or config.messages_fact", spec.Name)
	}
	maxTokens, err := configInt(spec.Config, "max_tokens", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	var temperature *float64
	if _, present := spec.Config["temperature"]; present {
		value, err := configFloat(spec.Config, "temperature", 0)
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", spec.Name, err)
		}
		temperature = &value
	}
	modelName := configString(spec.Config, "model", "")
	wantJSON := configBool(spec.Config, "json", false)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		env := actionEnv(ctx)
		request := spi.ChatRequest{Model: modelName, MaxTokens: maxTokens, Temperature: temperature}
		if system != nil {
			if request.System, err = system.Render(env); err != nil {
				return ActionResult{}, err
			}
		}
		if messagesFact != "" {
			value, found := resolvePath(ctx.Inputs, messagesFact)
			if !found {
				return ActionResult{}, invalidInput("no messages at %q", messagesFact)
			}
			items, err := requiredList(value, messagesFact)
			if err != nil {
				return ActionResult{}, err
			}
			for _, item := range items {
				object, ok := item.(map[string]any)
				if !ok {
					return ActionResult{}, invalidInput("each message must be an object with role and content")
				}
				request.Messages = append(request.Messages, spi.ChatMessage{
					Role:    Stringify(object["role"]),
					Content: Stringify(object["content"]),
				})
			}
		} else {
			text, err := prompt.Render(env)
			if err != nil {
				return ActionResult{}, err
			}
			request.Messages = []spi.ChatMessage{{Role: "user", Content: text}}
		}

		response, err := model.Chat(ctx.Context, request)
		if err != nil {
			return ActionResult{}, err
		}
		value := map[string]any{
			"text":          response.Text,
			"model":         response.Model,
			"finish_reason": response.FinishReason,
			"input_tokens":  response.InputTokens,
			"output_tokens": response.OutputTokens,
		}
		if wantJSON {
			var parsed any
			// Models routinely wrap JSON in a fenced code block. Stripping it is
			// not cleverness; it is the difference between this working and not.
			if err := json.Unmarshal([]byte(stripCodeFence(response.Text)), &parsed); err != nil {
				return ActionResult{}, invalidInput("the model did not return valid JSON: %v", err)
			}
			value["value"] = parsed
		}
		return singleOutput(spec, value), nil
	}), nil
})

func stripCodeFence(text string) string {
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, "```") {
		return trimmed
	}
	if newline := strings.IndexByte(trimmed, '\n'); newline >= 0 {
		trimmed = trimmed[newline+1:]
	}
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(trimmed), "```"))
}

var llmEmbedAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	embedder, err := requireResource[spi.Embedder](build, spec, "a service.llm resource that supports embeddings")
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	inputFact, err := requiredString(spec.Config, "input_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	model := configString(spec.Config, "model", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, found := resolvePath(ctx.Inputs, inputFact)
		if !found {
			return ActionResult{}, invalidInput("no input at %q", inputFact)
		}
		inputs := stringSlice(value)
		if len(inputs) == 0 {
			if text := Stringify(value); text != "" {
				inputs = []string{text}
			}
		}
		if len(inputs) == 0 {
			return ActionResult{}, invalidInput("nothing to embed at %q", inputFact)
		}
		vectors, err := embedder.Embed(ctx.Context, model, inputs)
		if err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, vectors), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

var searchIndexAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	index, err := requireResource[spi.SearchIndex](build, spec, "a search resource")
	if err != nil {
		return nil, err
	}
	collection, err := requiredString(spec.Config, "collection")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	documentsFact, err := requiredString(spec.Config, "documents_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	idField := configString(spec.Config, "id_field", "id")
	fields := configStrings(spec.Config, "fields")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, found := resolvePath(ctx.Inputs, documentsFact)
		if !found {
			return ActionResult{}, invalidInput("no documents at %q", documentsFact)
		}
		records := []any{value}
		if items, err := requiredList(value, documentsFact); err == nil && items != nil {
			records = items
		}
		docs := make([]spi.SearchDoc, 0, len(records))
		for _, record := range records {
			object, ok := record.(map[string]any)
			if !ok {
				return ActionResult{}, invalidInput("each document must be an object")
			}
			id := Stringify(object[idField])
			if id == "" {
				return ActionResult{}, invalidInput("a document has no %s to index it by", idField)
			}
			indexed := object
			if len(fields) > 0 {
				indexed = make(map[string]any, len(fields))
				for _, field := range fields {
					if item, present := object[field]; present {
						indexed[field] = item
					}
				}
			}
			docs = append(docs, spi.SearchDoc{ID: id, Fields: indexed, Payload: object, Tenant: ctx.TenantID})
		}
		if err := index.Index(ctx.Context, collection, docs); err != nil {
			return ActionResult{}, unavailable("could not index the documents: %v", err)
		}
		return acknowledgement(spec, map[string]any{"indexed": len(docs)}), nil
	}), nil
})

var searchQueryAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	index, err := requireResource[spi.SearchIndex](build, spec, "a search resource")
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	collection, err := requiredString(spec.Config, "collection")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	text, err := configTemplate(spec.Config, "text", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	textFact := configString(spec.Config, "text_fact", "")
	limit, err := configInt(spec.Config, "limit", 20)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	filters := configMap(spec.Config, "filters")
	offsetFact := configString(spec.Config, "offset_fact", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		query := spi.SearchQuery{Limit: limit, Filters: filters, Tenant: ctx.TenantID}
		if textFact != "" {
			if value, found := resolvePath(ctx.Inputs, textFact); found {
				query.Text = Stringify(value)
			}
		}
		if query.Text == "" && text != nil {
			rendered, err := text.Render(actionEnv(ctx))
			if err != nil {
				return ActionResult{}, err
			}
			query.Text = rendered
		}
		if offsetFact != "" {
			if value, found := resolvePath(ctx.Inputs, offsetFact); found {
				if offset, ok := ToFloat(value); ok && offset > 0 {
					query.Offset = int(offset)
				}
			}
		}
		result, err := index.Search(ctx.Context, collection, query)
		if err != nil {
			return ActionResult{}, unavailable("the search failed: %v", err)
		}
		return singleOutput(spec, map[string]any{"hits": result.Hits, "total": result.Total}), nil
	}), nil
})

var searchDeleteAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	index, err := requireResource[spi.SearchIndex](build, spec, "a search resource")
	if err != nil {
		return nil, err
	}
	collection, err := requiredString(spec.Config, "collection")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	idsFact, err := requiredString(spec.Config, "ids_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, found := resolvePath(ctx.Inputs, idsFact)
		if !found {
			return acknowledgement(spec, map[string]any{"deleted": 0}), nil
		}
		ids := stringSlice(value)
		if len(ids) == 0 {
			if text := Stringify(value); text != "" {
				ids = []string{text}
			}
		}
		if err := index.Delete(ctx.Context, collection, ids); err != nil {
			return ActionResult{}, unavailable("could not remove the documents: %v", err)
		}
		return acknowledgement(spec, map[string]any{"deleted": len(ids)}), nil
	}), nil
})
