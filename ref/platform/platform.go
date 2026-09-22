package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/bcl"
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/process"
	"github.com/oarkflow/fh/ref/runtime"
)

// The compiler.
//
// One BCL document becomes one immutable generation: every connection opened,
// every intent compiled into a proven REF plan, every process definition validated,
// every route mounted, every worker bound. Nothing is lazy, and that is the point —
// a misconfiguration is a deployment that refuses to start rather than a request
// that fails at 3am.
//
// The compile order is a dependency order, and each stage can only fail loudly:
//
//  1. Secrets     — resolve first, so everything else can reference them
//  2. Schemas     — compiled and cross-resolved
//  3. Roles       — flattened, cycles rejected
//  4. Resources   — opened in dependency order (a cache.sql before the store that uses it)
//  5. Intents     — compiled into REF plans; producers proven, cycles rejected
//  6. Engine      — plans built, speculation invariants enforced
//  7. Processes   — definitions validated against the intents that exist
//  8. Routes      — validated against intents, processes and resources
//  9. Workers     — bound and started
//  10. Schedules and triggers

// LoadOptions controls the BCL trust boundary.
type LoadOptions struct {
	Registry *Registry
	// Profile selects a BCL profile, for per-environment overrides.
	Profile string
	// AllowEnv permits env() and env.required() in the document. It defaults to
	// enabled for application-owned configuration, and should be disabled for a
	// document authored by somebody who should not read the process environment.
	AllowEnv bool
	Env      func(string) (string, bool)
	// ResolveImports permits the document to include other files.
	ResolveImports bool
	// Strict rejects unknown blocks and fields.
	Strict bool
	// ReplicaID identifies this process in process leases. Defaults to a random id.
	ReplicaID string
	// Observers are attached to the REF engine.
	Observers []observerFunc
}

type observerFunc any

// DefaultLoadOptions returns production-oriented defaults for trusted application
// configuration. Neither network access nor command execution is enabled by this
// compiler at all.
func DefaultLoadOptions() LoadOptions {
	return LoadOptions{
		Registry:       NewRegistry(),
		AllowEnv:       true,
		Env:            os.LookupEnv,
		ResolveImports: true,
		Strict:         true,
	}
}

// Platform is one immutable application generation.
type Platform struct {
	Document Document
	Engine   *runtime.Engine

	registry  *Registry
	resources map[string]Resource
	schemas   map[string]*CompiledSchema
	secrets   map[string]string
	closers   []io.Closer

	routes    []compiledRoute
	workers   []WorkerSpec
	schedules []compiledSchedule
	triggers  []compiledTrigger

	// engines holds one process engine per store, and processes maps each process
	// name to the engine that runs it.
	engines   map[string]*process.Engine
	processes map[string]*process.Engine
	// stepAuthz holds the gates declared on process steps, enforced by the step
	// runner rather than at compile time.
	stepAuthz map[string]*AuthzSpec

	replicaID string

	// intentIdempotent records which intents declared themselves replay-safe, so a
	// route's idempotency guard can refuse to short-circuit one that did not.
	intentIdempotent map[string]bool
	// advanceRegistered tracks which queues already carry the process engine's own
	// advance handler, so two processes sharing a queue register it once.
	advanceRegistered map[string]bool

	background context.CancelFunc
	wg         sync.WaitGroup
	closeOnce  sync.Once
	closeErr   error
}

type sessionContextKey struct{}

func sessionFromContext(ctx context.Context) (*session.Session, bool) {
	value, ok := ctx.Value(sessionContextKey{}).(*session.Session)
	return value, ok && value != nil
}

// workerQueue is the queue shape a managed worker needs.
type workerQueue interface {
	Enqueue(string, any, ...map[string]string) (string, error)
	Register(string, fh.QueueHandler)
	Start() error
}

// LoadFile parses, validates and compiles a BCL application file.
func LoadFile(ctx context.Context, path string, opts LoadOptions) (*Platform, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Compile(ctx, src, filepath.Dir(path), opts)
}

// Compile builds an immutable REF generation from BCL source.
func Compile(ctx context.Context, src []byte, baseDir string, opts LoadOptions) (*Platform, error) {
	if opts.Registry == nil {
		opts.Registry = NewRegistry()
	}
	if opts.Env == nil {
		opts.Env = os.LookupEnv
	}

	var doc Document
	if err := bcl.UnmarshalWithOptions(src, &doc, &bcl.Options{
		Profile:        opts.Profile,
		BaseDir:        baseDir,
		AllowEnv:       opts.AllowEnv,
		Env:            opts.Env,
		ResolveImports: opts.ResolveImports,
		Strict:         opts.Strict,
	}); err != nil {
		return nil, fmt.Errorf("ref/platform: compile BCL: %w", err)
	}

	p := &Platform{
		Engine:            runtime.NewEngine(),
		registry:          opts.Registry,
		resources:         make(map[string]Resource, len(doc.Resources)),
		schemas:           map[string]*CompiledSchema{},
		secrets:           map[string]string{},
		engines:           map[string]*process.Engine{},
		processes:         map[string]*process.Engine{},
		stepAuthz:         map[string]*AuthzSpec{},
		intentIdempotent:  map[string]bool{},
		advanceRegistered: map[string]bool{},
		replicaID:         opts.ReplicaID,
	}
	if p.replicaID == "" {
		p.replicaID = "replica-" + newPrefixedID("")
	}

	failed := true
	defer func() {
		if failed {
			// A partial generation must not leak connections, consumers or
			// goroutines. Closing in reverse order undoes exactly what was built.
			_ = p.Close()
		}
	}()

	if err := p.resolveSecrets(doc, opts); err != nil {
		return nil, err
	}
	var err error
	if p.schemas, err = compileSchemas(doc.Shapes); err != nil {
		return nil, err
	}
	if err := validateRoles(doc.Roles); err != nil {
		return nil, err
	}
	if err := validateDocument(doc, opts.Registry); err != nil {
		return nil, err
	}
	// The model is published here, before anything compiles against it, because a
	// composite node (flow.branch, a process step) has to resolve a cross-reference
	// to another intent while it is being built. Publishing it at the end would
	// leave every such lookup reading an empty document.
	p.Document = publicDocument(doc)
	if err := p.openResources(ctx, doc, opts.Registry); err != nil {
		return nil, err
	}

	// Processes compile before intents, and intents compile against them. The
	// dependency runs both ways — a step names an intent, and a node may start or
	// signal a process — so one of the two has to be resolvable before it is
	// built. Processes are the right side to go first: a step validates its
	// intent by *name* against the document, whereas a process.start node needs a
	// live engine. Nothing here dispatches: the step runner resolves its intent
	// through the engine at run time.
	if err := p.compileProcesses(doc); err != nil {
		return nil, err
	}
	for _, spec := range doc.Intents {
		if err := p.compileIntent(opts.Registry, doc, spec); err != nil {
			return nil, err
		}
		p.intentIdempotent[spec.Name] = spec.Idempotent
	}
	if err := p.Engine.Compile(); err != nil {
		return nil, fmt.Errorf("ref/platform: %w", err)
	}
	if err := p.compileRoutes(doc); err != nil {
		return nil, err
	}
	if err := p.startWorkers(doc); err != nil {
		return nil, err
	}
	if err := p.compileSchedules(doc); err != nil {
		return nil, err
	}
	if err := p.compileTriggers(doc); err != nil {
		return nil, err
	}
	p.startBackground()

	failed = false
	return p, nil
}

// buildContext is what action factories see.
func (p *Platform) buildContext() BuildContext {
	return BuildContext{
		Resources: p.resources,
		Schemas:   p.schemas,
		Platform:  p,
		Registry:  p.registry,
		Document:  &p.Document,
	}
}

// Registry returns the trust boundary this generation compiled against, so a host
// can serve its catalog.
func (p *Platform) Registry() *Registry { return p.registry }

// Resource returns an opened resource by name, for host code that needs one.
func (p *Platform) Resource(name string) (Resource, bool) {
	value, ok := p.resources[name]
	return value, ok
}

// ProcessEngine returns the engine running a named process.
func (p *Platform) ProcessEngine(name string) (*process.Engine, bool) {
	engine, ok := p.processes[name]
	return engine, ok
}

// ProcessEngines returns every distinct engine, for a host driving Tick itself.
func (p *Platform) ProcessEngines() []*process.Engine {
	engines := make([]*process.Engine, 0, len(p.engines))
	for _, engine := range p.engines {
		engines = append(engines, engine)
	}
	return engines
}

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

// resolveSecrets reads every secret before anything else compiles.
//
// A required secret that cannot be resolved fails the deployment. That is the whole
// value of declaring them: the alternative is a service that starts, serves, and
// fails on the first request that needed the credential.
func (p *Platform) resolveSecrets(doc Document, opts LoadOptions) error {
	for _, spec := range doc.Secrets {
		if spec.Name == "" {
			return fmt.Errorf("ref/platform: every secret needs a name")
		}
		if _, exists := p.secrets[spec.Name]; exists {
			return fmt.Errorf("ref/platform: duplicate secret %q", spec.Name)
		}
		value := spec.Value
		if value == "" && spec.Env != "" {
			if opts.Env == nil {
				return fmt.Errorf("ref/platform: secret %q reads the environment, which this load disallows", spec.Name)
			}
			if resolved, found := opts.Env(spec.Env); found {
				value = resolved
			}
		}
		if value == "" && spec.File != "" {
			data, err := os.ReadFile(spec.File)
			if err != nil {
				if spec.Required {
					return fmt.Errorf("ref/platform: secret %q: %w", spec.Name, err)
				}
			} else {
				value = strings.TrimRight(string(data), "\r\n")
			}
		}
		if value == "" && spec.Required {
			return fmt.Errorf("ref/platform: secret %q is required but could not be resolved (env %q, file %q)",
				spec.Name, spec.Env, spec.File)
		}
		p.secrets[spec.Name] = value
	}
	return nil
}

// Secret returns a resolved secret. Host code uses it; nothing serialises it.
func (p *Platform) Secret(name string) (string, bool) {
	value, ok := p.secrets[name]
	return value, ok && value != ""
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

// resourceDependencyKeys are the config keys that name another resource. The
// compiler reads them to order resource opening, so an author never has to think
// about declaration order.
var resourceDependencyKeys = []string{
	"database", "cache", "queue", "store", "lock", "rate_limit", "limiter",
	"authorizer", "mailer", "index", "outbox", "session", "circuit_breaker", "service",
}

// openResources opens every resource in dependency order.
func (p *Platform) openResources(ctx context.Context, doc Document, registry *Registry) error {
	order, err := orderResources(doc.Resources)
	if err != nil {
		return err
	}
	byName := make(map[string]ResourceSpec, len(doc.Resources))
	for _, spec := range doc.Resources {
		byName[spec.Name] = spec
	}

	for _, name := range order {
		spec := byName[name]
		factory, ok := registry.resource(spec.Kind)
		if !ok {
			return fmt.Errorf("ref/platform: resource %q uses unregistered kind %q. Registered kinds: %s",
				spec.Name, spec.Kind, strings.Join(registry.ResourceKinds(), ", "))
		}
		// Dependencies are handed to the provider already open, so it never reaches
		// back into the platform.
		spec.resolved = p.resources
		// The document's roles are injected into an authz resource's config so one
		// role set serves every authorizer rather than being duplicated.
		if strings.HasPrefix(spec.Kind, "authz.") {
			config := make(map[string]any, len(spec.Config)+1)
			maps.Copy(config, spec.Config)
			config["roles"] = doc.Roles
			spec.Config = config
		}

		resource, closer, err := factory.Open(ctx, spec)
		if err != nil {
			return fmt.Errorf("ref/platform: open resource %q (%s): %w", spec.Name, spec.Kind, err)
		}
		p.resources[spec.Name] = resource
		if closer != nil {
			p.closers = append(p.closers, closer)
		}
	}
	return nil
}

// orderResources topologically sorts resources by their inferred dependencies.
func orderResources(specs []ResourceSpec) ([]string, error) {
	dependencies := make(map[string][]string, len(specs))
	declared := make(map[string]bool, len(specs))
	for _, spec := range specs {
		declared[spec.Name] = true
	}
	for _, spec := range specs {
		var deps []string
		for _, key := range resourceDependencyKeys {
			if name := configString(spec.Config, key, ""); name != "" && declared[name] && name != spec.Name {
				deps = append(deps, name)
			}
		}
		// A list-valued reference, such as auth.chain's authenticators.
		for _, name := range configStrings(spec.Config, "authenticators") {
			if declared[name] && name != spec.Name {
				deps = append(deps, name)
			}
		}
		for _, name := range spec.DependsOn {
			if !declared[name] {
				return nil, fmt.Errorf("ref/platform: resource %q depends on undeclared resource %q", spec.Name, name)
			}
			deps = append(deps, name)
		}
		dependencies[spec.Name] = deps
	}

	var (
		order    []string
		visiting = map[string]bool{}
		visited  = map[string]bool{}
		visit    func(string, []string) error
	)
	visit = func(name string, path []string) error {
		if visited[name] {
			return nil
		}
		if visiting[name] {
			return fmt.Errorf("ref/platform: resources form a dependency cycle: %s -> %s",
				strings.Join(path, " -> "), name)
		}
		visiting[name] = true
		for _, dep := range dependencies[name] {
			if err := visit(dep, append(path, name)); err != nil {
				return err
			}
		}
		delete(visiting, name)
		visited[name] = true
		order = append(order, name)
		return nil
	}
	// Declaration order among independent resources, so the sequence is stable and
	// a failure message is reproducible.
	for _, spec := range specs {
		if err := visit(spec.Name, nil); err != nil {
			return nil, err
		}
	}
	return order, nil
}

// ---------------------------------------------------------------------------
// Intents
// ---------------------------------------------------------------------------

// compileIntent turns one intent block into a registered REF definition.
func (p *Platform) compileIntent(registry *Registry, doc Document, spec IntentSpec) error {
	prefix := "platform." + spec.Name + "."
	keys := make(map[string]fact.Key[any])
	key := func(name string) fact.Key[any] {
		if existing, ok := keys[name]; ok {
			return existing
		}
		created := fact.NewKey[any](prefix + name)
		keys[name] = created
		return created
	}
	inputKey := key("input")
	build := p.buildContext()

	var inputSchema, outputSchema *CompiledSchema
	if spec.InputSchema != "" {
		schema, ok := p.schemas[spec.InputSchema]
		if !ok {
			return fmt.Errorf("ref/platform: intent %q input_schema %q is not declared", spec.Name, spec.InputSchema)
		}
		inputSchema = schema
	}
	if spec.OutputSchema != "" {
		schema, ok := p.schemas[spec.OutputSchema]
		if !ok {
			return fmt.Errorf("ref/platform: intent %q output_schema %q is not declared", spec.Name, spec.OutputSchema)
		}
		outputSchema = schema
	}
	inputPipeline, err := compileDataSpec("intent "+spec.Name+" input_data", spec.InputData, p.schemas)
	if err != nil {
		return err
	}
	outputPipeline, err := compileDataSpec("intent "+spec.Name+" output_data", spec.OutputData, p.schemas)
	if err != nil {
		return err
	}

	// An intent-level authz gate becomes a decision node in the plan, so it
	// participates in the deny-dominant algebra and blocks every effect rather than
	// only the nodes downstream of it.
	if spec.Authz != nil {
		gate, err := compileAuthz(build, "intent "+spec.Name, spec.Authz)
		if err != nil {
			return err
		}
		gateKey := key("__authz")
		reg := capability.Registration{
			Name:     prefix + "__authz",
			Provides: []fact.AnyKey{gateKey.Any()},
			Kind:     graph.DecisionNode,
		}
		reg.Run = func(nc *execution.NodeContext) error {
			actionCtx := p.actionContext(nc, nil, nil, nil)
			allowed, message, err := gate.evaluate(actionCtx, actionEnv(actionCtx))
			if err != nil {
				nc.Decisions().RecordDeny(reg.Name, "authorization check failed")
				return err
			}
			if !allowed {
				nc.Decisions().RecordDeny(reg.Name, message)
				return permissionDenied(message)
			}
			nc.Decisions().RecordAllow(reg.Name, nil)
			execution.Publish(nc, gateKey, true)
			return nil
		}
		if err := p.Engine.Capabilities().Register(reg); err != nil {
			return err
		}
		// The response node is made to depend on the gate so the gate is part of the
		// plan rather than an orphan the compiler would prune.
		spec.Nodes = withGateDependency(spec.Nodes, spec.Response, "__authz")
	}

	for _, nodeSpec := range spec.Nodes {
		if err := p.compileNode(registry, build, spec, nodeSpec, prefix, keys, key); err != nil {
			return err
		}
	}

	intentTimeout, err := spec.Timeout.Parse("intent "+spec.Name+" timeout", 0)
	if err != nil {
		return err
	}
	responseKey := key(spec.Response)
	definition := &intent.Definition{
		Name:     intent.Name(spec.Name),
		Version:  1,
		InputKey: inputKey.Any(),
		Spec: intent.Spec{
			Description:   spec.Description,
			Requires:      []fact.AnyKey{responseKey.Any()},
			Timeout:       intentTimeout,
			MaxDBQueries:  spec.MaxDBQueries,
			MaxExternalIO: spec.MaxExternalIO,
			MaxMemory:     spec.MaxMemory,
			MaxEffects:    spec.MaxEffects,
		},
		DecodeNode: func(nc *execution.NodeContext) error {
			var value any
			raw := nc.Invocation().Input.RawBytes()
			if len(raw) == 0 {
				value = map[string]any{}
			} else if err := json.Unmarshal(raw, &value); err != nil {
				return intent.Failure{Code: "INVALID_INPUT", Category: intent.CategoryInvalidInput,
					Message: "the request body is not valid JSON", Cause: err}
			}
			if inputPipeline != nil {
				shaped, err := inputPipeline.Apply(value, Env{"input": value})
				if err != nil {
					if errors.Is(err, ErrDataFiltered) {
						return invalidInput("the request was rejected by this intent's input filter")
					}
					return err
				}
				value = shaped
			}
			if inputSchema != nil {
				validated, err := inputSchema.Validate(value)
				if err != nil {
					return err
				}
				value = validated
			}
			execution.Publish(nc, inputKey, value)
			return nil
		},
		Run: func(nc *execution.NodeContext) (any, effect.EffectPlan, intent.OutcomeMeta, error) {
			value, err := execution.Require(nc, responseKey)
			if err != nil {
				return nil, effect.EffectPlan{}, intent.OutcomeMeta{}, err
			}
			if outputPipeline != nil {
				shaped, shapeErr := outputPipeline.Apply(value, Env{"result": value})
				if shapeErr != nil && !errors.Is(shapeErr, ErrDataFiltered) {
					return nil, effect.EffectPlan{}, intent.OutcomeMeta{}, shapeErr
				}
				if shapeErr == nil {
					value = shaped
				}
			}
			if outputSchema != nil {
				if _, err := outputSchema.Validate(value); err != nil {
					// An output that does not match its declared schema is this
					// application's bug, not the caller's — so it reports as internal
					// rather than as invalid input.
					return nil, effect.EffectPlan{}, intent.OutcomeMeta{}, fmt.Errorf(
						"intent %q produced a response that does not match its output_schema: %w", spec.Name, err)
				}
			}
			return value, effect.EffectPlan{}, intent.OutcomeMeta{}, nil
		},
	}
	return p.Engine.RegisterDefinition(definition)
}

// withGateDependency makes the response-producing node depend on a synthetic fact,
// so a gate node cannot be pruned from the plan.
func withGateDependency(nodes []NodeSpec, response, gate string) []NodeSpec {
	out := slices.Clone(nodes)
	for i := range out {
		if slices.Contains(out[i].Provides, response) {
			out[i].Requires = append(slices.Clone(out[i].Requires), gate)
			return out
		}
	}
	return out
}

// compileNode compiles one node into a REF capability.
func (p *Platform) compileNode(registry *Registry, build BuildContext, spec IntentSpec, nodeSpec NodeSpec, prefix string, keys map[string]fact.Key[any], key func(string) fact.Key[any]) error {
	what := fmt.Sprintf("intent %q node %q", spec.Name, nodeSpec.Name)

	factory, ok := registry.action(nodeSpec.Uses)
	if !ok {
		return fmt.Errorf("ref/platform: %s uses unregistered action %q. Registered actions: %s",
			what, nodeSpec.Uses, strings.Join(registry.ActionNames(), ", "))
	}
	var resource Resource
	if nodeSpec.Resource != "" {
		resource, ok = p.resources[nodeSpec.Resource]
		if !ok {
			return fmt.Errorf("ref/platform: %s references undeclared resource %q", what, nodeSpec.Resource)
		}
	}
	action, err := factory.Build(build, nodeSpec)
	if err != nil {
		return fmt.Errorf("ref/platform: build %s: %w", what, err)
	}

	inputPipeline, err := compileDataSpec(what+" input_data", nodeSpec.InputData, p.schemas)
	if err != nil {
		return err
	}
	outputPipeline, err := compileDataSpec(what+" output_data", nodeSpec.OutputData, p.schemas)
	if err != nil {
		return err
	}
	retry, err := compileRetrySpec(nodeSpec.Retry)
	if err != nil {
		return fmt.Errorf("ref/platform: %s retry: %w", what, err)
	}
	gate, err := compileAuthz(build, what, nodeSpec.Authz)
	if err != nil {
		return err
	}
	nodeTimeout, err := nodeSpec.Timeout.Parse(what+" timeout", 0)
	if err != nil {
		return err
	}

	onError := strings.ToLower(strings.TrimSpace(nodeSpec.OnError))
	switch onError {
	case "", "fail":
		onError = "fail"
	case "continue", "fallback":
		onError = "continue"
		// A node that continues past a failure must still satisfy its consumers, or
		// the next node fails on a missing fact and the "continue" bought nothing.
		for _, name := range nodeSpec.Provides {
			if _, ok := nodeSpec.Fallback[name]; !ok {
				return fmt.Errorf("ref/platform: %s has on_error %q, so it needs a fallback value for every fact it provides — %q has none",
					what, nodeSpec.OnError, name)
			}
		}
	default:
		return fmt.Errorf("ref/platform: %s has unknown on_error %q (use fail, continue or fallback)", what, nodeSpec.OnError)
	}

	requires := make([]fact.AnyKey, 0, len(nodeSpec.Requires))
	for _, name := range nodeSpec.Requires {
		requires = append(requires, key(name).Any())
	}
	provides := make([]fact.AnyKey, 0, len(nodeSpec.Provides))
	for _, name := range nodeSpec.Provides {
		provides = append(provides, key(name).Any())
	}
	kind, err := nodeKind(nodeSpec.Kind)
	if err != nil {
		return fmt.Errorf("ref/platform: %s: %w", what, err)
	}
	speculation, err := speculationClass(nodeSpec.Speculation)
	if err != nil {
		return fmt.Errorf("ref/platform: %s: %w", what, err)
	}

	node := nodeSpec
	reg := capability.Registration{
		Name:        prefix + nodeSpec.Name,
		Requires:    requires,
		Provides:    provides,
		Kind:        kind,
		Speculation: speculation,
	}
	reg.Run = func(nc *execution.NodeContext) error {
		inputs := make(map[string]any, len(node.Requires))
		for _, name := range node.Requires {
			value, err := execution.Require(nc, keys[name])
			if err != nil {
				return fmt.Errorf("node %q needs fact %q, which was not published: %w", node.Name, name, err)
			}
			inputs[name] = value
		}
		actionCtx := p.actionContext(nc, inputs, node.Config, resource)

		if inputPipeline != nil {
			shaped, err := inputPipeline.Apply(inputs, actionEnv(actionCtx))
			if err != nil {
				return err
			}
			if object, ok := shaped.(map[string]any); ok {
				actionCtx.Inputs = object
			}
		}
		if gate != nil {
			allowed, message, err := gate.evaluate(actionCtx, actionEnv(actionCtx))
			if err != nil {
				nc.Decisions().RecordDeny(reg.Name, "authorization check failed")
				return err
			}
			if !allowed {
				if strings.EqualFold(node.Authz.OnDeny, "skip") {
					return publishFallback(nc, node, keys)
				}
				nc.Decisions().RecordDeny(reg.Name, message)
				return permissionDenied(message)
			}
			nc.Decisions().RecordAllow(reg.Name, nil)
		}

		result, err := p.runNodeAction(actionCtx, action, node, retry, nodeTimeout)
		if err != nil {
			if onError == "continue" {
				return publishFallback(nc, node, keys)
			}
			return err
		}

		if result.Decision != nil {
			if result.Decision.Allow {
				nc.Decisions().RecordAllow(reg.Name, result.Decision.Constraints, result.Decision.Obligations...)
			} else {
				nc.Decisions().RecordDeny(reg.Name, result.Decision.Message)
			}
		}
		for name, value := range result.Outputs {
			factKey, declared := keys[name]
			if !declared || !slices.Contains(node.Provides, name) {
				return fmt.Errorf("node %q published fact %q, which it does not declare in provides", node.Name, name)
			}
			if outputPipeline != nil {
				shaped, shapeErr := outputPipeline.Apply(value, actionEnv(actionCtx))
				if shapeErr != nil && !errors.Is(shapeErr, ErrDataFiltered) {
					return shapeErr
				}
				if shapeErr == nil {
					value = shaped
				}
			}
			execution.Publish(nc, factKey, value)
		}
		for _, fx := range result.Effects {
			nc.RecordEffect(fx)
		}
		return nil
	}
	return p.Engine.Capabilities().Register(reg)
}

// runNodeAction executes an action under its timeout and retry policy.
func (p *Platform) runNodeAction(ctx *ActionContext, action Action, node NodeSpec, retry *retryPolicy, timeout time.Duration) (ActionResult, error) {
	run := func(runCtx context.Context) (ActionResult, error) {
		scoped := *ctx
		scoped.Context = runCtx
		return action.Run(&scoped)
	}

	attempt := func() (ActionResult, error) {
		if timeout <= 0 {
			return run(ctx.Context)
		}
		timed, cancel := context.WithTimeout(ctx.Context, timeout)
		defer cancel()
		result, err := run(timed)
		if err != nil && errors.Is(timed.Err(), context.DeadlineExceeded) {
			return result, intent.Failure{Code: "TIMEOUT", Category: intent.CategoryTimeout,
				Message: fmt.Sprintf("%s did not finish within %s", node.Name, timeout), Cause: err}
		}
		return result, err
	}

	if retry == nil {
		return attempt()
	}
	var lastErr error
	for tries := 1; tries <= retry.attempts; tries++ {
		result, err := attempt()
		if err == nil {
			return result, nil
		}
		lastErr = err
		if tries == retry.attempts || !retry.shouldRetry(err) {
			break
		}
		select {
		case <-ctx.Context.Done():
			return ActionResult{}, ctx.Context.Err()
		case <-time.After(retry.delay(tries)):
		}
	}
	return ActionResult{}, lastErr
}

// publishFallback publishes a node's declared fallback values, for a node that
// continues past a failure or a skipped authorization gate.
func publishFallback(nc *execution.NodeContext, node NodeSpec, keys map[string]fact.Key[any]) error {
	for _, name := range node.Provides {
		factKey, ok := keys[name]
		if !ok {
			continue
		}
		execution.Publish(nc, factKey, node.Fallback[name])
	}
	return nil
}

// actionContext assembles the per-invocation context an action sees.
func (p *Platform) actionContext(nc *execution.NodeContext, inputs, config map[string]any, resource Resource) *ActionContext {
	// The pooled NodeContext must not escape as a context.Context: database/sql and
	// net/http may retain a context briefly after the call returns, and the pooled
	// one is cleared and reused the moment the node finishes.
	ctx := &ActionContext{
		Context:    nc.Context,
		Invocation: nc.Invocation(),
		Inputs:     inputs,
		Config:     config,
		Resource:   resource,
		Node:       nc,
		Platform:   p,
		Depth:      flowDepth(nc.Context),
		Now:        time.Now().UTC(),
	}
	if identity, ok := requestIdentityFrom(nc.Context); ok {
		ctx.Principal, ctx.TenantID = identity.principal, identity.tenant
	} else if identity, ok := processIdentityFrom(nc.Context); ok {
		ctx.Principal = Principal{ID: identity.principal, TenantID: identity.tenant}
		ctx.TenantID = identity.tenant
	}
	return ctx
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

func validateRoles(roles []RoleSpec) error {
	seen := make(map[string]bool, len(roles))
	for _, role := range roles {
		if role.Name == "" {
			return fmt.Errorf("ref/platform: every role needs a name")
		}
		if seen[role.Name] {
			return fmt.Errorf("ref/platform: duplicate role %q", role.Name)
		}
		seen[role.Name] = true
	}
	for _, role := range roles {
		for _, parent := range role.Inherits {
			if !seen[parent] {
				return fmt.Errorf("ref/platform: role %q inherits undeclared role %q", role.Name, parent)
			}
		}
	}
	return nil
}

// validateDocument checks everything that can be checked before anything is opened,
// so the cheapest failures happen first.
func validateDocument(doc Document, registry *Registry) error {
	if strings.TrimSpace(doc.Name) == "" {
		return fmt.Errorf("ref/platform: the application needs a name")
	}

	resources := map[string]bool{}
	for _, item := range doc.Resources {
		if item.Name == "" || item.Kind == "" {
			return fmt.Errorf("ref/platform: every resource needs a name and a kind")
		}
		if resources[item.Name] {
			return fmt.Errorf("ref/platform: duplicate resource %q", item.Name)
		}
		resources[item.Name] = true
	}

	intents := map[string]bool{}
	for _, item := range doc.Intents {
		if item.Name == "" {
			return fmt.Errorf("ref/platform: every intent needs a name")
		}
		if item.Response == "" {
			return fmt.Errorf("ref/platform: intent %q needs a response fact", item.Name)
		}
		if intents[item.Name] {
			return fmt.Errorf("ref/platform: duplicate intent %q", item.Name)
		}
		intents[item.Name] = true

		provided := map[string]string{"input": "the request decoder"}
		for index, node := range item.Nodes {
			if node.Name == "" || node.Uses == "" {
				return fmt.Errorf("ref/platform: intent %q node[%d] needs a name and a uses (name=%q uses=%q)",
					item.Name, index, node.Name, node.Uses)
			}
			if node.Family != "" {
				info, known := registry.nodeType(node.Family)
				if !known {
					return fmt.Errorf("ref/platform: intent %q node %q has unregistered family %q. Registered families: see the catalog", item.Name, node.Name, node.Family)
				}
				// A durable node type in a request intent cannot do what its name
				// promises: a request cannot wait two days for an approval.
				if info.Durable {
					return fmt.Errorf("ref/platform: intent %q node %q has family %q, which parks and waits. A request cannot do that — move it to a process step, and use a process.* or task.* action here if you only need to start or inspect one",
						item.Name, node.Name, node.Family)
				}
			}
			if node.Resource != "" && !resources[node.Resource] {
				return fmt.Errorf("ref/platform: intent %q node %q references undeclared resource %q", item.Name, node.Name, node.Resource)
			}
			for _, name := range node.Provides {
				if owner := provided[name]; owner != "" {
					return fmt.Errorf("ref/platform: intent %q fact %q is provided by both %q and %q — a fact needs exactly one producer",
						item.Name, name, owner, node.Name)
				}
				provided[name] = node.Name
			}
		}
		if provided[item.Response] == "" {
			return fmt.Errorf("ref/platform: intent %q names %q as its response, but no node provides it", item.Name, item.Response)
		}
		// A required fact nobody produces is a compile error in REF itself, but the
		// message here names the node and the intent, which is more useful.
		for _, node := range item.Nodes {
			for _, name := range node.Requires {
				if provided[name] == "" {
					return fmt.Errorf("ref/platform: intent %q node %q requires fact %q, which no node provides",
						item.Name, node.Name, name)
				}
			}
		}
	}

	processes := map[string]bool{}
	for _, item := range doc.Processes {
		if item.Name == "" {
			return fmt.Errorf("ref/platform: every process needs a name")
		}
		if processes[item.Name] {
			return fmt.Errorf("ref/platform: duplicate process %q", item.Name)
		}
		processes[item.Name] = true
		if item.Store != "" && !resources[item.Store] {
			return fmt.Errorf("ref/platform: process %q references undeclared store %q", item.Name, item.Store)
		}
		if item.Queue != "" && !resources[item.Queue] {
			return fmt.Errorf("ref/platform: process %q references undeclared queue %q", item.Name, item.Queue)
		}
		// A process that parks needs a queue, or nothing will ever wake it.
		if item.Queue == "" && processParks(item) {
			return fmt.Errorf("ref/platform: process %q parks on a timer, an event or a task, so it needs a queue to schedule its own wake-ups", item.Name)
		}
	}

	if err := validateRouteSpecs(doc, resources, intents, processes); err != nil {
		return err
	}
	if err := validateWorkerSpecs(doc, resources, intents, processes); err != nil {
		return err
	}
	return validateScheduleSpecs(doc, resources, intents, processes)
}

// processParks reports whether a process has any step or edge that suspends.
func processParks(spec ProcessSpec) bool {
	for _, step := range spec.Steps {
		if step.Task != nil || step.Process != "" {
			return true
		}
	}
	for _, edge := range spec.Edges {
		if process.EdgeType(strings.ToLower(edge.Kind)).Parks() {
			return true
		}
	}
	return false
}

func validateRouteSpecs(doc Document, resources, intents, processes map[string]bool) error {
	keys := map[string]bool{}
	for _, route := range doc.Routes {
		method := strings.ToUpper(strings.TrimSpace(route.Method))
		if route.Name == "" || method == "" || route.Path == "" {
			return fmt.Errorf("ref/platform: every route needs a name, a method and a path")
		}
		if route.Intent == "" && route.Process == "" {
			return fmt.Errorf("ref/platform: route %q needs an intent or a process", route.Name)
		}
		if route.Intent != "" && route.Process != "" {
			return fmt.Errorf("ref/platform: route %q names both an intent and a process; pick one", route.Name)
		}
		if route.Intent != "" && !intents[route.Intent] {
			return fmt.Errorf("ref/platform: route %q references unknown intent %q", route.Name, route.Intent)
		}
		if route.Process != "" && !processes[route.Process] {
			return fmt.Errorf("ref/platform: route %q references unknown process %q", route.Name, route.Process)
		}

		mode := strings.ToLower(strings.TrimSpace(route.Mode))
		if !slices.Contains([]string{"", "sync", "async", "stream"}, mode) {
			return fmt.Errorf("ref/platform: route %q has unsupported mode %q (use sync, async or stream)", route.Name, route.Mode)
		}
		if mode == "async" && (route.Queue == "" || !resources[route.Queue]) {
			return fmt.Errorf("ref/platform: async route %q needs a declared queue resource", route.Name)
		}
		for _, pair := range []struct {
			what string
			name string
		}{{"session", route.Session}, {"auth", route.Auth}} {
			if pair.name != "" && !resources[pair.name] {
				return fmt.Errorf("ref/platform: route %q references undeclared %s resource %q", route.Name, pair.what, pair.name)
			}
		}
		if route.RateLimit != nil {
			if route.RateLimit.Limiter != "" && !resources[route.RateLimit.Limiter] {
				return fmt.Errorf("ref/platform: route %q rate_limit references undeclared limiter %q", route.Name, route.RateLimit.Limiter)
			}
			if window, err := route.RateLimit.Window.Parse("route "+route.Name+" rate_limit window", 0); err != nil {
				return err
			} else if route.RateLimit.Limit <= 0 || window <= 0 {
				return fmt.Errorf("ref/platform: route %q rate_limit needs a positive limit and window", route.Name)
			}
		}
		if route.Idempotency != nil {
			if route.Idempotency.Store != "" && !resources[route.Idempotency.Store] {
				return fmt.Errorf("ref/platform: route %q idempotency references undeclared store %q", route.Name, route.Idempotency.Store)
			}
			if route.Intent != "" {
				idempotent := false
				for _, item := range doc.Intents {
					if item.Name == route.Intent {
						idempotent = item.Idempotent
						break
					}
				}
				if !idempotent {
					// Replaying a stored response for work that was never redone the
					// second time reports success for something that did not happen.
					return fmt.Errorf("ref/platform: route %q has an idempotency guard but intent %q is not marked idempotent. Mark it `idempotent true` once it really is safe to replay",
						route.Name, route.Intent)
				}
			}
		}
		if route.CORS != nil && route.CORS.AllowCredentials && slices.Contains(route.CORS.AllowOrigins, "*") {
			return fmt.Errorf("ref/platform: route %q allows credentials with a wildcard origin, which browsers reject and which is not what you want", route.Name)
		}
		if route.Authz != nil && route.Authz.Empty() {
			return fmt.Errorf("ref/platform: route %q has an authz block with no rules. Give it roles, permissions, scopes or a condition, or remove it", route.Name)
		}
		if route.Authz != nil && route.Auth == "" && route.Session == "" {
			return fmt.Errorf("ref/platform: route %q has an authz block but no auth or session resource, so no identity can ever be established and every request would be denied", route.Name)
		}

		key := method + " " + route.Path
		if keys[key] {
			return fmt.Errorf("ref/platform: duplicate route %s", key)
		}
		keys[key] = true
	}
	return nil
}

func validateWorkerSpecs(doc Document, resources, intents, processes map[string]bool) error {
	seen := map[string]bool{}
	for _, worker := range doc.Workers {
		if worker.Name == "" || worker.Queue == "" || worker.JobType == "" {
			return fmt.Errorf("ref/platform: every worker needs a name, a queue and a job_type")
		}
		if seen[worker.Name] {
			return fmt.Errorf("ref/platform: duplicate worker %q", worker.Name)
		}
		seen[worker.Name] = true
		if !resources[worker.Queue] {
			return fmt.Errorf("ref/platform: worker %q references undeclared queue %q", worker.Name, worker.Queue)
		}
		if worker.Intent == "" && worker.Process == "" {
			return fmt.Errorf("ref/platform: worker %q needs an intent or a process", worker.Name)
		}
		if worker.Intent != "" && !intents[worker.Intent] {
			return fmt.Errorf("ref/platform: worker %q references unknown intent %q", worker.Name, worker.Intent)
		}
		if worker.Process != "" && !processes[worker.Process] {
			return fmt.Errorf("ref/platform: worker %q references unknown process %q", worker.Name, worker.Process)
		}
	}
	return nil
}

func validateScheduleSpecs(doc Document, resources, intents, processes map[string]bool) error {
	seen := map[string]bool{}
	for _, schedule := range doc.Schedules {
		if schedule.Name == "" {
			return fmt.Errorf("ref/platform: every schedule needs a name")
		}
		if seen[schedule.Name] {
			return fmt.Errorf("ref/platform: duplicate schedule %q", schedule.Name)
		}
		seen[schedule.Name] = true

		set := 0
		for _, configured := range []bool{schedule.Every.Set(), schedule.Cron != "", schedule.At != ""} {
			if configured {
				set++
			}
		}
		if set != 1 {
			return fmt.Errorf("ref/platform: schedule %q needs exactly one of every, cron or at", schedule.Name)
		}
		if schedule.Cron != "" {
			if _, err := parseCron(schedule.Cron); err != nil {
				return fmt.Errorf("ref/platform: schedule %q cron: %w", schedule.Name, err)
			}
		}
		if schedule.At != "" {
			if _, err := time.Parse(time.RFC3339, schedule.At); err != nil {
				return fmt.Errorf("ref/platform: schedule %q at must be an RFC3339 instant: %w", schedule.Name, err)
			}
		}
		if schedule.Timezone != "" {
			if _, err := time.LoadLocation(schedule.Timezone); err != nil {
				return fmt.Errorf("ref/platform: schedule %q timezone: %w", schedule.Name, err)
			}
		}
		if schedule.Queue == "" || !resources[schedule.Queue] {
			return fmt.Errorf("ref/platform: schedule %q needs a declared queue — a schedule runs through the queue so exactly one replica fires each occurrence", schedule.Name)
		}
		if schedule.Intent == "" && schedule.Process == "" {
			return fmt.Errorf("ref/platform: schedule %q needs an intent or a process", schedule.Name)
		}
		if schedule.Intent != "" && !intents[schedule.Intent] {
			return fmt.Errorf("ref/platform: schedule %q references unknown intent %q", schedule.Name, schedule.Intent)
		}
		if schedule.Process != "" && !processes[schedule.Process] {
			return fmt.Errorf("ref/platform: schedule %q references unknown process %q", schedule.Name, schedule.Process)
		}
	}

	for _, trigger := range doc.Triggers {
		kind := strings.ToLower(trigger.Kind)
		switch kind {
		case "webhook":
			if trigger.Path == "" {
				return fmt.Errorf("ref/platform: webhook trigger %q needs a path", trigger.Name)
			}
			if trigger.Secret == "" {
				return fmt.Errorf("ref/platform: webhook trigger %q needs a secret for signature verification — an unauthenticated public mutation endpoint is never the intent", trigger.Name)
			}
		case "event":
			if trigger.Event == "" {
				return fmt.Errorf("ref/platform: event trigger %q needs an event name", trigger.Name)
			}
		default:
			return fmt.Errorf("ref/platform: trigger %q has unsupported kind %q (use webhook or event)", trigger.Name, trigger.Kind)
		}
		if trigger.Intent != "" && !intents[trigger.Intent] {
			return fmt.Errorf("ref/platform: trigger %q references unknown intent %q", trigger.Name, trigger.Intent)
		}
		if trigger.Process != "" && !processes[trigger.Process] {
			return fmt.Errorf("ref/platform: trigger %q references unknown process %q", trigger.Name, trigger.Process)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Document redaction
// ---------------------------------------------------------------------------

// publicDocument returns the model with every credential removed, so
// Platform.Document can be served from an introspection route without leaking a
// DSN or an API key.
func publicDocument(doc Document) Document {
	result := doc
	result.Resources = slices.Clone(doc.Resources)
	for i := range result.Resources {
		result.Resources[i].Config = redactResourceConfig(doc.Resources[i].Config)
		result.Resources[i].resolved = nil
	}
	// Secret blocks describe where a value comes from, never the value.
	result.Secrets = slices.Clone(doc.Secrets)
	for i := range result.Secrets {
		result.Secrets[i].Value = ""
	}
	return result
}

// sensitiveConfigKeys are redacted outright. The suffix list catches the
// composed names a real configuration grows: payment_api_key, webhook_secret.
var sensitiveConfigKeys = []string{
	"dsn", "read_replica_dsn", "key", "secret", "token", "password", "credential",
	"api_key", "sign_secret", "private_key", "client_key_file",
}

var sensitiveConfigSuffixes = []string{
	"_dsn", "_key", "_secret", "_token", "_password", "_credential", "_passphrase",
}

func redactResourceConfig(config map[string]any) map[string]any {
	result := make(map[string]any, len(config))
	for key, value := range config {
		lower := strings.ToLower(key)
		sensitive := slices.Contains(sensitiveConfigKeys, lower)
		if !sensitive {
			for _, suffix := range sensitiveConfigSuffixes {
				if strings.HasSuffix(lower, suffix) {
					sensitive = true
					break
				}
			}
		}
		if sensitive {
			result[key] = "[REDACTED]"
			continue
		}
		// A nested block can hold credentials too — headers { Authorization … } is
		// the common case.
		if nested, ok := value.(map[string]any); ok {
			result[key] = redactResourceConfig(nested)
			continue
		}
		result[key] = value
	}
	return result
}

// ---------------------------------------------------------------------------
// Kinds
// ---------------------------------------------------------------------------

func nodeKind(value string) (graph.NodeKind, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "pure":
		return graph.PureNode, nil
	case "read":
		return graph.ReadNode, nil
	case "decision":
		return graph.DecisionNode, nil
	case "effect":
		return graph.EffectNode, nil
	case "async_effect":
		return graph.AsyncEffect, nil
	case "stream":
		return graph.StreamNode, nil
	default:
		return 0, fmt.Errorf("unknown node kind %q (use pure, read, decision, effect, async_effect or stream)", value)
	}
}

func speculationClass(value string) (graph.SpeculationClass, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "none", "no_speculation":
		return graph.NoSpeculation, nil
	case "pre_auth_safe":
		return graph.PreAuthSafe, nil
	case "post_identity_safe":
		return graph.PostIdentitySafe, nil
	case "post_policy_safe":
		return graph.PostPolicySafe, nil
	default:
		return 0, fmt.Errorf("unknown speculation class %q", value)
	}
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------

// startWorkers binds queue job types to intents, processes and the engine's own
// advance work, then starts consumption.
func (p *Platform) startWorkers(doc Document) error {
	started := map[workerQueue]bool{}

	for _, spec := range doc.Workers {
		if spec.Disabled {
			continue
		}
		queue, ok := p.resources[spec.Queue].(workerQueue)
		if !ok {
			return fmt.Errorf("ref/platform: worker %q resource %q does not support managed consumption", spec.Name, spec.Queue)
		}
		worker := spec
		queue.Register(spec.JobType, func(ctx context.Context, job *fh.QueueJob) error {
			return p.runWorkerJob(ctx, worker, job)
		})
		started[queue] = true
	}

	// The process engine's own advance work rides on the same queues, so a
	// deployment that declared a queue for its processes gets multi-replica
	// advancing without declaring a worker for it.
	for _, spec := range doc.Processes {
		if spec.Queue == "" {
			continue
		}
		queue, ok := p.resources[spec.Queue].(workerQueue)
		if !ok {
			continue
		}
		if p.advanceRegistered[spec.Queue] {
			continue
		}
		engine := p.processes[spec.Name]
		queue.Register(processAdvanceJobType, func(ctx context.Context, job *fh.QueueJob) error {
			var payload struct {
				RunID string `json:"run_id"`
			}
			if err := json.Unmarshal(job.Payload, &payload); err != nil {
				return fmt.Errorf("ref/platform: advance job has an unreadable payload: %w", err)
			}
			if payload.RunID == "" {
				return fmt.Errorf("ref/platform: advance job has no run_id")
			}
			return engine.Advance(ctx, payload.RunID)
		})
		if p.advanceRegistered == nil {
			p.advanceRegistered = map[string]bool{}
		}
		p.advanceRegistered[spec.Queue] = true
		started[queue] = true
	}

	// Schedules also consume from their queue.
	for _, spec := range doc.Schedules {
		if spec.Disabled {
			continue
		}
		queue, ok := p.resources[spec.Queue].(workerQueue)
		if !ok {
			continue
		}
		started[queue] = true
	}

	for queue := range started {
		if err := queue.Start(); err != nil {
			return fmt.Errorf("ref/platform: start queue consumption: %w", err)
		}
	}
	p.workers = doc.Workers
	return nil
}

// runWorkerJob dispatches one queue job to its intent or process.
func (p *Platform) runWorkerJob(ctx context.Context, worker WorkerSpec, job *fh.QueueJob) error {
	if worker.Process != "" {
		engine, ok := p.processes[worker.Process]
		if !ok {
			return fmt.Errorf("ref/platform: worker %q names unknown process %q", worker.Name, worker.Process)
		}
		var input any
		if len(job.Payload) > 0 {
			if err := json.Unmarshal(job.Payload, &input); err != nil {
				return fmt.Errorf("ref/platform: worker %q received an unreadable payload: %w", worker.Name, err)
			}
		}
		_, err := engine.Start(ctx, worker.Process, input, process.StartOptions{
			// The job id is the idempotency key, so a queue redelivery resumes the
			// original run instead of starting a duplicate.
			IdempotencyKey: job.ID,
			TenantID:       job.Headers["tenant_id"],
			PrincipalID:    job.Headers["principal_id"],
			Detached:       true,
		})
		return err
	}

	inv := &invocation.Invocation{
		ID:     invocation.ID(job.ID),
		Intent: invocation.IntentID(worker.Intent),
		Input:  invocation.NewInput(job.Payload, "application/json"),
		Principal: invocation.NewPrincipalHint(
			job.Headers["bearer_token"], job.Headers["api_key"], nil, job.Headers["session_id"]),
		Metadata:  invocation.NewQueueMeta(worker.JobType, 0, job.ID, job.Headers),
		Transport: invocation.Transport{Protocol: "queue"},
		Received:  time.Now(),
	}
	// A job carries the identity of whoever published it, so a tenant-scoped query
	// inside the intent sees the same tenant the request did.
	ctx = withProcessIdentity(ctx, job.Headers["principal_id"], job.Headers["tenant_id"])
	_, err := p.Engine.Dispatch(ctx, inv)
	return err
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// startBackground launches the process engine's ticker and the schedule loop.
func (p *Platform) startBackground() {
	if len(p.engines) == 0 && len(p.schedules) == 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.background = cancel

	if len(p.engines) > 0 {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.tickProcesses(ctx)
		}()
	}
	if len(p.schedules) > 0 {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.runSchedules(ctx)
		}()
	}
}

// tickProcesses fires due timers and recovers stalled runs.
//
// Every replica runs this. The store claims timers atomically, so the work is
// shared rather than duplicated, and a replica going away costs nothing.
func (p *Platform) tickProcesses(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	recovery := time.NewTicker(time.Minute)
	defer recovery.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, engine := range p.engines {
				// A tick error is not fatal: the next tick tries again, and the runs
				// keep their state meanwhile.
				_, _ = engine.Tick(ctx, 50)
			}
		case <-recovery.C:
			for _, engine := range p.engines {
				_, _ = engine.RecoverStalled(ctx, 5*time.Minute, 50)
				_, _ = engine.Purge(ctx, 100)
			}
		}
	}
}

// Close releases every resource this generation owns, in reverse order of opening.
func (p *Platform) Close() error {
	p.closeOnce.Do(func() {
		if p.background != nil {
			p.background()
		}
		p.wg.Wait()
		for i := len(p.closers) - 1; i >= 0; i-- {
			if err := p.closers[i].Close(); err != nil && p.closeErr == nil {
				p.closeErr = err
			}
		}
	})
	return p.closeErr
}
