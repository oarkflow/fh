// Package platform compiles BCL application definitions into immutable REF
// execution generations.
//
// The split of responsibility is the whole point of this package. Application
// authors write configuration: resources, intents, nodes, processes, steps,
// edges and routes. Trusted host code supplies the executable pieces: resource
// factories that open connections and action factories that do work. A Registry
// is the boundary between the two — BCL may select any registered capability
// and may configure it, but it can never introduce native code, reach an
// unregistered connection kind, or execute an unregistered action.
//
// The spec types live in spec_app.go, spec_intent.go, spec_process.go and
// spec_route.go. This file holds the runtime contracts those specs compile
// against.
package platform

import (
	"context"
	"io"
	"time"

	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/platform/spi"
)

// Principal is the serializable identity exposed to no-code nodes. It is an
// alias of the SPI type so an authenticator adapter written against
// ref/platform/spi alone drops straight in.
type Principal = spi.Principal

// Cache is the portable cache contract. fh's kv.Store satisfies it, as can
// Redis, PostgreSQL, DynamoDB or a remote cache adapter.
type Cache = spi.Cache

// JobQueue is the portable publication surface used by queue.publish.
type JobQueue = spi.JobQueue

// Authenticator verifies transport credentials. Implementations commonly
// validate OIDC/JWT, API keys, HTTP basic credentials, mTLS certificates,
// session cookies or signed service credentials.
type Authenticator = spi.Authenticator

// Authorizer answers permission questions about a principal.
type Authorizer = spi.Authorizer

// Credentials is what a transport extracted before identity is known.
type Credentials = spi.Credentials

// LegacyAuthenticator is the pre-SPI authenticator shape, kept so existing host
// code and the auth.api_key provider continue to work unchanged. The platform
// accepts either shape wherever an authenticator is required.
type LegacyAuthenticator interface {
	Authenticate(context.Context, invocation.PrincipalHint) (Principal, error)
}

// AuthenticatorFunc adapts a function to LegacyAuthenticator.
type AuthenticatorFunc func(context.Context, invocation.PrincipalHint) (Principal, error)

// Authenticate implements LegacyAuthenticator.
func (f AuthenticatorFunc) Authenticate(ctx context.Context, hint invocation.PrincipalHint) (Principal, error) {
	return f(ctx, hint)
}

// CredentialAuthenticatorFunc adapts a function to the SPI Authenticator.
type CredentialAuthenticatorFunc func(context.Context, Credentials) (Principal, error)

// Authenticate implements spi.Authenticator.
func (f CredentialAuthenticatorFunc) Authenticate(ctx context.Context, creds Credentials) (Principal, error) {
	return f(ctx, creds)
}

// WorkflowService is the boundary to an external durable orchestrator. The
// platform ships its own durable process engine (see spec_process.go and
// ref/process), so this exists only for deployments that already run one —
// Temporal, Cadence, DAGFlow — and want BCL to drive it.
type WorkflowService interface {
	Start(context.Context, string, any, WorkflowStartOptions) (WorkflowRun, error)
	Signal(context.Context, string, string, any) error
	Status(context.Context, string) (WorkflowRun, error)
}

// WorkflowStartOptions carries the cross-cutting concerns an external
// orchestrator needs at start time.
type WorkflowStartOptions struct {
	IdempotencyKey string
	TenantID       string
}

// WorkflowRun is an external orchestrator's run handle.
type WorkflowRun struct {
	ID       string `json:"id"`
	Workflow string `json:"workflow"`
	Status   string `json:"status"`
	Version  int64  `json:"version,omitempty"`
	Result   any    `json:"result,omitempty"`
}

// Resource is deliberately open: database handles, cache providers, clients,
// queues, object stores, authenticators and tool sessions can all be
// registered without changing this package.
type Resource any

// ResourceFactory opens a configured resource for one immutable generation.
// Returning an io.Closer gives the generation ownership of its lifecycle, so
// every pool and consumer opened at compile time is closed on shutdown in
// reverse order.
type ResourceFactory interface {
	Open(context.Context, ResourceSpec) (Resource, io.Closer, error)
}

// ResourceFactoryFunc adapts a function to ResourceFactory.
type ResourceFactoryFunc func(context.Context, ResourceSpec) (Resource, io.Closer, error)

// Open implements ResourceFactory.
func (f ResourceFactoryFunc) Open(ctx context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	return f(ctx, spec)
}

// BuildContext is what an action factory sees when it compiles one node. It is
// the place to do every expensive, per-node thing exactly once: resolve the
// resource, compile expressions and data pipelines, validate config, prepare
// statements. Whatever an action closes over here costs nothing per request.
type BuildContext struct {
	// Resources holds every already-open resource of this generation.
	Resources map[string]Resource
	// Schemas holds the compiled schema blocks, so a node can validate against
	// one by name.
	Schemas map[string]*CompiledSchema
	// Platform is the generation being built. It is how composite actions
	// (flow.*, process.*) reach the engine to invoke child intents. It is
	// fully constructed for resources and the engine, but its intents may not
	// all be registered yet — so use it at run time, never at build time.
	Platform *Platform
	// Registry is the trust boundary this generation compiled against.
	Registry *Registry
	// Document is the whole application model, for a node that needs to
	// resolve a cross-reference such as a named role or channel.
	Document *Document
}

// Resource resolves a named resource, reporting whether it exists. Prefer it
// over indexing Resources directly so the failure message is uniform.
func (b BuildContext) Resource(name string) (Resource, bool) {
	if b.Resources == nil {
		return nil, false
	}
	value, ok := b.Resources[name]
	return value, ok
}

// ActionFactory compiles a node once. Per-request work belongs in Action.Run.
type ActionFactory interface {
	Build(BuildContext, NodeSpec) (Action, error)
}

// ActionFactoryFunc adapts a function to ActionFactory.
type ActionFactoryFunc func(BuildContext, NodeSpec) (Action, error)

// Build implements ActionFactory.
func (f ActionFactoryFunc) Build(ctx BuildContext, spec NodeSpec) (Action, error) {
	return f(ctx, spec)
}

// Action is a transport-neutral, reusable node implementation. One Action
// instance serves every invocation of its node concurrently, so it must be
// safe for concurrent use and must keep no per-request state of its own.
type Action interface {
	Run(*ActionContext) (ActionResult, error)
}

// ActionFunc adapts a function to Action.
type ActionFunc func(*ActionContext) (ActionResult, error)

// Run implements Action.
func (f ActionFunc) Run(ctx *ActionContext) (ActionResult, error) { return f(ctx) }

// ActionContext is scoped to one invocation and one node.
//
// Context is deliberately the execution context rather than the pooled REF
// NodeContext: database/sql and net/http may retain a context briefly after the
// call returns, and a pooled NodeContext is cleared and reused the moment the
// node finishes.
type ActionContext struct {
	Context    context.Context
	Invocation *invocation.Invocation
	// Inputs holds the node's required facts, keyed by their local names.
	Inputs map[string]any
	// Config is the node's own config block.
	Config map[string]any
	// Resource is the resource the node named, already opened.
	Resource Resource
	// Node is the REF node context, for recording effects and short-circuits.
	Node *execution.NodeContext
	// Principal is the authenticated identity, zero when anonymous.
	Principal Principal
	// TenantID is the resolved tenant, empty in a single-tenant deployment.
	TenantID string
	// Platform is the running generation, for composite actions that invoke
	// child intents or start process runs.
	Platform *Platform
	// Depth is how many composite actions deep this invocation already is. The
	// platform enforces a ceiling so a cyclic flow.subflow cannot exhaust the
	// stack.
	Depth int
	// Now is the invocation's logical clock. Use it rather than time.Now so a
	// replayed process step computes the same value it did the first time.
	Now time.Time
}

// Decision lets declarative policy actions participate in REF's deny-dominant
// decision algebra: one deny anywhere in the plan blocks every effect node,
// regardless of how many allows were recorded.
type Decision struct {
	Allow       bool
	Message     string
	Constraints []execution.Constraint
	Obligations []execution.Obligation
}

// ActionResult publishes facts, records effects and optionally records a policy
// decision. Output keys must be declared by NodeSpec.Provides — publishing an
// undeclared fact is a compile-time contract violation caught at run time and
// reported as an error, never silently dropped.
type ActionResult struct {
	Outputs  map[string]any
	Effects  []effect.Effect
	Decision *Decision
}
