package platform

// IntentSpec describes one REF dependency graph and the resource budget it may
// spend. An intent is the request-scoped unit of work: it is compiled into an
// immutable plan whose producers are proven, whose cycles are rejected, and
// whose independent nodes run concurrently.
//
// An intent has no control flow of its own — every node in its plan runs, gated
// only by policy decisions and speculation class. Conditionals, loops, races
// and retries are expressed with the flow.* actions, which invoke child intents
// from inside a single node (see flow.go). Anything that must survive a process
// restart belongs in a ProcessSpec instead.
type IntentSpec struct {
	Name        string `bcl:",id"`
	Description string `bcl:"description"`
	// Response names the fact whose value becomes the intent's result.
	Response string `bcl:"response"`

	Timeout       Duration `bcl:"timeout"`
	MaxDBQueries  int      `bcl:"max_db_queries"`
	MaxExternalIO int      `bcl:"max_external_io"`
	MaxMemory     int64    `bcl:"max_memory"`
	MaxEffects    int      `bcl:"max_effects"`

	// InputSchema names a schema block validated against the decoded input
	// before any node runs. This is the cheapest place to reject bad input:
	// nothing has been read, written or spent yet.
	InputSchema string `bcl:"input_schema"`
	// OutputSchema validates the response fact, catching a graph that quietly
	// stopped producing a field its consumers depend on.
	OutputSchema string `bcl:"output_schema"`

	// Authz gates the whole intent. It is enforced as a REF decision node, so
	// it participates in the deny-dominant decision algebra and blocks every
	// effect node in the plan rather than being advisory.
	Authz *AuthzSpec `bcl:"authz"`

	// Idempotent marks an intent safe to replay with the same input. Route and
	// worker idempotency guards refuse to short-circuit a non-idempotent
	// intent, so a replayed POST cannot silently return a cached success for
	// work that actually needs doing again.
	Idempotent bool `bcl:"idempotent"`

	// InputData and OutputData shape the intent's boundaries: InputData rewrites
	// the decoded request before it is published as the "input" fact, OutputData
	// shapes the response fact on the way out (redaction, field masking,
	// renames for a public API contract).
	InputData  *DataSpec `bcl:"input_data"`
	OutputData *DataSpec `bcl:"output_data"`

	Nodes []NodeSpec `bcl:"node,block"`
}

// NodeSpec describes one graph node. Requires and Provides are logical fact
// names local to the containing intent: the compiler resolves them to dense
// plan slots and proves that exactly one node produces each.
//
// Type is catalog metadata (which family this node belongs to, for an editor);
// Uses is the executable, versionable capability that actually runs. Kind and
// Speculation are the REF safety declarations — they decide when the scheduler
// is allowed to run this node relative to authentication and policy.
type NodeSpec struct {
	Name string `bcl:",id"`
	// Family is catalog metadata: which semantic family this node belongs to.
	// BCL cannot bind a field named `type`, so it is spelled `family`.
	Family string `bcl:"family,ident"`
	Uses   string `bcl:"uses"`
	// Resource names the resource this node's action operates on, if any.
	Resource string `bcl:"resource"`
	// Kind is the REF node kind: pure, read, decision, effect, async_effect or
	// stream. It determines whether the node runs before or after the effect
	// barrier, and whether a policy denial can stop it.
	Kind string `bcl:"kind,ident"`
	// Speculation is the REF speculation class: none, pre_auth_safe,
	// post_identity_safe or post_policy_safe. The compiler proves the claim
	// against the graph and refuses a node that cannot honour it.
	Speculation string `bcl:"speculation,ident"`

	Requires []string       `bcl:"requires"`
	Provides []string       `bcl:"provides"`
	Config   map[string]any `bcl:"config"`

	// Description is carried into the catalog for editors and generated docs.
	Description string `bcl:"description"`

	// InputData reshapes the assembled Requires map before the action sees it;
	// OutputData reshapes what the action published before it becomes a fact.
	// Together they let one reusable action serve differently-shaped graphs
	// without a bespoke adapter node.
	InputData  *DataSpec `bcl:"input_data"`
	OutputData *DataSpec `bcl:"output_data"`

	// Retry retries this node in place. It is request-scoped: use a process
	// step's retry policy for anything that must survive a restart.
	Retry *RetrySpec `bcl:"retry"`
	// Timeout bounds this node alone, inside the intent's own budget.
	Timeout Duration `bcl:"timeout"`

	// Authz gates this node specifically, as a decision in the plan. Use it
	// when one graph serves several audiences and only part of it is
	// privileged.
	Authz *AuthzSpec `bcl:"authz"`

	// OnError selects the failure posture: "fail" (default, the invocation
	// fails), "continue" (publish Fallback and carry on) or "fallback"
	// (identical to continue, named for readability). A node that continues
	// must declare a Fallback value for every fact it provides, so downstream
	// nodes can never observe a missing fact.
	OnError  string         `bcl:"on_error,ident"`
	Fallback map[string]any `bcl:"fallback"`

	// Sensitive marks the node's outputs as containing regulated data. Audit
	// records and error messages redact them, and the debug introspection
	// surface reports the fact name without its value.
	Sensitive bool `bcl:"sensitive"`
}

// RetrySpec is one retry policy, shared by intent nodes and process steps.
//
// Strategy is "fixed", "linear", "exponential", "exponential_jitter" or
// "decorrelated_jitter". Jitter is applied on top of any strategy when set,
// which matters for a dependency many replicas hit at once.
type RetrySpec struct {
	MaxAttempts  int      `bcl:"max_attempts"`
	Strategy     string   `bcl:"strategy,ident"`
	InitialDelay Duration `bcl:"initial_delay"`
	MaxDelay     Duration `bcl:"max_delay"`
	Jitter       bool     `bcl:"jitter"`
	// RetryOn restricts retries to these failure categories (invalid_input,
	// not_found, conflict, permission, auth, rate_limit, unavailable, timeout,
	// internal). Empty means retry only the transient categories — never
	// invalid_input or permission, which will fail identically forever.
	RetryOn []string `bcl:"retry_on"`
}

// ---------------------------------------------------------------------------
// Data shaping
// ---------------------------------------------------------------------------

// DataSpec is the declarative payload-shaping pipeline, applied wherever data
// crosses a boundary: an intent's input and output, a node's inputs and
// outputs, a process edge's payload, a route's response.
//
// The stages run in a fixed, documented order so the same spec always produces
// the same result regardless of how it was written:
//
//  1. Source      — narrow to a sub-path of the incoming value
//  2. Defaults    — fill absent keys
//  3. Extract     — build target keys from expressions over the input
//  4. Set         — assign literal or templated values
//  5. Transforms  — per-path operations, in declaration order
//  6. Append/Prepend — string and list concatenation
//  7. Rename      — move keys
//  8. Coerce      — convert types
//  9. Flatten     — lift nested objects into the parent
//  10. Pick/Omit  — select the surviving key set
//  11. Redact/Mask — remove or partially hide sensitive values
//  12. Filters    — accept or reject the whole payload
//  13. Limits     — enforce MaxBytes and MaxDepth
//  14. Schema     — structural validation, last, on the final shape
//
// Every stage is optional. A zero DataSpec is a no-op and costs nothing: the
// compiler omits the pipeline entirely rather than running fourteen empty
// stages per request.
type DataSpec struct {
	// Source narrows the incoming value to one path before anything else runs,
	// e.g. "order.customer".
	Source string `bcl:"source"`

	// Extract builds output keys from expressions evaluated over the input:
	// extract { total "order.amount * order.quantity" }. The target key may be
	// dotted to build nested output.
	Extract map[string]string `bcl:"extract"`

	// Set assigns literal values. String values support {{ }} templating over
	// the same variable environment as expressions.
	Set map[string]any `bcl:"set"`

	// Defaults fill keys that are absent or null, never keys that are present
	// and empty — "" and 0 are values, not absences.
	Defaults map[string]any `bcl:"defaults"`

	// Pick keeps only these keys (dotted paths allowed); Omit removes these.
	// When both are set Pick runs first.
	Pick []string `bcl:"pick"`
	Omit []string `bcl:"omit"`

	// Rename moves keys: rename { customer_id "customerId" }.
	Rename map[string]string `bcl:"rename"`

	// Coerce converts values in place: coerce { amount "float" age "int" }.
	// Supported targets are string, int, float, bool, time, duration, json.
	Coerce map[string]string `bcl:"coerce"`

	// Append and Prepend concatenate onto an existing string or list value.
	Append  map[string]string `bcl:"append"`
	Prepend map[string]string `bcl:"prepend"`

	// Flatten lifts the named nested objects' keys into the parent and removes
	// the nesting, e.g. flatten [address] turns address.city into city.
	Flatten []string `bcl:"flatten"`

	// Redact removes these paths entirely. Mask replaces them with a fixed
	// number of asterisks plus the last few characters, so a support agent can
	// still recognise a value without being able to read it.
	Redact []string `bcl:"redact"`
	Mask   []string `bcl:"mask"`
	// MaskKeep is how many trailing characters Mask preserves. Default 4.
	MaskKeep int `bcl:"mask_keep"`

	Transforms []DataTransformSpec `bcl:"transform,block"`
	Filters    []DataFilterSpec    `bcl:"filter,block"`

	// MaxBytes and MaxDepth bound the payload after shaping. They are a real
	// safety limit, not a hint: exceeding either fails the boundary rather
	// than truncating, because silently truncated data is worse than a refusal.
	MaxBytes int64 `bcl:"max_bytes"`
	MaxDepth int   `bcl:"max_depth"`

	// Strict rejects the payload when Extract or Transforms reference a path
	// that does not exist, instead of treating it as null. Turn it on for
	// anything whose correctness depends on the field being there.
	Strict bool `bcl:"strict"`

	// Schema names a schema block validated against the final shape.
	Schema string `bcl:"schema"`
}

// Empty reports whether this spec would do nothing, so the compiler can skip
// building a pipeline for it.
func (d *DataSpec) Empty() bool {
	if d == nil {
		return true
	}
	return d.Source == "" && len(d.Extract) == 0 && len(d.Set) == 0 && len(d.Defaults) == 0 &&
		len(d.Pick) == 0 && len(d.Omit) == 0 && len(d.Rename) == 0 && len(d.Coerce) == 0 &&
		len(d.Append) == 0 && len(d.Prepend) == 0 && len(d.Flatten) == 0 &&
		len(d.Redact) == 0 && len(d.Mask) == 0 && len(d.Transforms) == 0 && len(d.Filters) == 0 &&
		d.MaxBytes == 0 && d.MaxDepth == 0 && d.Schema == ""
}

// DataTransformSpec is one per-path operation inside a DataSpec.
//
// Either Expr (an expression whose result replaces the value at Path) or Op
// (a named operation, optionally parameterised by Arg) must be set. Named ops
// are the common cases that would otherwise need an awkward expression:
// upper, lower, trim, title, slug, hash, base64, base64_decode, json_encode,
// json_decode, round, abs, length, first, last, unique, sort, join, split,
// now, timestamp, uuid, default.
type DataTransformSpec struct {
	Path string `bcl:"path"`
	Expr string `bcl:"expr"`
	Op   string `bcl:"op,ident"`
	Arg  string `bcl:"arg"`
	// OnlyIf gates the transform on an expression, so one spec can shape
	// conditionally without a branch node. (BCL reserves `when`.)
	OnlyIf string `bcl:"only_if"`
}

// DataFilterSpec accepts or rejects a whole payload.
//
// Mode is "allow" (the default: the payload proceeds only when Expr is true) or
// "deny" (the payload is rejected when Expr is true). A filtered payload on an
// intent boundary is an invalid-input failure; on a process edge it means the
// edge does not traverse, which is how DAGFlow's filter edges behave.
type DataFilterSpec struct {
	Expr   string `bcl:"expr"`
	Mode   string `bcl:"mode,ident"`
	Reason string `bcl:"reason"`
}
