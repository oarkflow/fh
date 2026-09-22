package platform

// Document is the whole BCL application model: one file (or one directory of
// imported files) describes an entire deployable system.
//
// The block set is deliberately closed. BCL selects and configures capabilities
// that trusted Go code registered in a Registry; it can never introduce native
// code, open an unregistered connection kind, or execute an unregistered
// action. Everything an application author writes is data.
type Document struct {
	Name        string `bcl:"name"`
	Environment string `bcl:"environment"`
	// Version is stamped onto every process run so a long-lived run can be
	// migrated or rejected deliberately after a deploy.
	Version string `bcl:"version"`

	Secrets   []SecretSpec   `bcl:"secret,block"`
	Resources []ResourceSpec `bcl:"resource,block"`
	// Shapes are the reusable structural contracts. The block is spelled
	// `shape` because BCL reserves `schema` for its own schema support, and a
	// block by that name never reaches this document.
	Shapes  []SchemaSpec `bcl:"shape,block"`
	Roles   []RoleSpec   `bcl:"role,block"`
	Tenants []TenantSpec `bcl:"tenant,block"`

	Intents   []IntentSpec  `bcl:"intent,block"`
	Processes []ProcessSpec `bcl:"process,block"`

	Routes    []RouteSpec    `bcl:"route,block"`
	Workers   []WorkerSpec   `bcl:"worker,block"`
	Schedules []ScheduleSpec `bcl:"schedule,block"`
	Triggers  []TriggerSpec  `bcl:"trigger,block"`
}

// SecretSpec resolves one named secret at load time, from the environment or a
// file. A required secret that cannot be resolved fails compilation, so a
// misconfigured deployment never starts serving — it refuses to boot.
//
// Secret values are never included in the redacted Document that
// Platform.Document exposes, and never logged.
type SecretSpec struct {
	Name     string `bcl:",id"`
	Env      string `bcl:"env"`
	File     string `bcl:"file"`
	Value    string `bcl:"value"`
	Required bool   `bcl:"required"`
}

// ResourceSpec describes one named connection, service or coordination
// primitive. Kind selects a registered ResourceFactory; Config is that
// factory's own schema.
//
// A resource is opened exactly once per immutable generation and is shared by
// every node that names it, so connection pools, caches and queue consumers
// have generation lifetime rather than request lifetime.
type ResourceSpec struct {
	Name string `bcl:",id"`
	Kind string `bcl:"kind"`
	// Description is operator-facing documentation carried into the catalog.
	Description string         `bcl:"description"`
	Config      map[string]any `bcl:"config"`
	// DependsOn names resources that must be opened before this one, for a
	// dependency the compiler cannot infer from a well-known config key.
	// Inferred dependencies (config.database, config.cache, config.queue and
	// the rest listed in resourceDependencyKeys) need no declaration.
	DependsOn []string `bcl:"depends_on"`

	// resolved holds the already-open resources this provider depends on, keyed
	// by the config key that named them. It is populated by the compiler in
	// dependency order and is not a BCL field — a provider reads it through
	// requireSQLDependency and friends rather than reaching into the platform.
	resolved map[string]Resource
}

// Dependency returns the already-open resource named by the given config key.
func (r ResourceSpec) Dependency(key string) (Resource, bool) {
	name := configString(r.Config, key, "")
	if name == "" {
		return nil, false
	}
	value, ok := r.resolved[name]
	return value, ok
}

// SchemaSpec is a reusable structural contract — a `shape` block — referenced by
// validate.schema nodes, CRUD resources, human-task forms and route bodies.
// One schema declared once is validated identically at every boundary.
type SchemaSpec struct {
	Name        string `bcl:",id"`
	Kind        string `bcl:"kind,ident"`
	Description string `bcl:"description"`
	// Props are the shape's fields. The block is spelled `prop` because BCL
	// reserves `field` for its own schema support and a block by that name never
	// binds.
	Props []SchemaFieldSpec `bcl:"prop,block"`
	// Required lists field names required at this level, as an alternative to
	// marking each field. Both forms are honoured.
	Required []string `bcl:"required"`
	// AdditionalProperties defaults to true; set false to reject unknown keys.
	AdditionalProperties *bool `bcl:"additional_properties"`
}

// SchemaFieldSpec is one property of a shape — a `prop` block. Kind is one of
// string, int, number, bool, time, array, object, or the name of another shape.
type SchemaFieldSpec struct {
	Name        string `bcl:",id"`
	Kind        string `bcl:"kind,ident"`
	Description string `bcl:"description"`
	Required    bool   `bcl:"required"`
	// Items names the element type or schema for an array field.
	Items string `bcl:"items"`
	// Schema names another schema block for an object field.
	Schema string `bcl:"schema"`
	// Enum restricts the value to a fixed set.
	Enum []string `bcl:"enum"`
	// Pattern is a Go regular expression applied to string values.
	Pattern string `bcl:"pattern"`
	// Min/Max bound numbers, and MinLength/MaxLength bound strings and arrays.
	Min       *float64 `bcl:"min"`
	Max       *float64 `bcl:"max"`
	MinLength *int     `bcl:"min_length"`
	MaxLength *int     `bcl:"max_length"`
	// Format applies a named string check: email, uuid, url, date, date_time.
	Format string `bcl:"format,ident"`
	// Default is substituted when the field is absent.
	Default any `bcl:"default"`
	// Sensitive fields are redacted from audit records and error messages.
	Sensitive bool `bcl:"sensitive"`
}

// RoleSpec declares one role, its direct permissions, and the roles it
// inherits. The authz.rbac resource resolves the transitive closure once at
// load time, so a permission check is a map lookup rather than a graph walk.
//
// Permissions may end in ":*" to grant a whole namespace, e.g. "order:*".
type RoleSpec struct {
	Name        string   `bcl:",id"`
	Description string   `bcl:"description"`
	Permissions []string `bcl:"permissions"`
	// Inherits names roles whose permissions this role also holds. Cycles are
	// rejected at compile time.
	Inherits []string `bcl:"inherits"`
}

// TenantSpec declares one tenant of a multi-tenant deployment: its own limits,
// its own constants, and the queues and processes it may use. An empty
// allowlist means "no restriction", never "nothing allowed".
type TenantSpec struct {
	Name        string         `bcl:",id"`
	DisplayName string         `bcl:"display_name"`
	Disabled    bool           `bcl:"disabled"`
	RateLimit   int            `bcl:"rate_limit"`
	RateWindow  Duration       `bcl:"rate_window"`
	Processes   []string       `bcl:"processes"`
	Queues      []string       `bcl:"queues"`
	Constants   map[string]any `bcl:"constants"`
	Metadata    map[string]any `bcl:"metadata"`
}

// WorkerSpec binds a durable queue job type to an intent or a process.
//
// Multiple process replicas may declare the same worker: the queue resource's
// own Claim implementation owns the lease and atomic ownership, so exactly one
// replica runs each job.
type WorkerSpec struct {
	Name    string `bcl:",id"`
	Queue   string `bcl:"queue"`
	JobType string `bcl:"job_type"`
	Intent  string `bcl:"intent"`
	// Process, when set instead of Intent, starts (or advances) a durable
	// process run from the job payload.
	Process string `bcl:"process"`
	// Concurrency is a per-replica cap on simultaneous handlers for this job
	// type. Zero uses the queue resource's own worker count.
	Concurrency int `bcl:"concurrency"`
	// MaxAttempts overrides the queue default for this job type.
	MaxAttempts int `bcl:"max_attempts"`
	// Disabled keeps the declaration but stops consumption — the supported way
	// to drain a job type without editing every deployment.
	Disabled bool `bcl:"disabled"`
}

// ScheduleSpec runs an intent or a process on a recurring or one-shot
// schedule. Schedules are enqueued onto a durable queue rather than run from an
// in-process ticker, so exactly one replica executes each firing and a missed
// window is not silently lost.
type ScheduleSpec struct {
	Name string `bcl:",id"`
	// Every is a fixed interval; Cron is a 5-field crontab expression; At is a
	// single RFC3339 instant. Exactly one must be set.
	Every Duration `bcl:"every"`
	Cron  string   `bcl:"cron"`
	At    string   `bcl:"at"`
	// Timezone names the IANA location a Cron expression is evaluated in.
	// Empty means UTC — never the host's local zone, which would make the same
	// configuration behave differently per machine.
	Timezone string         `bcl:"timezone"`
	Queue    string         `bcl:"queue"`
	Intent   string         `bcl:"intent"`
	Process  string         `bcl:"process"`
	TenantID string         `bcl:"tenant_id"`
	Payload  map[string]any `bcl:"payload"`
	Disabled bool           `bcl:"disabled"`
	// Jitter spreads firings across replicas and avoids thundering herds on a
	// shared dependency. The delay added is uniform in [0, Jitter).
	Jitter Duration `bcl:"jitter"`
}

// TriggerSpec is a non-HTTP-route entry point: an inbound webhook whose body
// starts work, or a named event that resumes parked process runs.
type TriggerSpec struct {
	Name string `bcl:",id"`
	// Kind is "webhook" or "event". BCL cannot bind a field named `type`, so the
	// discriminator is spelled `kind` here and everywhere else in this spec.
	Kind string `bcl:"kind,ident"`
	Path string `bcl:"path"`
	// Event is the process event name this trigger delivers, for type "event".
	Event   string `bcl:"event"`
	Intent  string `bcl:"intent"`
	Process string `bcl:"process"`
	Queue   string `bcl:"queue"`
	// Secret names a secret block holding the HMAC key for webhook signature
	// verification. A webhook trigger without one is rejected at compile time:
	// an unauthenticated public mutation entry point is never the intent.
	Secret string `bcl:"secret"`
	// SignatureHeader and SignatureAlgorithm describe the incoming signature.
	// Defaults are "X-Signature" and "sha256".
	SignatureHeader    string `bcl:"signature_header"`
	SignatureAlgorithm string `bcl:"signature_algorithm"`
	// TimestampHeader and Tolerance bound replay. Zero tolerance disables the
	// timestamp check, which is only safe behind an idempotency guard.
	TimestampHeader string   `bcl:"timestamp_header"`
	Tolerance       Duration `bcl:"tolerance"`
	// CorrelationPath extracts the event correlation key from the body, e.g.
	// "order.id", so the event reaches the one run waiting for it.
	CorrelationPath string `bcl:"correlation_path"`
	Disabled        bool   `bcl:"disabled"`
}
