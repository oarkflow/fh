package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

// Observability actions: audit records, metrics and trace annotations.
//
// The audit trail here is hash-chained. Each entry carries the hash of the
// previous entry for its stream, so removing or editing a record breaks the chain
// from that point onward and the break is detectable. That property is what makes
// an audit log evidence rather than a second, less reliable copy of the
// application's logs — and it is cheap: one SHA-256 over the serialised entry.
//
// The chain is per stream (usually per tenant), not global, so two tenants writing
// concurrently do not serialise behind each other.

func registerObservabilityActions(r *Registry) {
	mustAction(r, "audit.record", auditRecordAction, ActionInfo{
		Family:       "observability",
		Summary:      "Append a hash-chained audit entry",
		ResourceKind: "database",
		Provides:     "An object with the entry id and its hash",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "action", Type: "template", Required: true, Summary: "What happened, e.g. \"order.approved\""},
			{Name: "subject", Type: "template", Summary: "What it happened to, e.g. an order id"},
			{Name: "outcome", Type: "template", Default: "success"},
			{Name: "detail_fact", Type: "fact", Summary: "Structured detail to record"},
			{Name: "stream", Type: "template", Summary: "Chain to append to. Defaults to the tenant, or \"default\"."},
			{Name: "table", Type: "string", Default: "platform_audit"},
			{Name: "migrate", Type: "bool", Default: "true"},
			{Name: "redact", Type: "[]string", Summary: "Paths removed from the recorded detail"},
			{Name: "mask", Type: "[]string"},
		},
	})

	mustAction(r, "metric.emit", metricEmitAction, ActionInfo{
		Family:   "observability",
		Summary:  "Record a counter or gauge observation",
		Provides: "The observed value",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "name", Type: "string", Required: true},
			{Name: "kind", Type: "string", Default: "counter", Summary: "counter or gauge"},
			{Name: "value_fact", Type: "fact", Summary: "Numeric value. Omitted counts one."},
			{Name: "labels", Type: "map", Summary: "Templated label values"},
		},
	})

	mustAction(r, "trace.span", traceSpanAction, ActionInfo{
		Family:   "observability",
		Summary:  "Annotate this invocation with a named event and attributes",
		Provides: "The recorded annotation",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "name", Type: "string", Required: true},
			{Name: "attributes", Type: "map"},
			{Name: "level", Type: "string", Default: "info", Summary: "debug, info, warn or error"},
		},
	})
}

// ---------------------------------------------------------------------------
// Audit
// ---------------------------------------------------------------------------

// auditLog appends hash-chained entries to a table.
//
// Head hashes are cached per stream so the common case is one insert rather than
// a read followed by an insert. The cache is authoritative only because this
// process is the one appending; the insert itself carries the previous hash, so a
// second process appending concurrently produces a detectable fork rather than a
// silently rewritten chain.
type auditLog struct {
	db    *Database
	table string

	mu        sync.Mutex
	heads     map[string]string
	sequences map[string]int64
	ready     bool
}

var (
	auditLogsMu sync.Mutex
	auditLogs   = map[string]*auditLog{}
)

// sharedAuditLog returns one log per (database, table) pair so every audit node
// in an application appends to the same chain rather than each keeping its own
// idea of the head.
func sharedAuditLog(db *Database, table string) *auditLog {
	key := fmt.Sprintf("%p/%s", db, table)
	auditLogsMu.Lock()
	defer auditLogsMu.Unlock()
	if existing, ok := auditLogs[key]; ok {
		return existing
	}
	log := &auditLog{db: db, table: table, heads: map[string]string{}, sequences: map[string]int64{}}
	auditLogs[key] = log
	return log
}

func (a *auditLog) migrate(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ready {
		return nil
	}
	dialect := a.db.Dialect
	statements := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id            %s NOT NULL PRIMARY KEY,
			stream        %s NOT NULL,
			sequence      BIGINT NOT NULL,
			recorded_at   %s NOT NULL,
			actor_id      %s,
			actor_name    %s,
			tenant_id     %s,
			action        %s NOT NULL,
			subject       %s,
			outcome       %s,
			detail        TEXT,
			previous_hash %s,
			entry_hash    %s NOT NULL
		)`, a.table,
			textType(dialect), textType(dialect), timestampType(dialect),
			textType(dialect), textType(dialect), textType(dialect),
			textType(dialect), textType(dialect), textType(dialect),
			textType(dialect), textType(dialect)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_stream_idx ON %s (stream, sequence)`, a.table, a.table),
	}
	for _, statement := range statements {
		if _, err := a.db.ExecContext(ctx, statement); err != nil {
			if strings.Contains(statement, "CREATE INDEX") {
				continue
			}
			return err
		}
	}
	a.ready = true
	return nil
}

// auditEntry is one record. The field order here is the order they are hashed in,
// and it must not change: an existing chain would stop verifying.
type auditEntry struct {
	ID           string    `json:"id"`
	Stream       string    `json:"stream"`
	Sequence     int64     `json:"sequence"`
	RecordedAt   time.Time `json:"recorded_at"`
	ActorID      string    `json:"actor_id,omitempty"`
	ActorName    string    `json:"actor_name,omitempty"`
	TenantID     string    `json:"tenant_id,omitempty"`
	Action       string    `json:"action"`
	Subject      string    `json:"subject,omitempty"`
	Outcome      string    `json:"outcome,omitempty"`
	Detail       string    `json:"detail,omitempty"`
	PreviousHash string    `json:"previous_hash,omitempty"`
	EntryHash    string    `json:"entry_hash"`
}

// hash computes the entry's own hash over its canonical fields plus the previous
// hash. It excludes EntryHash itself, which is what makes the chain verifiable.
func (e auditEntry) hash() string {
	digest := sha256.New()
	for _, field := range []string{
		e.ID, e.Stream, fmt.Sprint(e.Sequence), e.RecordedAt.UTC().Format(time.RFC3339Nano),
		e.ActorID, e.ActorName, e.TenantID, e.Action, e.Subject, e.Outcome, e.Detail, e.PreviousHash,
	} {
		digest.Write([]byte(field))
		digest.Write([]byte{0})
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// append writes one entry, reading the chain head from the table the first time a
// stream is seen in this process.
func (a *auditLog) append(ctx context.Context, entry auditEntry) (auditEntry, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	previous, known := a.heads[entry.Stream]
	sequence := a.sequences[entry.Stream]
	if !known {
		// First append to this stream in this process: read the current head so the
		// chain continues from whatever is already stored rather than forking.
		statement := rebind(a.db.Dialect, fmt.Sprintf(
			"SELECT entry_hash, sequence FROM %s WHERE stream = $1 ORDER BY sequence DESC LIMIT 1", a.table))
		var (
			headHash     string
			headSequence int64
		)
		if err := a.db.QueryRowContext(ctx, statement, entry.Stream).Scan(&headHash, &headSequence); err == nil {
			previous, sequence = headHash, headSequence
		} else {
			// No rows yet: this is the start of the chain. A genuinely unreadable
			// table surfaces on the insert below rather than being papered over.
			previous, sequence = "", 0
		}
	}

	entry.Sequence = sequence + 1
	entry.PreviousHash = previous
	entry.EntryHash = entry.hash()

	statement := rebind(a.db.Dialect, fmt.Sprintf(`INSERT INTO %s
		(id, stream, sequence, recorded_at, actor_id, actor_name, tenant_id, action, subject, outcome, detail, previous_hash, entry_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, a.table))
	if _, err := a.db.ExecContext(ctx, statement,
		entry.ID, entry.Stream, entry.Sequence, entry.RecordedAt,
		nullString(entry.ActorID), nullString(entry.ActorName), nullString(entry.TenantID),
		entry.Action, nullString(entry.Subject), nullString(entry.Outcome),
		nullString(entry.Detail), nullString(entry.PreviousHash), entry.EntryHash,
	); err != nil {
		return auditEntry{}, err
	}
	a.heads[entry.Stream] = entry.EntryHash
	a.sequences[entry.Stream] = entry.Sequence
	return entry, nil
}

var auditRecordAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	db, err := requireResource[*Database](build, spec, "a database.sql resource")
	if err != nil {
		return nil, err
	}
	table, err := safeIdentifier(configString(spec.Config, "table", "platform_audit"))
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	action, err := configTemplate(spec.Config, "action", "")
	if err != nil || action == nil {
		return nil, fmt.Errorf("node %q: audit.record needs config.action", spec.Name)
	}
	subject, err := configTemplate(spec.Config, "subject", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	outcome, err := configTemplate(spec.Config, "outcome", "success")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	stream, err := configTemplate(spec.Config, "stream", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	detailFact := configString(spec.Config, "detail_fact", "")

	// The redaction pipeline is built from the node's own redact and mask lists,
	// plus the fields no audit record should ever carry.
	redactSpec := &DataSpec{
		Redact: append(configStrings(spec.Config, "redact"), alwaysRedacted...),
		Mask:   configStrings(spec.Config, "mask"),
	}
	redactor, err := compileDataSpec("node "+spec.Name+" audit", redactSpec, build.Schemas)
	if err != nil {
		return nil, err
	}
	log := sharedAuditLog(db, table)
	shouldMigrate := configBool(spec.Config, "migrate", true)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		if shouldMigrate {
			if err := log.migrate(ctx.Context); err != nil {
				return ActionResult{}, unavailable("could not prepare the audit table: %v", err)
			}
		}
		env := actionEnv(ctx)
		entry := auditEntry{
			ID:         newPrefixedID("aud"),
			RecordedAt: ctx.Now,
			ActorID:    ctx.Principal.ID,
			ActorName:  ctx.Principal.Username,
			TenantID:   ctx.TenantID,
		}
		if entry.Action, err = action.Render(env); err != nil {
			return ActionResult{}, err
		}
		if subject != nil {
			if entry.Subject, err = subject.Render(env); err != nil {
				return ActionResult{}, err
			}
		}
		if outcome != nil {
			if entry.Outcome, err = outcome.Render(env); err != nil {
				return ActionResult{}, err
			}
		}
		entry.Stream = ctx.TenantID
		if stream != nil {
			if rendered, err := stream.Render(env); err == nil && rendered != "" {
				entry.Stream = rendered
			}
		}
		if entry.Stream == "" {
			entry.Stream = "default"
		}
		if detailFact != "" {
			if value, found := resolvePath(ctx.Inputs, detailFact); found {
				redacted, err := redactor.Apply(value, env)
				if err != nil {
					return ActionResult{}, err
				}
				encoded, err := json.Marshal(redacted)
				if err != nil {
					return ActionResult{}, err
				}
				entry.Detail = string(encoded)
			}
		}
		written, err := log.append(ctx.Context, entry)
		if err != nil {
			return ActionResult{}, unavailable("could not write the audit entry: %v", err)
		}
		return acknowledgement(spec, map[string]any{
			"id":       written.ID,
			"hash":     written.EntryHash,
			"sequence": written.Sequence,
			"stream":   written.Stream,
		}), nil
	}), nil
})

// alwaysRedacted are paths no audit record may carry, whatever the node
// configured. An audit trail that contains the password somebody submitted is a
// liability, not a control.
var alwaysRedacted = []string{
	"password", "new_password", "current_password", "password_hash",
	"token", "access_token", "refresh_token", "api_key", "secret",
	"card_number", "cvv", "authorization",
}

// ---------------------------------------------------------------------------
// Metrics and traces
// ---------------------------------------------------------------------------

var metricEmitAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	name, err := requiredString(spec.Config, "name")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	kind := strings.ToLower(configString(spec.Config, "kind", "counter"))
	if kind != "counter" && kind != "gauge" {
		return nil, fmt.Errorf("node %q: metric kind must be counter or gauge", spec.Name)
	}
	valueFact := configString(spec.Config, "value_fact", "")
	labels, err := templateMap(spec, "labels")
	if err != nil {
		return nil, err
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value := 1.0
		if valueFact != "" {
			raw, found := resolvePath(ctx.Inputs, valueFact)
			if !found {
				return ActionResult{}, invalidInput("no numeric value at %q", valueFact)
			}
			number, ok := ToFloat(raw)
			if !ok {
				return ActionResult{}, invalidInput("%q is not a number", valueFact)
			}
			value = number
		}
		rendered, err := renderTemplateMap(labels, actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		recordMetric(name, kind, value, rendered)
		return acknowledgement(spec, value), nil
	}), nil
})

// metricStore is a minimal in-process aggregator.
//
// This is deliberately not a Prometheus client: adding one would put a dependency
// in this package for something every deployment already solves its own way. What
// it does give you is a snapshot endpoint and, more importantly, one place a host
// can redirect metrics from by overriding the metric.emit action.
type metricStore struct {
	mu      sync.Mutex
	samples map[string]*metricSample
}

type metricSample struct {
	Name   string            `json:"name"`
	Kind   string            `json:"kind"`
	Labels map[string]string `json:"labels,omitempty"`
	Count  int64             `json:"count"`
	Sum    float64           `json:"sum"`
	Last   float64           `json:"last"`
	Min    float64           `json:"min"`
	Max    float64           `json:"max"`
}

var metrics = &metricStore{samples: map[string]*metricSample{}}

func recordMetric(name, kind string, value float64, labels map[string]string) {
	key := name + "|" + labelKey(labels)
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	sample, ok := metrics.samples[key]
	if !ok {
		sample = &metricSample{Name: name, Kind: kind, Labels: labels, Min: value, Max: value}
		metrics.samples[key] = sample
	}
	sample.Count++
	sample.Sum += value
	sample.Last = value
	if value < sample.Min {
		sample.Min = value
	}
	if value > sample.Max {
		sample.Max = value
	}
}

// MetricSnapshot returns every metric observed since the process started, sorted
// for stable output. Serve it from an admin route, or ignore it and override
// metric.emit with your own exporter.
func MetricSnapshot() []map[string]any {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	out := make([]map[string]any, 0, len(metrics.samples))
	for _, sample := range metrics.samples {
		entry := map[string]any{
			"name":  sample.Name,
			"kind":  sample.Kind,
			"count": sample.Count,
			"sum":   sample.Sum,
			"last":  sample.Last,
			"min":   sample.Min,
			"max":   sample.Max,
		}
		if len(sample.Labels) > 0 {
			entry["labels"] = sample.Labels
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		return Stringify(out[i]["name"])+labelKey(nil) < Stringify(out[j]["name"])+labelKey(nil)
	})
	return out
}

func labelKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	for _, key := range keys {
		out.WriteString(key)
		out.WriteByte('=')
		out.WriteString(labels[key])
		out.WriteByte(',')
	}
	return out.String()
}

var traceSpanAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	name, err := requiredString(spec.Config, "name")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	attributes, err := templateMap(spec, "attributes")
	if err != nil {
		return nil, err
	}
	level := strings.ToLower(configString(spec.Config, "level", "info"))
	slogLevel := slog.LevelInfo
	switch level {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	case "info":
	default:
		return nil, fmt.Errorf("node %q: level must be debug, info, warn or error", spec.Name)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		rendered, err := renderTemplateMap(attributes, actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		fields := make([]any, 0, len(rendered)*2+4)
		fields = append(fields, "node", spec.Name)
		if ctx.Invocation != nil {
			fields = append(fields, "invocation", string(ctx.Invocation.ID))
		}
		for key, value := range rendered {
			fields = append(fields, key, value)
		}
		slog.Log(ctx.Context, slogLevel, name, fields...)
		return acknowledgement(spec, map[string]any{"name": name, "attributes": rendered}), nil
	}), nil
})
