package platform

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/bcl"
)

// BCL binding tests.
//
// Every one of these guards against a failure mode that is silent. A spec field
// named `type` does not error — it stays empty, and the validation that depended on
// it quietly never runs. A `time.Duration` field does not error — it becomes zero,
// and the timeout an author configured never applies. Those are exactly the bugs
// that reach production, so they are asserted here rather than trusted.

// TestBCLReservedNamesStillBehaveAsDocumented pins the four constraints
// bclcompat.go is written around. If a BCL upgrade changes any of them, this fails
// here rather than in somebody's deployment.
func TestBCLReservedNamesStillBehaveAsDocumented(t *testing.T) {
	type probe struct {
		Name string `bcl:",id"`
		// Each of these is a name the spec deliberately avoids.
		Type    string `bcl:"type"`
		Mapping string `bcl:"map"`
		Fine    string `bcl:"condition"`
	}
	type doc struct {
		Probes []probe `bcl:"probe,block"`
	}

	t.Run("type and map do not bind", func(t *testing.T) {
		var parsed doc
		source := "probe \"p\" {\n  type \"x\"\n  map \"y\"\n  condition \"z\"\n}\n"
		if err := bcl.UnmarshalWithOptions([]byte(source), &parsed, &bcl.Options{}); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(parsed.Probes) != 1 {
			t.Fatalf("parsed %d probes", len(parsed.Probes))
		}
		if parsed.Probes[0].Fine != "z" {
			t.Fatalf("an ordinary field did not bind: %q", parsed.Probes[0].Fine)
		}
		if parsed.Probes[0].Type != "" || parsed.Probes[0].Mapping != "" {
			t.Fatal("`type` or `map` now binds — bclcompat.go's constraints are out of date, and the spec could use the natural names again")
		}
	})

	t.Run("field blocks never bind", func(t *testing.T) {
		type inner struct {
			Name string `bcl:",id"`
			Val  string `bcl:"val"`
		}
		type outer struct {
			Name   string  `bcl:",id"`
			Fields []inner `bcl:"field,block"`
			Props  []inner `bcl:"prop,block"`
		}
		type doc2 struct {
			Outers []outer `bcl:"outer,block"`
		}
		var parsed doc2
		source := "outer \"o\" {\n  field \"a\" {\n    val \"x\"\n  }\n  prop \"b\" {\n    val \"y\"\n  }\n}\n"
		if err := bcl.UnmarshalWithOptions([]byte(source), &parsed, &bcl.Options{}); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(parsed.Outers) != 1 || len(parsed.Outers[0].Props) != 1 {
			t.Fatalf("a `prop` block did not bind: %+v", parsed.Outers)
		}
		if len(parsed.Outers[0].Fields) != 0 {
			t.Fatal("`field` blocks now bind — shapes could use the natural block name again")
		}
	})

	t.Run("a schema block never reaches the document", func(t *testing.T) {
		type shaped struct {
			Schemas []probe `bcl:"schema,block"`
			Shapes  []probe `bcl:"shape,block"`
		}
		var parsed shaped
		source := "schema \"A\" {\n  condition \"x\"\n}\nshape \"B\" {\n  condition \"y\"\n}\n"
		if err := bcl.UnmarshalWithOptions([]byte(source), &parsed, &bcl.Options{}); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(parsed.Shapes) != 1 {
			t.Fatalf("a `shape` block did not bind: %d", len(parsed.Shapes))
		}
		if len(parsed.Schemas) != 0 {
			t.Fatal("`schema` blocks now bind — the spec could use the natural block name again")
		}
	})

	t.Run("from and to bind only on their own line", func(t *testing.T) {
		type edge struct {
			Name string `bcl:",id"`
			From string `bcl:"from"`
			To   string `bcl:"to"`
		}
		type edged struct {
			Edges []edge `bcl:"edge,block"`
		}
		var inline, split edged
		if err := bcl.UnmarshalWithOptions([]byte("edge \"a\" { from \"x\" to \"y\" }\n"), &inline, &bcl.Options{}); err != nil {
			t.Fatalf("parse inline: %v", err)
		}
		if err := bcl.UnmarshalWithOptions([]byte("edge \"a\" {\n  from \"x\"\n  to \"y\"\n}\n"), &split, &bcl.Options{}); err != nil {
			t.Fatalf("parse split: %v", err)
		}
		if len(split.Edges) != 1 || split.Edges[0].From != "x" || split.Edges[0].To != "y" {
			t.Fatalf("one key per line did not bind: %+v", split.Edges)
		}
		if len(inline.Edges) == 1 && inline.Edges[0].From == "x" && inline.Edges[0].To == "y" {
			t.Fatal("`from`/`to` now bind inline — the one-key-per-line rule could be relaxed")
		}
	})

	t.Run("when derails the parse", func(t *testing.T) {
		var parsed doc
		source := "probe \"p\" {\n  when \"x\"\n  condition \"z\"\n}\n"
		err := bcl.UnmarshalWithOptions([]byte(source), &parsed, &bcl.Options{})
		bound := err == nil && len(parsed.Probes) == 1 && parsed.Probes[0].Fine == "z"
		if bound {
			t.Fatal("`when` no longer derails the parse — the spec could accept it as a guard alias again")
		}
	})
}

// TestDurationFieldsParse asserts that the platform's own Duration type does what
// a time.Duration field silently failed to do.
func TestDurationFieldsParse(t *testing.T) {
	type doc struct {
		Steps []struct {
			Name    string   `bcl:",id"`
			Timeout Duration `bcl:"timeout"`
			Grace   Duration `bcl:"grace"`
		} `bcl:"step,block"`
	}
	var parsed doc
	source := "step \"a\" {\n  timeout 45m\n  grace \"1h30m\"\n}\n"
	if err := bcl.UnmarshalWithOptions([]byte(source), &parsed, &bcl.Options{}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	step := parsed.Steps[0]

	// Both the bare and the quoted form must work: an author should not have to
	// remember which.
	timeout, err := step.Timeout.Parse("timeout", 0)
	if err != nil {
		t.Fatalf("bare duration: %v", err)
	}
	if timeout != 45*time.Minute {
		t.Fatalf("timeout = %s, want 45m", timeout)
	}
	grace, err := step.Grace.Parse("grace", 0)
	if err != nil {
		t.Fatalf("quoted duration: %v", err)
	}
	if grace != 90*time.Minute {
		t.Fatalf("grace = %s, want 1h30m", grace)
	}

	// An unparseable duration must be an error rather than a silent zero.
	if _, err := Duration("5 minutes").Parse("field", 0); err == nil {
		t.Fatal("\"5 minutes\" was accepted as a duration")
	}
	if _, err := Duration("-1h").Parse("field", 0); err == nil {
		t.Fatal("a negative duration was accepted")
	}
	// An unset duration takes the fallback, which is how an optional timeout works.
	if got, err := Duration("").Parse("field", time.Second); err != nil || got != time.Second {
		t.Fatalf("unset duration = %s, %v", got, err)
	}
}

// TestExampleDocumentsBind parses every shipped example and asserts the fields
// that used to bind silently wrong now carry their values.
//
// It does not open resources: that would need a database. What it proves is that
// the documents are syntactically valid against the current spec and that the
// interesting fields actually arrive — which is the half that was broken before.
func TestExampleDocumentsBind(t *testing.T) {
	root := exampleRoot(t)
	for _, name := range []string{"ref-platform", "ref-platform-todo", "ref-platform-complete", "ref-bookmark"} {
		path := filepath.Join(root, name, "app.bcl")
		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(path)
			if err != nil {
				t.Skipf("example not present: %v", err)
			}
			var parsed Document
			if err := bcl.UnmarshalWithOptions(source, &parsed, &bcl.Options{
				BaseDir:        filepath.Dir(path),
				AllowEnv:       true,
				Env:            exampleEnv,
				ResolveImports: true,
				Strict:         true,
			}); err != nil {
				t.Fatalf("parse: %v", err)
			}
			if parsed.Name == "" {
				t.Fatal("the application has no name")
			}
			if len(parsed.Resources) == 0 || len(parsed.Intents) == 0 || len(parsed.Routes) == 0 {
				t.Fatalf("thin document: %d resources, %d intents, %d routes",
					len(parsed.Resources), len(parsed.Intents), len(parsed.Routes))
			}

			// Node families and durations are the two things that used to bind to
			// nothing, so they are asserted specifically.
			families, timeouts := 0, 0
			for _, intent := range parsed.Intents {
				if intent.Timeout.Set() {
					if _, err := intent.Timeout.Parse("timeout", 0); err != nil {
						t.Fatalf("intent %q timeout: %v", intent.Name, err)
					}
					timeouts++
				}
				for _, node := range intent.Nodes {
					if node.Family != "" {
						families++
					}
					if node.Uses == "" {
						t.Fatalf("intent %q node %q has no uses", intent.Name, node.Name)
					}
				}
			}
			if families == 0 && name != "ref-platform-complete" {
				t.Fatal("no node declared a family — the `family` key is not binding")
			}
			if timeouts == 0 {
				t.Fatal("no intent declared a timeout — durations are not binding")
			}
		})
	}
}

// TestOrderExampleDeclaresTheWholeSurface asserts the flagship example actually
// exercises what it claims to: every tier, not just the request one.
//
// This is a documentation test as much as a behavioural one. The example is the
// thing somebody reads to learn the platform, and an example that quietly stopped
// covering the durable tier would teach the wrong lesson.
func TestOrderExampleDeclaresTheWholeSurface(t *testing.T) {
	path := filepath.Join(exampleRoot(t), "ref-platform", "app.bcl")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("example not present: %v", err)
	}
	var doc Document
	if err := bcl.UnmarshalWithOptions(source, &doc, &bcl.Options{
		BaseDir: filepath.Dir(path), AllowEnv: true, Env: exampleEnv,
		ResolveImports: true, Strict: true,
	}); err != nil {
		t.Fatalf("parse: %v", err)
	}

	kinds := make([]string, 0, len(doc.Resources))
	for _, resource := range doc.Resources {
		kinds = append(kinds, resource.Kind)
	}
	for _, want := range []string{
		"database.sql", "cache.sql", "session.sql", "queue.sql", "store.sql",
		"lock.store", "ratelimit.store", "auth.jwt", "authz.rbac",
		"service.http", "service.smtp", "storage.fs",
	} {
		if !slices.Contains(kinds, want) {
			t.Errorf("the example no longer uses %s; it is meant to show every family", want)
		}
	}

	if len(doc.Secrets) == 0 {
		t.Error("no secret blocks: the example should show credentials resolved at load time")
	}
	if len(doc.Shapes) == 0 {
		t.Error("no shape blocks")
	}
	if len(doc.Roles) == 0 {
		t.Error("no role blocks: the example should show RBAC")
	}
	if len(doc.Workers) == 0 {
		t.Error("no worker blocks")
	}
	if len(doc.Schedules) == 0 {
		t.Error("no schedule blocks")
	}
	if len(doc.Triggers) == 0 {
		t.Error("no trigger blocks")
	}
	if len(doc.Processes) != 1 {
		t.Fatalf("expected one process, got %d", len(doc.Processes))
	}

	fulfil := doc.Processes[0]
	if fulfil.Store == "" || fulfil.Queue == "" || fulfil.Start == "" {
		t.Fatalf("the process is missing its store, queue or start: %+v", fulfil)
	}
	if fulfil.SLA == nil {
		t.Error("the process declares no SLA")
	}

	// A human task, a compensation and a fan-in are the three things that
	// distinguish the durable tier from a request graph.
	var hasTask, hasCompensation bool
	for _, step := range fulfil.Steps {
		if step.Task != nil {
			hasTask = true
			if len(step.Task.Actions) == 0 {
				t.Errorf("task step %q declares no actions, so its outgoing branches cannot match anything", step.Name)
			}
			if !step.Task.Due.Set() {
				t.Errorf("task step %q has no due date", step.Name)
			}
			if len(step.Task.ForbidPrincipals) == 0 {
				t.Errorf("task step %q has no four-eyes control", step.Name)
			}
		}
		if step.Compensate != "" {
			hasCompensation = true
		}
	}
	if !hasTask {
		t.Error("the process has no human task")
	}
	if !hasCompensation {
		t.Error("no step declares a compensation, so the example shows no saga")
	}

	edgeKinds := make([]string, 0, len(fulfil.Edges))
	for _, edge := range fulfil.Edges {
		edgeKinds = append(edgeKinds, edge.Kind)
		if edge.Kind == "" {
			t.Errorf("edge %q has no kind — the `kind` key is not binding", edge.Name)
		}
	}
	for _, want := range []string{"branch", "fanout", "fanin", "compensate"} {
		if !slices.Contains(edgeKinds, want) {
			t.Errorf("the process no longer uses a %s edge", want)
		}
	}

	// Every edge must carry its endpoints. `from` and `to` bind only on their own
	// line, so this is the assertion that catches an example drifting back to the
	// one-line form.
	for _, edge := range fulfil.Edges {
		endpoints := len(edge.Sources) + len(edge.Targets)
		if edge.From != "" {
			endpoints++
		}
		if edge.To != "" {
			endpoints++
		}
		if endpoints < 2 {
			t.Errorf("edge %q lost an endpoint (from=%q to=%q sources=%v targets=%v) — is it written on one line?",
				edge.Name, edge.From, edge.To, edge.Sources, edge.Targets)
		}
	}

	// Shape fields must carry their constraints, not just their names.
	for _, shape := range doc.Shapes {
		if len(shape.Props) == 0 {
			t.Errorf("shape %q declares no props", shape.Name)
		}
		for _, field := range shape.Props {
			if field.Kind == "" {
				t.Errorf("shape %q prop %q has no kind", shape.Name, field.Name)
			}
		}
	}

	// Resource config must arrive whole: a half-bound config is a connection with
	// default pool sizes and no migrations.
	for _, resource := range doc.Resources {
		if resource.Kind == "" {
			t.Errorf("resource %q has no kind", resource.Name)
		}
		if len(resource.Config) == 0 && resource.Kind != "authz.rbac" {
			t.Errorf("resource %q has an empty config", resource.Name)
		}
	}
	for _, name := range []string{"cache", "locks", "limits", "runs", "documents"} {
		for _, resource := range doc.Resources {
			if resource.Name != name {
				continue
			}
			if len(resource.Config) < 2 {
				t.Errorf("resource %q bound only %d config keys: %v — is its config written on one line?",
					name, len(resource.Config), resource.Config)
			}
		}
	}

	// Guards must be spelled `condition`, since BCL cannot carry `when`.
	conditioned := 0
	for _, edge := range fulfil.Edges {
		if edge.Condition != "" {
			conditioned++
		}
	}
	if conditioned == 0 {
		t.Error("no edge declares a condition")
	}

	// Every route guard the example advertises should actually be there.
	var authz, limited, audited int
	for _, route := range doc.Routes {
		if route.Authz != nil {
			authz++
		}
		if route.RateLimit != nil {
			limited++
		}
		if route.Audit != nil {
			audited++
		}
	}
	if authz == 0 || limited == 0 || audited == 0 {
		t.Errorf("route guards are thin: %d authz, %d rate limits, %d audits", authz, limited, audited)
	}
}

// exampleEnv supplies the values the example's env.required() calls need, so the
// document parses without a real deployment's secrets.
func exampleEnv(name string) (string, bool) {
	values := map[string]string{
		"DATABASE_URL":           "postgres://localhost/orders",
		"SESSION_SECRET":         strings.Repeat("s", 48),
		"JWT_SECRET":             strings.Repeat("j", 48),
		"PAYMENT_API_KEY":        "Bearer test-payment-key",
		"PAYMENT_WEBHOOK_SECRET": strings.Repeat("w", 32),
		"TODO_NOTIFICATION_HOST": "127.0.0.1",
		"WEBHOOK_API_KEY":        "test-webhook-key-with-sufficient-length",
		"WEBHOOK_TARGET_URL":     "https://hooks.example.test/events",
		"WEBHOOK_ALLOWED_HOST":   "hooks.example.test",
	}
	value, ok := values[name]
	return value, ok
}

// exampleRoot locates the repository's examples directory from the test's working
// directory, so the test works wherever `go test` is invoked from.
func exampleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}
	for range 5 {
		candidate := filepath.Join(dir, "examples")
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Skip("examples directory not found from the test's working directory")
	return ""
}

// TestExamplesValidateAgainstTheRegistry runs the compiler's own document
// validation over both examples, plus the reference checks that only the build
// stage would otherwise make.
//
// It stops short of opening resources, which is the only part that needs
// PostgreSQL — so this catches a misspelled action, an undeclared resource, an
// unreachable fact or a child intent that does not exist, on every `go test`,
// with no infrastructure at all. Those are exactly the mistakes that used to
// survive until a deployment.
func TestExamplesValidateAgainstTheRegistry(t *testing.T) {
	registry := NewRegistry()
	actions := registry.ActionNames()

	for _, name := range []string{"ref-platform", "ref-platform-todo"} {
		path := filepath.Join(exampleRoot(t), name, "app.bcl")
		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(path)
			if err != nil {
				t.Skipf("example not present: %v", err)
			}
			var doc Document
			if err := bcl.UnmarshalWithOptions(source, &doc, &bcl.Options{
				BaseDir: filepath.Dir(path), AllowEnv: true, Env: exampleEnv,
				ResolveImports: true, Strict: true,
			}); err != nil {
				t.Fatalf("parse: %v", err)
			}
			if err := validateDocument(doc, registry); err != nil {
				t.Fatalf("validate: %v", err)
			}

			declared := map[string]bool{}
			for _, intent := range doc.Intents {
				declared[intent.Name] = true
			}

			// Every node's action must exist in the registry, and every child
			// intent it names must be declared — whether it is named directly or
			// inside a cases/branches list.
			for _, intent := range doc.Intents {
				for _, node := range intent.Nodes {
					where := fmt.Sprintf("intent %q node %q", intent.Name, node.Name)
					if !slices.Contains(actions, node.Uses) {
						t.Errorf("%s uses unregistered action %q", where, node.Uses)
					}
					for _, child := range childIntentNames(node.Config) {
						if !declared[child] {
							t.Errorf("%s calls undeclared intent %q", where, child)
						}
					}
				}
			}

			for _, proc := range doc.Processes {
				for _, step := range proc.Steps {
					where := fmt.Sprintf("process %q step %q", proc.Name, step.Name)
					if step.Intent != "" && !declared[step.Intent] {
						t.Errorf("%s runs undeclared intent %q", where, step.Intent)
					}
					if step.Compensate != "" && !declared[step.Compensate] {
						t.Errorf("%s compensates with undeclared intent %q", where, step.Compensate)
					}
				}
			}
		})
	}
}

// childIntentNames collects every intent a node's config invokes, at the top
// level and inside a cases or branches list.
func childIntentNames(config map[string]any) []string {
	var out []string
	if name := configString(config, "intent", ""); name != "" {
		out = append(out, name)
	}
	for _, key := range []string{"cases", "branches", "steps"} {
		for _, block := range configBlocks(config, key) {
			if name := configString(block, "intent", ""); name != "" {
				out = append(out, name)
			}
		}
	}
	return out
}

// TestCatalogNeverAdvertisesAReservedName guards the catalog against advertising a
// key BCL cannot carry.
//
// The catalog is what a visual builder reads to decide which keys to emit, so a
// field named `when` there would make the builder generate documents that parse
// into silence. This is the same failure the platform exists to prevent, one level
// up.
func TestCatalogNeverAdvertisesAReservedName(t *testing.T) {
	catalog := NewRegistry().Catalog()
	reserved := append(append([]string{}, reservedBCLNames...), inlineUnsafeBCLNames...)

	check := func(where, field string) {
		// from/to are endpoints an edge must name; they are unsafe only inline, which
		// is a formatting rule, not a naming one.
		if field == "from" || field == "to" {
			return
		}
		if slices.Contains(reserved, field) {
			t.Errorf("%s advertises %q, which BCL cannot bind", where, field)
		}
	}
	for _, edge := range catalog.EdgeTypes {
		for _, field := range edge.Fields {
			check("edge type "+edge.Name, field)
		}
	}
	for _, action := range catalog.Actions {
		for _, field := range action.Config {
			check("action "+action.Name, field.Name)
		}
	}
	for _, kind := range catalog.ResourceKinds {
		for _, field := range kind.Config {
			check("resource kind "+kind.Name, field.Name)
		}
	}
}
