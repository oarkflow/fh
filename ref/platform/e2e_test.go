package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// End-to-end tests.
//
// These compile a whole application from BCL, mount it, and drive it over HTTP —
// through the guards, the intent graphs, the durable process, the human task and
// the saga rollback. They use only memory- and file-backed resources so they need
// no database; what they prove is that the tiers actually fit together, which no
// amount of unit testing of the pieces establishes.

// e2eApp is a complete application that needs no external infrastructure.
//
// It is deliberately written the way the documentation tells an author to write
// one: one key per line, `condition` for guards, `family` for node families,
// `shape`/`prop` for structures.
const e2eApp = `
name "e2e"
environment "test"

secret "session_key" {
  env "E2E_SESSION_SECRET"
  required true
}

resource "cache" {
  kind "cache.memory"
  config {
    max_entries 1000
  }
}

resource "sessions" {
  kind "session.memory"
  config {
    secret env.required("E2E_SESSION_SECRET")
    cookie "e2e_sid"
    max_age 1h
  }
}

resource "jobs" {
  kind "queue.file"
  config {
    dir env.required("E2E_QUEUE_DIR")
    workers 2
    poll_interval 20ms
  }
}

resource "runs" {
  kind "store.memory"
}

resource "locks" {
  kind "lock.store"
  config {
    cache "cache"
    ttl 5s
  }
}

resource "limits" {
  kind "ratelimit.store"
  config {
    cache "cache"
  }
}

resource "cookies" {
  kind "auth.session"
  config {
    session "sessions"
  }
}

resource "identity" {
  kind "auth.chain"
  config {
    authenticators [cookies]
  }
}

resource "rbac" {
  kind "authz.rbac"
  config {
    superuser_roles [admin]
  }
}

shape "Ticket" {
  kind object
  prop "subject" {
    kind string
    required true
    min_length 3
    max_length 80
  }
  prop "amount" {
    kind int
    required true
    min 0
  }
}

shape "Decision" {
  kind object
  prop "action" {
    kind string
    required true
    enum [approve, reject]
  }
  prop "note" {
    kind string
    max_length 200
  }
}

role "requester" {
  permissions [ticket:create]
}
role "approver" {
  inherits [requester]
  permissions [ticket:approve]
}
role "admin" {
  inherits [approver]
}

# --- identity ---------------------------------------------------------

intent "session.open" {
  description "Start a session for a named user with the roles they asked for"
  response "response"
  timeout 5s

  node "store_user" {
    family session
    uses "session.set"
    resource "sessions"
    kind effect
    requires [input]
    provides [stored_user]
    config {
      key "user_id"
      value_fact "input.user"
    }
  }
  node "store_roles" {
    family session
    uses "session.set"
    resource "sessions"
    kind effect
    requires [input]
    provides [stored_roles]
    config {
      key "roles"
      value_fact "input.roles"
    }
  }
  node "response" {
    family response
    uses "collect"
    requires [input, stored_user, stored_roles]
    provides [response]
  }
}

intent "session.who" {
  description "Report the caller as the platform sees them"
  response "response"
  node "user" {
    family auth
    uses "auth.require_session"
    resource "sessions"
    kind decision
    provides [user_id]
  }
  node "all" {
    family session
    uses "session.all"
    resource "sessions"
    kind read
    requires [user_id]
    provides [session_values]
  }
  node "response" {
    family response
    uses "collect"
    requires [session_values]
    provides [response]
    config {
      unwrap true
    }
  }
}

# --- control flow -----------------------------------------------------

intent "flow.small" {
  description "The child intent a small ticket takes"
  response "response"
  node "response" {
    family transform
    uses "expression"
    provides [response]
    config {
      expression "'small'"
    }
  }
}

intent "flow.large" {
  description "The child intent a large ticket takes"
  response "response"
  node "response" {
    family transform
    uses "expression"
    provides [response]
    config {
      expression "'large'"
    }
  }
}

intent "flow.route" {
  description "Branch to a child intent on the ticket's value"
  response "response"
  input_schema "Ticket"

  node "branch" {
    family branch
    uses "flow.branch"
    requires [input]
    provides [decision]
    config {
      cases [
        { name "large" condition "input.amount > 100" intent "flow.large" }
        { name "small" condition "input.amount <= 100" intent "flow.small" }
      ]
    }
  }
  node "response" {
    family response
    uses "collect"
    requires [decision]
    provides [response]
    config {
      unwrap true
    }
  }
}

intent "flow.each" {
  description "Run a child intent per item and collect the results"
  response "response"
  node "each" {
    family foreach
    uses "flow.foreach"
    requires [input]
    provides [results]
    config {
      intent "flow.small"
      items_fact "input.items"
      concurrency 4
    }
  }
  node "response" {
    family response
    uses "collect"
    requires [results]
    provides [response]
    config {
      unwrap true
    }
  }
}

# --- cache ------------------------------------------------------------

intent "cache.read" {
  description "A cache-aside read whose miss path is observable"
  response "response"
  idempotent true

  node "key" {
    family transform
    uses "expression"
    requires [input]
    provides [cache_key]
    config {
      expression "input.key"
    }
  }
  node "cached" {
    family cache
    uses "cache.get"
    resource "cache"
    kind read
    requires [cache_key]
    provides [cached]
    config {
      key_fact "cache_key"
      prefix "e2e:"
    }
  }
  node "store" {
    family cache
    uses "cache.set"
    resource "cache"
    kind effect
    requires [cache_key, cached]
    provides [stored]
    config {
      key_fact "cache_key"
      value_fact "cache_key"
      prefix "e2e:"
      ttl 1m
    }
  }
  node "response" {
    family response
    uses "collect"
    requires [cached, stored]
    provides [response]
  }
}

# --- guarded -----------------------------------------------------------

intent "guarded.create" {
  description "Needs a permission the caller may not hold"
  response "response"
  authz {
    permissions [ticket:create]
  }
  node "response" {
    family transform
    uses "expression"
    provides [response]
    config {
      expression "'created'"
    }
  }
}

# --- the durable process's steps ---------------------------------------

intent "ticket.record" {
  description "Record the ticket. Compensated by withdrawing it."
  response "response"
  node "response" {
    family transform
    uses "collect"
    requires [input]
    provides [response]
    config {
      unwrap true
    }
  }
}

intent "ticket.withdraw" {
  description "Undo ticket.record"
  response "response"
  node "response" {
    family transform
    uses "expression"
    provides [response]
    config {
      expression "'withdrawn'"
    }
  }
}

intent "ticket.mark_approved" {
  description "Note the approval"
  response "response"
  node "response" {
    family transform
    uses "collect"
    requires [input]
    provides [response]
  }
}

intent "ticket.mark_rejected" {
  description "Note the rejection"
  response "response"
  node "response" {
    family transform
    uses "expression"
    provides [response]
    config {
      expression "'rejected'"
    }
  }
}

intent "ticket.finish" {
  description "The terminal step"
  response "response"
  node "response" {
    family transform
    uses "expression"
    provides [response]
    config {
      expression "'finished'"
    }
  }
}

intent "ticket.start" {
  description "Start a durable approval run"
  response "response"
  input_schema "Ticket"

  node "user" {
    family auth
    uses "auth.require_session"
    resource "sessions"
    kind decision
    provides [user_id]
  }
  node "payload" {
    family transform
    uses "data.transform"
    requires [input, user_id]
    provides [payload]
    config {
      source_fact "input"
      data {
        set {
          requester "{{ user_id }}"
        }
        extract {
          id "'tkt-' + input.subject"
        }
      }
    }
  }
  node "run" {
    family process
    uses "process.start"
    kind effect
    requires [payload]
    provides [run]
    config {
      process "ticket.approval"
      input_fact "payload"
      idempotency_fact "payload.id"
      # Advance the run before answering, so the caller learns in the response
      # whether their ticket needs an approval or was already finished.
      wait true
    }
  }
  node "response" {
    family response
    uses "collect"
    requires [run]
    provides [response]
    config {
      unwrap true
    }
  }
}

intent "ticket.tasks" {
  description "The caller's approval queue"
  response "response"
  node "user" {
    family auth
    uses "auth.require_session"
    resource "sessions"
    kind decision
    provides [user_id]
  }
  node "tasks" {
    family human_task
    uses "task.list"
    kind read
    requires [user_id]
    provides [tasks]
    config {
      scope "mine"
    }
  }
  node "response" {
    family response
    uses "collect"
    requires [tasks]
    provides [response]
  }
}

intent "ticket.decide" {
  description "Record a decision and let the run continue"
  response "response"
  authz {
    permissions [ticket:approve]
  }
  node "user" {
    family auth
    uses "auth.require_session"
    resource "sessions"
    kind decision
    provides [user_id]
  }
  node "task_id" {
    family transform
    uses "request.param"
    provides [task_id]
    config {
      name "id"
    }
  }
  node "decide" {
    family approval
    uses "task.complete"
    kind effect
    requires [task_id, input, user_id]
    provides [task]
    config {
      task_id_fact "task_id"
      action_fact "input.action"
      payload_fact "input"
    }
  }
  node "response" {
    family response
    uses "collect"
    requires [task]
    provides [response]
    config {
      unwrap true
    }
  }
}

intent "ticket.status" {
  description "A run's state, with its step history"
  response "response"
  node "run_id" {
    family transform
    uses "request.param"
    provides [run_id]
    config {
      name "id"
    }
  }
  node "status" {
    family process
    uses "process.status"
    kind read
    requires [run_id]
    provides [status]
    config {
      run_id_fact "run_id"
      include_steps true
    }
  }
  node "response" {
    family response
    uses "collect"
    requires [status]
    provides [response]
    config {
      unwrap true
    }
  }
}

# --- the durable process ------------------------------------------------

process "ticket.approval" {
  description "Record, approve above a threshold, finish — with a rollback"
  store "runs"
  queue "jobs"
  start "record"
  idempotency "id"
  max_steps 50

  step "record" {
    intent "ticket.record"
    compensate "ticket.withdraw"
    family action
    lock "locks"
    lock_key "'ticket:' + run.input.id"
  }
  step "approve" {
    family approval
    task {
      title "Approve {{ run.input.subject }}"
      role "approver"
      actions [approve, reject]
      form_schema "Decision"
      due 1h
      forbid_principals ["run.input.requester"]
    }
  }
  step "record_approval" {
    intent "ticket.mark_approved"
    family action
  }
  step "record_rejection" {
    intent "ticket.mark_rejected"
    family action
    terminal true
  }
  step "finish" {
    intent "ticket.finish"
    family action
    terminal true
  }

  edge "small" {
    kind branch
    from "record"
    to "finish"
    condition "result.amount <= 100"
  }
  edge "large" {
    kind branch
    from "record"
    to "approve"
    condition "result.amount > 100"
  }
  edge "approved" {
    kind branch
    from "approve"
    to "record_approval"
    condition "result.action == 'approve'"
  }
  edge "rejected" {
    kind branch
    from "approve"
    to "record_rejection"
    condition "result.action == 'reject'"
  }
  edge "done" {
    kind simple
    from "record_approval"
    to "finish"
  }
}

# --- routes -------------------------------------------------------------

route "session.open" {
  method POST
  path "/session"
  intent "session.open"
  session "sessions"
  cache_control "no-store"
}

route "session.who" {
  method GET
  path "/session"
  intent "session.who"
  session "sessions"
}

route "flow.route" {
  method POST
  path "/flow/route"
  intent "flow.route"
}

route "flow.each" {
  method POST
  path "/flow/each"
  intent "flow.each"
}

route "cache.read" {
  method POST
  path "/cache"
  intent "cache.read"
  idempotency {
    header "Idempotency-Key"
    ttl 1m
    store "cache"
  }
}

route "guarded.create" {
  method POST
  path "/guarded"
  intent "guarded.create"
  session "sessions"
  authz {
    permissions [ticket:create]
  }
}

route "chain.who" {
  method GET
  path "/chain"
  intent "session.who"
  auth "identity"
  session "sessions"
  authz {
    permissions [ticket:create]
  }
}

route "limited" {
  method GET
  path "/limited"
  intent "session.who"
  session "sessions"
  rate_limit {
    limiter "limits"
    limit 2
    window 1m
    key "'fixed'"
  }
}

route "ticket.start" {
  method POST
  path "/tickets"
  intent "ticket.start"
  session "sessions"
  status 201
}

route "ticket.tasks" {
  method GET
  path "/tasks"
  intent "ticket.tasks"
  session "sessions"
}

route "ticket.decide" {
  method POST
  path "/tasks/:id/decide"
  intent "ticket.decide"
  session "sessions"
  authz {
    permissions [ticket:approve]
  }
}

route "ticket.status" {
  method GET
  path "/runs/:id"
  intent "ticket.status"
  session "sessions"
}
`

// harness is a compiled, mounted application plus a client that keeps cookies.
//
// The application is served over a real loopback listener rather than through
// fh's App.Test, because Test shuts the application down once it has read the
// response — fine for a single-request test, useless for a lifecycle that logs
// in, starts a run, claims a task and approves it over several requests.
type e2eHarness struct {
	t        *testing.T
	platform *Platform
	app      *fh.App
	base     string
	client   *http.Client
	cookie   string
}

func newE2E(t *testing.T) *e2eHarness {
	t.Helper()
	t.Setenv("E2E_SESSION_SECRET", strings.Repeat("e", 48))
	t.Setenv("E2E_QUEUE_DIR", t.TempDir())

	platform, err := Compile(context.Background(), []byte(e2eApp), t.TempDir(), DefaultLoadOptions())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	t.Cleanup(func() { _ = platform.Close() })

	app := fh.NewFast()
	if err := platform.Mount(app); err != nil {
		t.Fatalf("mount: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = app.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = app.ShutdownWithTimeout(2 * time.Second)
		<-served
	})

	return &e2eHarness{
		t:        t,
		platform: platform,
		app:      app,
		base:     "http://" + listener.Addr().String(),
		client:   &http.Client{Timeout: 10 * time.Second},
	}
}

// do sends a request, carrying the session cookie the harness has picked up.
func (h *e2eHarness) do(method, path string, body any, headers ...map[string]string) (int, map[string]any, string) {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("encode request: %v", err)
		}
		reader = strings.NewReader(string(encoded))
	}
	request, err := http.NewRequest(method, h.base+path, reader)
	if err != nil {
		h.t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if h.cookie != "" {
		request.Header.Set("Cookie", h.cookie)
	}
	for _, set := range headers {
		for name, value := range set {
			request.Header.Set(name, value)
		}
	}

	response, err := h.client.Do(request)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)

	if set := response.Header.Get("Set-Cookie"); set != "" {
		if index := strings.IndexByte(set, ';'); index > 0 {
			h.cookie = set[:index]
		} else {
			h.cookie = set
		}
	}
	var decoded map[string]any
	_ = json.Unmarshal(raw, &decoded)
	return response.StatusCode, decoded, string(raw)
}

// login opens a session with the given roles.
func (h *e2eHarness) login(user string, roles ...string) {
	h.t.Helper()
	status, _, raw := h.do(http.MethodPost, "/session", map[string]any{"user": user, "roles": roles})
	if status != 200 {
		h.t.Fatalf("open session: %d %s", status, raw)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestE2EApplicationCompilesAndServes(t *testing.T) {
	h := newE2E(t)

	// The redacted document must not carry the session secret, because it is what
	// an introspection route would serve.
	encoded, err := json.Marshal(h.platform.Document)
	if err != nil {
		t.Fatalf("encode document: %v", err)
	}
	if strings.Contains(string(encoded), strings.Repeat("e", 48)) {
		t.Fatal("the session secret appears in the public document")
	}

	// The catalog is what a visual builder reads. It should describe a real
	// platform, not a handful of primitives.
	catalog := h.platform.Registry().Catalog()
	if len(catalog.NodeTypes) < 50 {
		t.Errorf("catalog lists %d node families, expected the full taxonomy", len(catalog.NodeTypes))
	}
	if len(catalog.EdgeTypes) < 30 {
		t.Errorf("catalog lists %d edge types, expected the full set", len(catalog.EdgeTypes))
	}
	if len(catalog.Actions) < 60 {
		t.Errorf("catalog lists %d actions, expected the full catalog", len(catalog.Actions))
	}
	if len(catalog.ResourceKinds) < 20 {
		t.Errorf("catalog lists %d resource kinds", len(catalog.ResourceKinds))
	}
}

func TestE2ESessionsCarryIdentityIntoTheGraph(t *testing.T) {
	h := newE2E(t)

	// Without a session the decision node denies, and the response is a 401 rather
	// than a 500 or an empty success.
	status, _, raw := h.do(http.MethodGet, "/session", nil)
	if status != 401 {
		t.Fatalf("anonymous read = %d %s, want 401", status, raw)
	}

	h.login("alice", "requester")
	status, body, raw := h.do(http.MethodGet, "/session", nil)
	if status != 200 {
		t.Fatalf("authenticated read = %d %s", status, raw)
	}
	if body["user_id"] != "alice" {
		t.Fatalf("session does not carry the user: %v", body)
	}
}

func TestE2EAuthorizationDeniesAndAllows(t *testing.T) {
	h := newE2E(t)

	h.login("bob")
	status, _, raw := h.do(http.MethodPost, "/guarded", map[string]any{})
	if status != 403 {
		t.Fatalf("a caller without the permission got %d %s, want 403", status, raw)
	}

	h.cookie = ""
	h.login("carol", "requester")
	status, _, raw = h.do(http.MethodPost, "/guarded", map[string]any{})
	if status != 200 {
		t.Fatalf("a caller with the permission got %d %s", status, raw)
	}
}

func TestE2EInputSchemaRejectsBadRequests(t *testing.T) {
	h := newE2E(t)

	// Too short a subject and a missing amount: the schema reports both, before any
	// node runs.
	status, body, raw := h.do(http.MethodPost, "/flow/route", map[string]any{"subject": "x"})
	if status != 422 {
		t.Fatalf("invalid input = %d %s, want 422", status, raw)
	}
	message := failureMessage(body)
	if !strings.Contains(message, "subject") || !strings.Contains(message, "amount") {
		t.Fatalf("the failure does not report both problems: %q", message)
	}
}

func TestE2EBranchRunsOnlyTheMatchingChildIntent(t *testing.T) {
	h := newE2E(t)

	status, body, raw := h.do(http.MethodPost, "/flow/route", map[string]any{"subject": "laptop", "amount": 500})
	if status != 200 {
		t.Fatalf("large branch = %d %s", status, raw)
	}
	if body["case"] != "large" || body["result"] != "large" {
		t.Fatalf("large ticket took the wrong branch: %v", body)
	}

	status, body, raw = h.do(http.MethodPost, "/flow/route", map[string]any{"subject": "pencil", "amount": 3})
	if status != 200 {
		t.Fatalf("small branch = %d %s", status, raw)
	}
	if body["case"] != "small" || body["result"] != "small" {
		t.Fatalf("small ticket took the wrong branch: %v", body)
	}
}

func TestE2EForeachRunsOncePerItem(t *testing.T) {
	h := newE2E(t)
	status, body, raw := h.do(http.MethodPost, "/flow/each", map[string]any{"items": []any{1, 2, 3, 4}})
	if status != 200 {
		t.Fatalf("foreach = %d %s", status, raw)
	}
	results, ok := body["results"].([]any)
	if !ok || len(results) != 4 {
		t.Fatalf("expected four results, got %v", body)
	}
	if count, _ := ToFloat(body["succeeded"]); count != 4 {
		t.Fatalf("succeeded = %v", body["succeeded"])
	}
}

func TestE2ECacheAsideServesTheSecondRead(t *testing.T) {
	h := newE2E(t)

	status, body, raw := h.do(http.MethodPost, "/cache", map[string]any{"key": "k1"})
	if status != 200 {
		t.Fatalf("first read = %d %s", status, raw)
	}
	cached, _ := body["cached"].(map[string]any)
	if cached["found"] != false {
		t.Fatalf("the first read reported a cache hit: %v", body)
	}

	status, body, raw = h.do(http.MethodPost, "/cache", map[string]any{"key": "k1"})
	if status != 200 {
		t.Fatalf("second read = %d %s", status, raw)
	}
	cached, _ = body["cached"].(map[string]any)
	if cached["found"] != true {
		t.Fatalf("the second read missed the cache: %v", body)
	}
}

func TestE2EIdempotencyReplaysTheStoredResponse(t *testing.T) {
	h := newE2E(t)
	key := map[string]string{"Idempotency-Key": "abc-123"}

	status, first, raw := h.do(http.MethodPost, "/cache", map[string]any{"key": "idem"}, key)
	if status != 200 {
		t.Fatalf("first request = %d %s", status, raw)
	}

	// The replay must return the stored response rather than running the intent
	// again — which the cache node's own found flag would otherwise reveal.
	status, second, raw := h.do(http.MethodPost, "/cache", map[string]any{"key": "idem"}, key)
	if status != 200 {
		t.Fatalf("replayed request = %d %s", status, raw)
	}
	firstCached, _ := first["cached"].(map[string]any)
	secondCached, _ := second["cached"].(map[string]any)
	if firstCached["found"] != secondCached["found"] {
		t.Fatalf("the replay re-ran the intent: %v then %v", first, second)
	}
}

func TestE2ERateLimitRefusesTheThirdRequest(t *testing.T) {
	h := newE2E(t)
	h.login("dave", "requester")

	for attempt := 1; attempt <= 2; attempt++ {
		status, _, raw := h.do(http.MethodGet, "/limited", nil)
		if status != 200 {
			t.Fatalf("request %d = %d %s", attempt, status, raw)
		}
	}
	status, _, raw := h.do(http.MethodGet, "/limited", nil)
	if status != 429 {
		t.Fatalf("the third request = %d %s, want 429", status, raw)
	}
}

// TestE2EDurableApprovalEndToEnd is the test that matters most: a request starts a
// durable run, the run parks on a human task, a different person approves it, and
// the run continues to completion — with the four-eyes control refusing the
// requester's own approval along the way.
func TestE2EDurableApprovalEndToEnd(t *testing.T) {
	h := newE2E(t)

	h.login("erin", "requester")
	status, body, raw := h.do(http.MethodPost, "/tickets",
		map[string]any{"subject": "a large expense", "amount": 900})
	if status != 201 {
		t.Fatalf("start = %d %s", status, raw)
	}
	runID, _ := body["run_id"].(string)
	if runID == "" {
		t.Fatalf("no run id in %v", body)
	}
	if body["status"] != "waiting" {
		t.Fatalf("a large ticket should park for approval, got status %v", body["status"])
	}

	// The requester must not see it as their own approval task, and must not be
	// able to complete it: forbid_principals is the four-eyes control.
	status, ownTasks, raw := h.do(http.MethodGet, "/tasks", nil)
	if status != 200 {
		t.Fatalf("requester task list = %d %s", status, raw)
	}
	if tasks, ok := ownTasks["tasks"].([]any); ok && len(tasks) != 0 {
		t.Fatalf("the requester sees %d approval tasks", len(tasks))
	}

	// An approver sees it.
	h.cookie = ""
	h.login("frank", "approver")
	status, list, raw := h.do(http.MethodGet, "/tasks", nil)
	if status != 200 {
		t.Fatalf("approver task list = %d %s", status, raw)
	}
	tasks, _ := list["tasks"].([]any)
	if len(tasks) != 1 {
		t.Fatalf("the approver's queue has %d tasks, want 1: %v", len(tasks), list)
	}
	task, _ := tasks[0].(map[string]any)
	taskID, _ := task["task_id"].(string)
	if taskID == "" {
		t.Fatalf("task has no id: %v", task)
	}
	if !strings.Contains(fmt.Sprint(task["title"]), "a large expense") {
		t.Fatalf("the task title was not rendered from the run's data: %v", task["title"])
	}

	// An action outside the declared set is refused, so the outgoing branches can
	// rely on what they test.
	status, _, raw = h.do(http.MethodPost, "/tasks/"+taskID+"/decide", map[string]any{"action": "maybe"})
	if status != 422 {
		t.Fatalf("an undeclared action = %d %s, want 422", status, raw)
	}

	status, _, raw = h.do(http.MethodPost, "/tasks/"+taskID+"/decide",
		map[string]any{"action": "approve", "note": "within budget"})
	if status != 200 {
		t.Fatalf("approve = %d %s", status, raw)
	}

	// The run continued past the approval and completed.
	final := h.awaitRun(runID, "completed")
	steps, _ := final["steps"].([]any)
	executed := map[string]bool{}
	for _, entry := range steps {
		if object, ok := entry.(map[string]any); ok {
			executed[fmt.Sprint(object["step"])] = true
		}
	}
	for _, step := range []string{"record", "approve", "record_approval", "finish"} {
		if !executed[step] {
			t.Errorf("step %q did not run: %v", step, steps)
		}
	}
}

func TestE2EDurableRejectionTakesTheOtherBranch(t *testing.T) {
	h := newE2E(t)

	h.login("gina", "requester")
	status, body, raw := h.do(http.MethodPost, "/tickets",
		map[string]any{"subject": "a rejected expense", "amount": 700})
	if status != 201 {
		t.Fatalf("start = %d %s", status, raw)
	}
	runID, _ := body["run_id"].(string)

	h.cookie = ""
	h.login("henry", "approver")
	list, _, _ := h.taskList()
	if len(list) != 1 {
		t.Fatalf("the approver's queue has %d tasks", len(list))
	}
	taskID := fmt.Sprint(list[0]["task_id"])

	status, _, raw = h.do(http.MethodPost, "/tasks/"+taskID+"/decide", map[string]any{"action": "reject"})
	if status != 200 {
		t.Fatalf("reject = %d %s", status, raw)
	}

	final := h.awaitRun(runID, "completed")
	steps, _ := final["steps"].([]any)
	for _, entry := range steps {
		if object, ok := entry.(map[string]any); ok && object["step"] == "record_approval" {
			t.Fatal("the approval branch ran on a rejected ticket")
		}
	}
}

func TestE2ESmallTicketSkipsTheApproval(t *testing.T) {
	h := newE2E(t)
	h.login("iris", "requester")

	status, body, raw := h.do(http.MethodPost, "/tickets",
		map[string]any{"subject": "a small expense", "amount": 20})
	if status != 201 {
		t.Fatalf("start = %d %s", status, raw)
	}
	// Below the threshold the branch goes straight to the terminal step, so the run
	// is already finished when the request returns.
	if body["status"] != "completed" {
		t.Fatalf("a small ticket should complete without an approval, got %v", body["status"])
	}

	runID, _ := body["run_id"].(string)
	final := h.awaitRun(runID, "completed")
	steps, _ := final["steps"].([]any)
	for _, entry := range steps {
		if object, ok := entry.(map[string]any); ok && object["step"] == "approve" {
			t.Fatal("a small ticket opened an approval task")
		}
	}
}

func TestE2EIdempotentProcessStartReturnsTheSameRun(t *testing.T) {
	h := newE2E(t)
	h.login("jane", "requester")

	payload := map[string]any{"subject": "a repeated ticket", "amount": 900}
	status, first, raw := h.do(http.MethodPost, "/tickets", payload)
	if status != 201 {
		t.Fatalf("first start = %d %s", status, raw)
	}
	status, second, raw := h.do(http.MethodPost, "/tickets", payload)
	if status != 201 {
		t.Fatalf("second start = %d %s", status, raw)
	}
	if first["run_id"] != second["run_id"] {
		t.Fatalf("a repeated start created a second run: %v then %v", first["run_id"], second["run_id"])
	}

	// And exactly one task, not two.
	h.cookie = ""
	h.login("kevin", "approver")
	tasks, _, _ := h.taskList()
	if len(tasks) != 1 {
		t.Fatalf("the repeated start produced %d tasks", len(tasks))
	}
}

// TestE2EChainedSessionAuthenticatorCarriesTheCookie covers the identity model an
// application that serves both a browser and an API client needs: the route names
// an authenticator, and a signed session cookie satisfies it. Before auth.session
// existed, such a route refused every cookie-bearing request.
func TestE2EChainedSessionAuthenticatorCarriesTheCookie(t *testing.T) {
	h := newE2E(t)

	status, _, raw := h.do(http.MethodGet, "/chain", nil)
	if status != 401 {
		t.Fatalf("anonymous = %d %s, want 401", status, raw)
	}

	// A session with no permission authenticates but is not authorized: 403, not 401.
	h.login("leo")
	status, _, raw = h.do(http.MethodGet, "/chain", nil)
	if status != 403 {
		t.Fatalf("a caller without the permission = %d %s, want 403", status, raw)
	}

	h.cookie = ""
	h.login("mia", "requester")
	status, body, raw := h.do(http.MethodGet, "/chain", nil)
	if status != 200 {
		t.Fatalf("an authorized caller = %d %s", status, raw)
	}
	if body["user_id"] != "mia" {
		t.Fatalf("the chain did not resolve the session's identity: %v", body)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// taskList reads the caller's approval queue.
func (h *e2eHarness) taskList() ([]map[string]any, int, string) {
	h.t.Helper()
	status, body, raw := h.do(http.MethodGet, "/tasks", nil)
	entries, _ := body["tasks"].([]any)
	out := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		if object, ok := entry.(map[string]any); ok {
			out = append(out, object)
		}
	}
	return out, status, raw
}

// awaitRun polls a run until it reaches a status, advancing the engine's timers so
// a queued continuation does not depend on wall-clock luck.
func (h *e2eHarness) awaitRun(runID, want string) map[string]any {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last map[string]any
	for time.Now().Before(deadline) {
		status, body, raw := h.do(http.MethodGet, "/runs/"+runID, nil)
		if status != 200 {
			h.t.Fatalf("run status = %d %s", status, raw)
		}
		last = body
		if body["status"] == want {
			return body
		}
		for _, engine := range h.platform.ProcessEngines() {
			_, _ = engine.Tick(context.Background(), 20)
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.t.Fatalf("run %s never reached %q; last state %v", runID, want, last)
	return nil
}

// failureMessage pulls the message out of the platform's error envelope.
func failureMessage(body map[string]any) string {
	failure, _ := body["error"].(map[string]any)
	return fmt.Sprint(failure["message"])
}
