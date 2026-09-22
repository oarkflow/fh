package platform

// The node and edge type taxonomies.
//
// A node type is what a step *means*; the action named by `uses` is what
// actually runs. Keeping them separate is what lets one action back several
// semantic types (a notification and an email are both service.smtp) and one
// type be backed by several actions (a database node may query, exec or run
// CRUD) without the catalog and the executor drifting apart.
//
// Durable types are marked as such. Using one inside a request intent is
// rejected at compile time: a request thread cannot wait two days for an
// approval, and pretending otherwise is exactly the failure this platform
// exists to avoid.

func registerNodeTypes(r *Registry) {
	types := []NodeTypeInfo{
		// --- computation -------------------------------------------------
		{Name: "action", Family: "compute", Summary: "Generic unit of work"},
		{Name: "transform", Family: "compute", Summary: "Reshape a payload", DefaultAction: "data.transform"},
		{Name: "validate", Family: "compute", Summary: "Structural or rule validation", DefaultAction: "validate.schema"},
		{Name: "script", Family: "compute", Summary: "Evaluate a configured expression", DefaultAction: "expression"},
		{Name: "template", Family: "compute", Summary: "Render a text template", DefaultAction: "data.template"},
		{Name: "constant", Family: "compute", Summary: "Publish a fixed value", DefaultAction: "constant"},
		{Name: "custom", Family: "compute", Summary: "Host-registered action"},

		// --- decision ----------------------------------------------------
		{Name: "decision", Family: "decision", Summary: "Policy decision that can deny", DefaultAction: "decision.expression"},
		{Name: "decision_matrix", Family: "decision", Summary: "Decision table over rows and thresholds", DefaultAction: "decision.table"},
		{Name: "condition", Family: "decision", Summary: "Boolean guard", DefaultAction: "decision.expression"},
		{Name: "rules", Family: "decision", Summary: "Rule set evaluation", DefaultAction: "decision.table"},

		// --- control flow (request tier) ---------------------------------
		{Name: "branch", Family: "flow", Summary: "First matching case runs a child intent", DefaultAction: "flow.branch"},
		{Name: "switch", Family: "flow", Summary: "Value-matched dispatch to a child intent", DefaultAction: "flow.switch"},
		{Name: "foreach", Family: "flow", Summary: "Run a child intent per item", DefaultAction: "flow.foreach"},
		{Name: "iterator", Family: "flow", Summary: "Run a child intent per item", DefaultAction: "flow.foreach"},
		{Name: "parallel_map", Family: "flow", Summary: "Concurrent map over a collection", DefaultAction: "flow.parallel_map"},
		{Name: "parallel", Family: "flow", Summary: "Run named child intents concurrently", DefaultAction: "flow.parallel"},
		{Name: "race", Family: "flow", Summary: "First successful child intent wins", DefaultAction: "flow.race"},
		{Name: "quorum", Family: "flow", Summary: "N of M child intents must succeed", DefaultAction: "flow.quorum"},
		{Name: "join", Family: "flow", Summary: "Merge parallel results", DefaultAction: "collect"},
		{Name: "batch", Family: "flow", Summary: "Process a collection in groups", DefaultAction: "flow.foreach"},
		{Name: "subflow", Family: "flow", Summary: "Invoke one child intent", DefaultAction: "flow.subflow"},
		{Name: "pipeline", Family: "flow", Summary: "Invoke child intents in sequence", DefaultAction: "flow.pipeline"},
		{Name: "loop", Family: "flow", Summary: "Repeat until a condition holds", DefaultAction: "flow.loop_until"},
		{Name: "retry", Family: "flow", Summary: "Retry a child intent with backoff", DefaultAction: "flow.retry"},
		{Name: "timeout", Family: "flow", Summary: "Bound a child intent's duration", DefaultAction: "flow.timeout"},
		{Name: "fallback", Family: "flow", Summary: "Try alternatives in order", DefaultAction: "flow.fallback"},

		// --- data --------------------------------------------------------
		{Name: "database", Family: "data", Summary: "SQL query or statement", DefaultAction: "database.query", ResourceKinds: []string{"database."}},
		{Name: "db", Family: "data", Summary: "SQL query or statement", DefaultAction: "database.query", ResourceKinds: []string{"database."}},
		{Name: "crud", Family: "data", Summary: "Generated create/read/update/delete", DefaultAction: "database.crud", ResourceKinds: []string{"database."}},
		{Name: "cache", Family: "data", Summary: "Cache read, write or invalidation", DefaultAction: "cache.get", ResourceKinds: []string{"cache."}},
		{Name: "search", Family: "data", Summary: "Full-text search or indexing", DefaultAction: "search.query", ResourceKinds: []string{"search."}},
		{Name: "file", Family: "data", Summary: "Read or write a file", DefaultAction: "storage.get", ResourceKinds: []string{"storage."}},
		{Name: "storage", Family: "data", Summary: "Object storage operation", DefaultAction: "storage.put", ResourceKinds: []string{"storage."}},

		// --- identity ----------------------------------------------------
		{Name: "auth", Family: "identity", Summary: "Authenticate or manage credentials", DefaultAction: "auth.authenticate", ResourceKinds: []string{"auth."}},
		{Name: "session", Family: "identity", Summary: "Read or write session state", DefaultAction: "session.get", ResourceKinds: []string{"session."}},
		{Name: "authz", Family: "identity", Summary: "Authorization decision", DefaultAction: "decision.authz", ResourceKinds: []string{"authz."}},

		// --- messaging ---------------------------------------------------
		{Name: "queue", Family: "messaging", Summary: "Publish a durable job", DefaultAction: "queue.publish", ResourceKinds: []string{"queue."}},
		{Name: "worker", Family: "messaging", Summary: "Queue consumer entry point", DefaultAction: "collect"},
		{Name: "event", Family: "messaging", Summary: "Emit a domain event", DefaultAction: "queue.publish"},
		{Name: "outbox", Family: "messaging", Summary: "Transactional outbox publish", DefaultAction: "queue.outbox_publish"},
		{Name: "inbox", Family: "messaging", Summary: "Inbound deduplication", DefaultAction: "queue.inbox_dedupe"},
		{Name: "notification", Family: "messaging", Summary: "Send a notification", DefaultAction: "notify.send"},
		{Name: "email", Family: "messaging", Summary: "Send mail", DefaultAction: "service.smtp", ResourceKinds: []string{"service."}},
		{Name: "webhook", Family: "messaging", Summary: "Deliver a signed webhook", DefaultAction: "service.webhook_send", ResourceKinds: []string{"service."}},
		{Name: "stream", Family: "messaging", Summary: "Emit incremental results", DefaultAction: "stream.emit"},

		// --- integrations ------------------------------------------------
		{Name: "http", Family: "integration", Summary: "Outbound HTTP call", DefaultAction: "service.http", ResourceKinds: []string{"service."}},
		{Name: "service", Family: "integration", Summary: "Call a configured service", DefaultAction: "service.http", ResourceKinds: []string{"service."}},
		{Name: "grpc", Family: "integration", Summary: "gRPC-over-JSON call", DefaultAction: "service.grpc_json", ResourceKinds: []string{"service."}},
		{Name: "graphql", Family: "integration", Summary: "GraphQL query or mutation", DefaultAction: "service.graphql", ResourceKinds: []string{"service."}},
		{Name: "websocket", Family: "integration", Summary: "WebSocket message", DefaultAction: "service.http"},
		{Name: "tool", Family: "integration", Summary: "Invoke a registered tool", DefaultAction: "service.http"},
		{Name: "connector", Family: "integration", Summary: "Host-registered connector"},

		// --- intelligence ------------------------------------------------
		{Name: "llm", Family: "intelligence", Summary: "Language model completion", DefaultAction: "service.llm_chat", ResourceKinds: []string{"service."}},
		{Name: "embedding", Family: "intelligence", Summary: "Compute embeddings", DefaultAction: "service.llm_embed", ResourceKinds: []string{"service."}},
		{Name: "rag", Family: "intelligence", Summary: "Retrieve grounding context", DefaultAction: "service.rag_retrieve"},
		{Name: "classifier", Family: "intelligence", Summary: "Classify a payload", DefaultAction: "service.llm_chat"},

		// --- coordination ------------------------------------------------
		{Name: "lock", Family: "coordination", Summary: "Acquire or release a lease", DefaultAction: "lock.acquire", ResourceKinds: []string{"lock."}},
		{Name: "rate_limit", Family: "coordination", Summary: "Consume a rate-limit token", DefaultAction: "rate_limit.check", ResourceKinds: []string{"ratelimit."}},
		{Name: "circuit_breaker", Family: "coordination", Summary: "Guard an unreliable dependency", DefaultAction: "circuit_breaker.guard"},
		{Name: "idempotency", Family: "coordination", Summary: "Deduplicate a repeated operation", DefaultAction: "idempotency.guard"},

		// --- durable orchestration (process tier only) -------------------
		{Name: "process", Family: "process", Summary: "Start or signal a durable run", DefaultAction: "process.start"},
		{Name: "workflow", Family: "process", Summary: "Start an external orchestrator run", DefaultAction: "workflow.start"},
		{Name: "subprocess", Family: "process", Summary: "Run a child process", Durable: true},
		{Name: "compensation", Family: "process", Summary: "Undo a committed step", Durable: true},
		{Name: "wait", Family: "process", Summary: "Park until an external event", Durable: true},
		{Name: "wait_event", Family: "process", Summary: "Park until a named event", Durable: true},
		{Name: "timer", Family: "process", Summary: "Park until an instant", Durable: true},
		{Name: "delay", Family: "process", Summary: "Park for a duration", Durable: true},
		// These four park when they appear as a process *step*, but a request may
		// legitimately list, claim and complete the work items they create — which
		// is what an approval UI does. So they are not marked durable: the durable
		// flag is for families with no request-tier action at all.
		{Name: "human_task", Family: "process", Summary: "A work item somebody must complete", DefaultAction: "task.list"},
		{Name: "approval", Family: "process", Summary: "A decision somebody must make", DefaultAction: "task.complete"},
		{Name: "manual_review", Family: "process", Summary: "Work held for manual review", DefaultAction: "task.list"},
		{Name: "form", Family: "process", Summary: "Input collected from a person", DefaultAction: "task.complete"},
		{Name: "external_task", Family: "process", Summary: "Park until an external worker reports", Durable: true},
		{Name: "escalation", Family: "process", Summary: "Reassign or raise overdue work", DefaultAction: "task.reassign"},

		// --- observability and terminals ---------------------------------
		{Name: "audit", Family: "observability", Summary: "Append a hash-chained audit record", DefaultAction: "audit.record"},
		{Name: "metric", Family: "observability", Summary: "Emit a metric", DefaultAction: "metric.emit"},
		{Name: "trace", Family: "observability", Summary: "Annotate the current trace", DefaultAction: "trace.span"},
		{Name: "log", Family: "observability", Summary: "Record a structured log line", DefaultAction: "audit.record"},
		{Name: "response", Family: "terminal", Summary: "Assemble the response", DefaultAction: "collect", Terminal: true},
		{Name: "terminal", Family: "terminal", Summary: "End the graph", DefaultAction: "flow.terminate", Terminal: true},
		{Name: "noop", Family: "terminal", Summary: "Do nothing", DefaultAction: "collect"},
	}
	for _, info := range types {
		if err := r.RegisterNodeType(info); err != nil {
			panic("ref/platform: built-in node type: " + err.Error())
		}
	}
}

func registerEdgeTypes(r *Registry) {
	types := []EdgeTypeInfo{
		// --- sequencing ---------------------------------------------------
		{Name: "simple", Family: "sequence", Summary: "Traverse to the target",
			Fields: []string{"from", "to", "condition", "data"}},
		{Name: "branch", Family: "sequence", Summary: "Traverse only when the condition holds",
			Fields: []string{"from", "to", "condition", "data"}},
		{Name: "switch", Family: "sequence", Summary: "Value-matched branch",
			Fields: []string{"from", "to", "condition", "data"}},
		{Name: "conditional_fork", Family: "sequence", Summary: "Traverse every target whose condition holds", Multi: true,
			Fields: []string{"from", "targets", "condition", "data"}},
		{Name: "threshold", Family: "sequence", Summary: "Route by numeric band",
			Fields: []string{"from", "threshold", "data"}},
		{Name: "weighted", Family: "sequence", Summary: "Weighted random choice among siblings",
			Fields: []string{"from", "to", "weight", "condition"}},
		{Name: "priority", Family: "sequence", Summary: "Lowest priority value among siblings wins",
			Fields: []string{"from", "to", "priority", "condition"}},

		// --- concurrency --------------------------------------------------
		{Name: "fanout", Family: "concurrency", Summary: "Traverse to every target", Multi: true,
			Fields: []string{"from", "targets", "condition", "data"}},
		{Name: "dynamic_fanout", Family: "concurrency", Summary: "Targets chosen at run time", Multi: true,
			Fields: []string{"from", "targets_path", "condition", "data"}},
		{Name: "fanin", Family: "concurrency", Summary: "Wait for sources, then continue", Multi: true,
			Fields: []string{"sources", "to", "strategy", "quorum", "data"}},
		{Name: "join", Family: "concurrency", Summary: "Wait for sources, then continue", Multi: true,
			Fields: []string{"sources", "to", "strategy", "quorum", "data"}},
		{Name: "quorum", Family: "concurrency", Summary: "Continue once enough sources finish", Multi: true,
			Fields: []string{"sources", "to", "quorum", "strategy", "data"}},
		{Name: "parallel", Family: "concurrency", Summary: "Run targets concurrently", Multi: true,
			Fields: []string{"from", "targets", "max_concurrency", "fail_fast", "continue_on_error"}},
		{Name: "race", Family: "concurrency", Summary: "First successful target wins", Multi: true,
			Fields: []string{"from", "targets", "cancel_losers", "timeout"}},

		// --- iteration ----------------------------------------------------
		{Name: "iterator", Family: "iteration", Summary: "Run the target once per item",
			Fields: []string{"from", "to", "items_path", "max_concurrency", "continue_on_error"}},
		{Name: "batch_iterator", Family: "iteration", Summary: "Run the target once per batch",
			Fields: []string{"from", "to", "items_path", "batch_size", "max_concurrency"}},
		{Name: "loop_until", Family: "iteration", Summary: "Repeat the target until the condition holds",
			Fields: []string{"from", "to", "condition", "max_concurrency"}},

		// --- reliability --------------------------------------------------
		{Name: "retry", Family: "reliability", Summary: "Retry the target with backoff",
			Fields: []string{"from", "to", "attempts", "timeout"}},
		{Name: "timeout", Family: "reliability", Summary: "Bound the target's duration",
			Fields: []string{"from", "to", "timeout", "on_timeout"}},
		{Name: "rate_limited", Family: "reliability", Summary: "Park until the rate limit admits the run", Parks: true,
			Fields: []string{"from", "to", "rate_limit", "limit", "window"}},
		{Name: "error", Family: "reliability", Summary: "Traverse when the source fails", ErrorPath: true,
			Fields: []string{"from", "to", "condition", "data"}},
		{Name: "fallback", Family: "reliability", Summary: "Alternative path after a failure", ErrorPath: true,
			Fields: []string{"from", "to", "condition", "data"}},
		{Name: "compensate", Family: "reliability", Summary: "Run compensation after a failure", ErrorPath: true,
			Fields: []string{"from", "to", "condition"}},

		// --- suspension ---------------------------------------------------
		{Name: "delayed", Family: "suspension", Summary: "Park for a duration, then continue", Parks: true,
			Fields: []string{"from", "to", "timeout", "data"}},
		{Name: "wait_event", Family: "suspension", Summary: "Park until a named event arrives", Parks: true,
			Fields: []string{"from", "to", "event", "correlation", "timeout", "on_timeout"}},
		{Name: "manual", Family: "suspension", Summary: "Park until an operator advances the run", Parks: true,
			Fields: []string{"from", "to", "condition"}},
		{Name: "escalation", Family: "suspension", Summary: "Raise or reassign overdue work", Parks: true,
			Fields: []string{"from", "to", "timeout", "escalate", "notify"}},

		// --- termination and shaping --------------------------------------
		{Name: "cancel", Family: "termination", Summary: "Cancel the run",
			Fields: []string{"from", "condition"}},
		{Name: "transform", Family: "shaping", Summary: "Reshape the payload in transit",
			Fields: []string{"from", "to", "data", "extract"}},
		{Name: "filter", Family: "shaping", Summary: "Traverse only when the payload passes the filter",
			Fields: []string{"from", "to", "data"}},
		{Name: "stream_pipe", Family: "shaping", Summary: "Stream the payload to the target",
			Fields: []string{"from", "to", "data"}},
	}
	for _, info := range types {
		if err := r.RegisterEdgeType(info); err != nil {
			panic("ref/platform: built-in edge type: " + err.Error())
		}
	}
}
